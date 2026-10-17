#!/usr/bin/env python3
"""
mssql_query.py - A command-line MSSQL query tool (like sqlite3 CLI)

Usage:
  python mssql_query.py [OPTIONS] [DATABASE]
  python mssql_query.py [OPTIONS] [DATABASE] [SQL_FILE]

Connection env vars:
  MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE

Windows Integrated Authentication (-E/--trusted-connection) uses pyodbc and
the ODBC Driver for SQL Server instead of pymssql, and its own env var:
  MSSQL_ODBC_DRIVER

MySQL (--mysql) uses pymysql and its own env vars:
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
"""

import argparse
import csv
import decimal
import datetime
import itertools
import json
import math
import os
import shlex
import struct
import sys
import re
import tempfile
import importlib
import atexit
import time
from io import StringIO
from pathlib import Path

# Every driver is optional at import time and checked lazily when a
# connection is actually requested, so e.g. a MySQL-only install doesn't need
# pymssql. pymssql handles standard SQL logins; pyodbc handles
# --trusted-connection (Windows Integrated Auth / SSO), which pymssql's FreeTDS
# backend cannot do; pymysql handles --mysql.
try:
    import pymssql
except ImportError:
    pymssql = None

try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    import pymysql
except ImportError:
    pymysql = None

PYMSSQL_INSTALL_HINT = (
    "pip install pymssql\n"
    "  On Windows, use: pip install pymssql --only-binary :all:\n"
    "  If no binary wheel is available for your Python version, see:\n"
    "  https://pymssql.readthedocs.io/en/stable/intro.html#installation"
)

# Every driver's connection/cursor errors that call sites need to catch,
# regardless of which backend actually connected. Built once from whichever
# driver(s) are importable, so `except DBError` catches pymssql.Error,
# pyodbc.Error, and pymysql.Error alike. pymssql's low-level _mssql
# exceptions are not pymssql.Error subclasses, and some Connection methods
# (rollback() with unread results, for example) let them escape unwrapped.
DBError = tuple(
    error_type
    for error_type in (
        getattr(pymssql, "Error", None),
        getattr(getattr(pymssql, "_mssql", None), "MSSQLException", None),
        getattr(pyodbc, "Error", None),
        getattr(pymysql, "Error", None),
    )
    if isinstance(error_type, type)
)

# pymssql releases before 2.4 return datetimeoffset values as raw bytes and
# misread the milliseconds of most time/datetime2 values.
PYMSSQL_MIN_RELIABLE_VERSION = (2, 4)

def _load_readline():
    """Load a readline-compatible module when available."""
    for module_name in ("readline", "pyreadline3"):
        try:
            return importlib.import_module(module_name)
        except ImportError:
            pass
    return None


readline = _load_readline()


# Keep non-interactive input bounded.  SQL Server scripts are normally small,
# and accepting an unlimited pipe or file makes the client an easy way to
# exhaust its own memory before it can report a useful error.
MAX_SQL_INPUT_BYTES = 64 * 1024 * 1024
MAX_GO_REPEAT_COUNT = 10_000

# A JSON array (unlike CSV/JSONL, or a JSON array's own .jsonl equivalent)
# can't be streamed — the whole file must be parsed to confirm the closing
# ']' before any row is usable — so bound it the same way MAX_SQL_INPUT_BYTES
# bounds SQL text, instead of letting a huge/malformed .json file exhaust
# memory before read_json_data reports anything.
MAX_JSON_ARRAY_IMPORT_BYTES = 512 * 1024 * 1024
# Per-field cap for CSV import. csv.reader's 128 KiB default is far below what
# .export writes for (max) columns, so a table could not re-import its own export.
MAX_CSV_IMPORT_FIELD_CHARS = 512 * 1024 * 1024
# SQL Server import requests: rows per stacked-INSERT request, parameters per
# request (the server's limit for parameterized requests is 2,100), and a
# rough cap on the characters of values in one request.
IMPORT_ROWS_PER_REQUEST = 1000
IMPORT_PARAMS_PER_REQUEST = 2000
IMPORT_CHARS_PER_REQUEST = 4 * 1024 * 1024


# ── Output Formatters ──────────────────────────────────────────────────────────

def _format_timedelta(value):
    """Render a timedelta as a [-]HH:MM:SS[.ffffff] TIME value. pymysql
    returns MySQL TIME columns as timedelta (TIME spans -838:59:59 to
    838:59:59, so it can't be a datetime.time); str() would give
    "-2 days, 17:45:00", which is neither readable nor a valid TIME."""
    sign = "-" if value < datetime.timedelta(0) else ""
    total_us = abs(value) // datetime.timedelta(microseconds=1)
    secs, micros = divmod(total_us, 1_000_000)
    hours, rem = divmod(secs, 3600)
    minutes, secs = divmod(rem, 60)
    frac = f".{micros:06d}" if micros else ""
    return f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}{frac}"


def _cell_text(value):
    """Text for one cell in table/vertical/line output."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime.timedelta):
        return _format_timedelta(value)
    return str(value)


def _hex_encode_bytes(value):
    """Render bytes as the 0x-prefixed hex string sql_literal uses for INSERT
    output, so CSV/JSON export/print don't fall back to Python's repr() of
    bytes (e.g. "b'\\x01\\x02'"), which corrupts VARBINARY/IMAGE columns and
    is not re-importable. MySQL TIME values (timedelta) are rendered as
    HH:MM:SS for the same reason."""
    if isinstance(value, datetime.timedelta):
        return _format_timedelta(value)
    return "0x" + value.hex() if isinstance(value, bytes) else value


def _json_safe(value):
    """Make a value safe to pass to json.dumps: hex-encode bytes (as above)
    and map non-finite floats to JSON null. Mirrors sql_literal's NaN/Inf
    handling for INSERT statements — SQL Server has no literal syntax for
    them, and Python's default json.dumps emits bare `NaN`/`Infinity` tokens
    that violate RFC 8259 and most non-Python JSON parsers reject."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return _hex_encode_bytes(value)


def format_table(rows, headers, show_headers=True):
    """Render results as a bordered ASCII table."""
    if not rows and not headers:
        return "(no rows returned)"
    col_widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i >= len(col_widths):
                break  # defensive: ignore cells with no matching header
            col_widths[i] = max(col_widths[i], len(_cell_text(cell)))
    sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    def fmt_row(row):
        return "|" + "|".join(
            " " + _cell_text(c).ljust(w) + " "
            for c, w in zip(row, col_widths)
        ) + "|"
    lines = [sep, fmt_row(headers), sep] if show_headers else [sep]
    for row in rows:
        lines.append(fmt_row(row))
    if rows:
        lines.append(sep)
    return "\n".join(lines)


def format_csv(rows, headers, show_headers=True):
    """Render results as CSV. None → empty field (standard CSV null representation)."""
    buf = StringIO()
    writer = csv.writer(buf)
    if show_headers:
        writer.writerow(headers)
    for row in rows:
        writer.writerow(["" if c is None else _hex_encode_bytes(c) for c in row])
    text = buf.getvalue()
    if text.endswith("\r\n"):
        return text[:-2]
    if text.endswith("\n"):
        return text[:-1]
    return text


def format_json(rows, headers):
    """Render results as a JSON array of objects."""
    _validate_json_headers(headers)
    return json.dumps(
        [{h: _json_safe(c) for h, c in zip(headers, row)} for row in rows],
        indent=2, default=str,
    )


class ResultFormatError(Exception):
    """A result cannot be represented without losing data."""


def _validate_json_headers(headers):
    if len(headers) != len(set(headers)):
        raise ResultFormatError(
            "JSON requires unique column labels; alias duplicate columns"
        )


def format_vertical(rows, headers):
    """Render each row vertically (field: value)."""
    if not rows:
        return "(no rows returned)"
    lines = []
    width = max((len(str(h)) for h in headers), default=0)
    for i, row in enumerate(rows):
        lines.append(f"{'─'*20} row {i+1} {'─'*20}")
        for h, c in zip(headers, row):
            lines.append(f"  {str(h).rjust(width)}: {_cell_text(c)}")
    return "\n".join(lines)


def format_line(rows, headers):
    """Render as key=value pairs, one row per line."""
    lines = []
    for row in rows:
        lines.append(", ".join(f"{h}={_cell_text(c)}" for h, c in zip(headers, row)))
    return "\n".join(lines) if lines else "(no rows returned)"


FORMATTERS = {
    "table":    format_table,
    "csv":      format_csv,
    "json":     format_json,
    "vertical": format_vertical,
    "line":     format_line,
}

ALL_MODES = list(FORMATTERS) + ["insert"]


# ── INSERT Statement Formatter ─────────────────────────────────────────────────

# Identifier quote characters per SQL dialect: SQL Server uses [brackets],
# MySQL uses `backticks`. Either way a literal closing quote inside the name
# is escaped by doubling it.
IDENTIFIER_QUOTES = {"mssql": ("[", "]"), "mysql": ("`", "`")}


def _dialect(ctx):
    """Return the active connection's SQL dialect ("mssql" or "mysql")."""
    return ctx.get("dialect", "mssql")


def _split_identifier_parts(name, dialect="mssql"):
    """Split a multipart identifier on dots outside the dialect's identifier quotes."""
    open_q, close_q = IDENTIFIER_QUOTES[dialect]
    parts = []
    current = []
    in_quotes = False
    i = 0
    while i < len(name):
        ch = name[i]
        nxt = name[i + 1] if i + 1 < len(name) else ""
        if in_quotes:
            current.append(ch)
            if ch == close_q:
                if nxt == close_q:
                    current.append(nxt)
                    i += 1
                else:
                    in_quotes = False
            i += 1
            continue
        if ch == open_q:
            in_quotes = True
            current.append(ch)
        elif ch == ".":
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
        else:
            current.append(ch)
        i += 1

    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts


def _quote_identifier_part(part, dialect="mssql"):
    """Quote one identifier part ([x] for SQL Server, `x` for MySQL) with escaping."""
    open_q, close_q = IDENTIFIER_QUOTES[dialect]
    return open_q + str(part).replace(close_q, close_q * 2) + close_q


def _normalize_identifier_part(part, dialect="mssql"):
    """Return an unquoted identifier part, unescaping doubled closing quotes."""
    open_q, close_q = IDENTIFIER_QUOTES[dialect]
    if len(part) >= 2 and part.startswith(open_q) and part.endswith(close_q):
        return part[1:-1].replace(close_q * 2, close_q)
    return part


def _bracket_table(name, dialect="mssql"):
    """Quote each part of a table name ([schema].[table], or `db`.`table` for MySQL)."""
    # Split on unquoted dots and quote each part individually.
    # This preserves already-quoted parts while fixing mixed forms like
    # "[dbo].users" -> "[dbo].[users]" and supports three-part names.
    parts = _split_identifier_parts(name, dialect)
    if not parts:
        return _quote_identifier_part("", dialect)
    return ".".join(
        _quote_identifier_part(_normalize_identifier_part(part, dialect), dialect)
        for part in parts
    )


def _table_ref(database_name, schema_name, table_name, dialect="mssql"):
    parts = [p for p in (database_name, schema_name, table_name) if p is not None]
    return ".".join(_quote_identifier_part(part, dialect) for part in parts)


def _split_sql_identifier(name, default_schema=None, dialect="mssql"):
    """
    Split an identifier on unquoted dots.
    Returns the bare identifier parts with surrounding quotes removed.
    """
    parts = [
        _normalize_identifier_part(part, dialect)
        for part in _split_identifier_parts(name.strip(), dialect)
    ]

    if not parts:
        return []
    if default_schema is not None and len(parts) == 1:
        return [default_schema, parts[0]]
    return parts


def _qualify_table_parts(name, default_schema=None, dialect="mssql"):
    """
    Normalize a table identifier into (database, schema, table).
    Database may be None when not supplied.

    MySQL has no schema level separate from the database: `table` resolves
    to (None, None, table) — i.e. the session's current database — and
    `db.table` to (None, db, table), with db filling the schema slot so it
    matches INFORMATION_SCHEMA's TABLE_SCHEMA. default_schema is ignored.
    """
    if dialect == "mysql":
        parts = _split_sql_identifier(name, dialect=dialect)
        if len(parts) == 1:
            return None, None, parts[0]
        if len(parts) == 2:
            return None, parts[0], parts[1]
        raise ValueError(
            f"unsupported table name '{name}'. Use table or database.table."
        )
    parts = _split_sql_identifier(name, default_schema=default_schema)
    if len(parts) == 2:
        schema_name, table_name = parts
        return None, schema_name, table_name
    if len(parts) == 3:
        database_name, schema_name, table_name = parts
        return database_name, schema_name, table_name
    raise ValueError(
        f"unsupported table name '{name}'. "
        "Use table, schema.table, or database.schema.table."
    )


def _split_column_list(column_list, dialect="mssql"):
    """Split a comma-separated column list on commas outside identifier quotes."""
    open_q, close_q = IDENTIFIER_QUOTES[dialect]
    items, current, in_quotes, i = [], [], False, 0
    while i < len(column_list):
        ch = column_list[i]
        current.append(ch)
        if in_quotes:
            if ch == close_q:
                if column_list[i + 1:i + 2] == close_q:
                    current.append(close_q)
                    i += 1
                else:
                    in_quotes = False
        elif ch == open_q:
            in_quotes = True
        elif ch == ",":
            current.pop()
            items.append("".join(current))
            current = []
        i += 1
    items.append("".join(current))
    return items


def _quote_column_list(column_list, dialect="mssql"):
    """Quote a comma-separated list of column identifiers for simple exports."""
    quoted = []
    for raw_col in _split_column_list(column_list, dialect):
        col = raw_col.strip()
        if not col:
            raise ValueError("--columns contains an empty column name")
        if col == "*":
            quoted.append("*")
            continue
        parts = _split_sql_identifier(col, dialect=dialect)
        if not parts:
            raise ValueError(f"invalid column name '{col}'")
        quoted.append(".".join(_quote_identifier_part(part, dialect) for part in parts))
    return ", ".join(quoted)


def _reject_stacked_sql_fragment(option_name, value):
    if ";" in value:
        raise ValueError(f"{option_name} must not contain statement separators")


# Sentinel for a query parameter placeholder, used only by this module's own
# internally-generated SQL (never raw user-typed SQL). Query-building code
# writes PARAM_PLACEHOLDER instead of a literal %s/?; _adapt_placeholders
# substitutes the active driver's real placeholder syntax right before
# execute()/executemany(). A literal "%s" is legal text inside a SQL Server
# bracket-quoted identifier (e.g. a database named [my%sdb]), so replacing
# "%s" directly in the finished SQL string risks mangling an interpolated
# identifier that happens to contain it, or throwing off the parameter
# count. This NUL-delimited sentinel can't appear in identifier or literal
# text, so it can never collide with anything else in the query.
PARAM_PLACEHOLDER = "\x00P\x00"


def _adapt_placeholders(sql, ctx):
    """Replace this module's PARAM_PLACEHOLDER sentinel with the active
    driver's real placeholder syntax: %s for pymssql/pymysql, ? for pyodbc."""
    if ctx.get("paramstyle") == "qmark":
        return sql.replace(PARAM_PLACEHOLDER, "?")
    if _dialect(ctx) == "mysql":
        # pymysql %-formats the whole statement (executemany even formats the
        # INSERT prefix with no args), so a literal % in a quoted identifier
        # such as `pct%` must be doubled. Every caller passes args, so the
        # driver always collapses %% back to %.
        sql = sql.replace("%", "%%")
    elif _uses_named_params(ctx):
        # pymssql substitutes every "%s" in the statement -- including one
        # inside a bracketed identifier such as [p%s] -- and never collapses
        # "%%", but leaves "%s" alone when named placeholders are used.
        counter = itertools.count()
        return re.sub(
            re.escape(PARAM_PLACEHOLDER),
            lambda _m: f"%(p{next(counter)})s",
            sql,
        )
    return sql.replace(PARAM_PLACEHOLDER, "%s")


def _uses_named_params(ctx):
    return ctx.get("driver_name") == "pymssql" and _dialect(ctx) == "mssql"


def _adapt_params(params, ctx):
    """Match _adapt_placeholders: pymssql gets {"p0": ..., "p1": ...}."""
    if params and _uses_named_params(ctx):
        return {f"p{i}": _pymssql_param_value(value) for i, value in enumerate(params)}
    return params


def _pymssql_param_value(value):
    # pymssql inlines parameters into the SQL text as N'...' literals, so
    # T-SQL's backslash line continuation applies to them as well.
    if isinstance(value, str):
        return _TSQL_LINE_CONTINUATION.sub(_escape_tsql_line_continuation, value)
    return value


def sql_literal(value, dialect="mssql", datetime_timespec="milliseconds"):
    """
    Convert a Python value returned by the driver into a safe SQL literal string.
    Handles: None, bool, int, float, Decimal, datetime, date, time, timedelta,
    bytes, str.

    datetime values default to millisecond precision because SQL Server's
    legacy datetime type rejects strings with more fractional digits. With
    datetime_timespec="microseconds", values that have sub-millisecond digits
    keep them (for datetime2/datetimeoffset); millisecond-aligned values, which
    include everything read from a legacy datetime column, stay at 3 digits.
    """
    if value is None:
        return "NULL"
    # bool must come before int (bool is a subclass of int)
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        # SQL Server has no literal syntax for Inf/NaN; emit NULL so generated
        # statements stay valid and re-importable.
        if not math.isfinite(value):
            return "NULL"
        # repr preserves full precision and avoids scientific notation for
        # numbers that fit in standard range
        return repr(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, datetime.datetime):
        if datetime_timespec == "microseconds" and value.microsecond % 1000 == 0:
            datetime_timespec = "milliseconds"
        if dialect == "mssql" and value.utcoffset() is not None:
            return "'" + value.isoformat(sep="T", timespec=datetime_timespec) + "'"
        # isoformat always zero-pads the year; strftime("%Y") does not on
        # glibc, and SQL Server reads '1-01-01' as 2001-01-01.
        naive = value.replace(tzinfo=None)
        return "'" + naive.isoformat(sep=" ", timespec=datetime_timespec) + "'"
    if isinstance(value, datetime.date):
        return f"'{value.isoformat()}'"
    if isinstance(value, datetime.time):
        return f"'{value.isoformat()}'"
    if isinstance(value, datetime.timedelta):
        return f"'{_format_timedelta(value)}'"
    if isinstance(value, bytes):
        if dialect == "mysql":
            # MySQL rejects a bare "0x" for empty binary; X'' is always valid.
            return f"X'{value.hex()}'"
        return "0x" + value.hex()
    text = str(value)
    if dialect == "mysql":
        # MySQL's default sql_mode treats backslash as an escape character
        # inside string literals, so a literal backslash must be doubled or
        # 'C:\temp' would silently become 'C:temp'. NUL is escaped too so the
        # generated script stays plain text.
        text = text.replace("\\", "\\\\").replace("\x00", "\\0")
    # str and anything else: single-quote with '' escaping
    if dialect == "mssql":
        text = _TSQL_LINE_CONTINUATION.sub(_escape_tsql_line_continuation, text)
        if "\x00" in text:
            return _mssql_nul_literal(text)
    prefix = "N" if dialect == "mssql" else ""
    return prefix + "'" + text.replace("'", "''") + "'"


# T-SQL treats a backslash before a line break inside a string literal as a
# line continuation: it drops the backslash together with LF or CRLF (but
# keeps a lone CR), so 'C:\dir\<newline>x' would lose characters.
_TSQL_LINE_CONTINUATION = re.compile(r"\\(\r\n|\n|\r)")


def _escape_tsql_line_continuation(match):
    """Double the backslash (and an LF/CRLF the server would also drop) so
    exactly the original backslash and line break survive."""
    line_break = match.group(1)
    return "\\\\" + line_break + ("" if line_break == "\r" else line_break)


def _mssql_nul_literal(text):
    """T-SQL expression for text containing NUL, which can't appear in SQL
    text sent through pymssql (it truncates there). NUL is written as an
    unused private-use character and swapped back with a binary-collation
    REPLACE, so the expression stays a single literal however many NULs."""
    marker = next(chr(cp) for cp in range(0xE000, 0xF900) if chr(cp) not in text)
    body = text.replace("\x00", marker).replace("'", "''")
    return (
        f"REPLACE(CAST(N'{body}' AS nvarchar(max)) COLLATE Latin1_General_BIN2, "
        f"NCHAR({ord(marker)}), NCHAR(0))"
    )


def format_insert(rows, headers, table_ref, dialect="mssql", datetime_timespec="milliseconds"):
    """Render result rows as a series of INSERT statements."""
    if not rows:
        return "-- (no rows)"
    col_list = ", ".join(_quote_identifier_part(h, dialect) for h in headers)
    lines = []
    for row in rows:
        vals = ", ".join(sql_literal(v, dialect, datetime_timespec) for v in row)
        lines.append(f"INSERT INTO {table_ref} ({col_list}) VALUES ({vals});")
    return "\n".join(lines)


def _csv_export_field(text, delimiter, quote_empty):
    if (text == "" and quote_empty) or any(c in text for c in (delimiter, '"', "\r", "\n")):
        return '"' + text.replace('"', '""') + '"'
    return text


def _csv_export_line(row, delimiter, null_text):
    """Format one CSV data row like csv.writer, except that an empty string
    is written as "" so it stays distinct from NULL (written as null_text,
    unquoted; an all-NULL one-column row is therefore a blank line)."""
    fields = []
    for value in row:
        if value is None:
            fields.append(_csv_export_field(null_text, delimiter, False))
        else:
            fields.append(_csv_export_field(str(_hex_encode_bytes(value)), delimiter, True))
    return delimiter.join(fields) + "\r\n"


# ── Import Helpers ─────────────────────────────────────────────────────────────

class _RowSource:
    """Wraps a streaming row generator together with the file handle it
    reads from, so close() always releases the handle even when the caller
    never iterated a single row (e.g. import_data rejects the target table's
    columns before its main loop ever runs).

    A bare generator's own try/finally only executes once next() has been
    called at least once; close()ing (or garbage-collecting) a generator
    that never started running is a documented no-op that skips the
    finally entirely, which would otherwise leak the underlying file
    descriptor on every validation failure that happens before the first
    row is consumed."""

    def __init__(self, generator, file_handle):
        self._generator = generator
        self._file = file_handle

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._generator)

    def close(self):
        self._generator.close()
        if not self._file.closed:
            self._file.close()


def _iter_csv_records(lines, delimiter):
    """Parse CSV like csv.reader (QUOTE_MINIMAL, doubled quotes) but yield
    None for an unquoted empty field and "" for a quoted one ("").

    csv.reader only reports that difference from Python 3.13 on, and it is
    the only way a CSV file can tell NULL apart from an empty string.
    `lines` must come from a file opened with newline="" so line endings,
    including those inside quoted fields, are preserved."""
    limit = MAX_CSV_IMPORT_FIELD_CHARS

    def _finish(field, quoted):
        text = "".join(field)
        if len(text) > limit:
            raise csv.Error(f"field larger than field limit ({limit})")
        return text if field or quoted else None

    record = []
    field = []
    quoted = False
    in_quotes = False
    for line in lines:
        if not in_quotes and not field and not record and '"' not in line:
            # Fast path: a whole record with no quoting on one line.
            body = line.rstrip("\r\n")
            if not body:
                yield []
                continue
            values = body.split(delimiter)
            if any(len(v) > limit for v in values):
                raise csv.Error(f"field larger than field limit ({limit})")
            yield [v if v else None for v in values]
            continue
        i, n = 0, len(line)
        while i < n:
            ch = line[i]
            if in_quotes:
                if ch == '"':
                    if i + 1 < n and line[i + 1] == '"':
                        field.append('"')
                        i += 2
                        continue
                    in_quotes = False
                    i += 1
                else:
                    # Copy up to the next quote in one step; large quoted
                    # fields would otherwise cost a Python iteration per char.
                    end = line.find('"', i)
                    end = n if end < 0 else end
                    field.append(line[i:end])
                    i = end
                continue
            if ch == delimiter:
                record.append(_finish(field, quoted))
                field, quoted = [], False
            elif ch == '"' and not field and not quoted:
                in_quotes = quoted = True
            elif ch in "\r\n":
                if ch == "\r" and i + 1 < n and line[i + 1] == "\n":
                    i += 1
                if record or field or quoted:
                    record.append(_finish(field, quoted))
                yield record
                record, field, quoted = [], [], False
            else:
                field.append(ch)
            i += 1
    # Like csv.reader (non-strict), an unterminated quoted field ends at EOF.
    if record or field or quoted:
        record.append(_finish(field, quoted))
        yield record


def read_csv_data(filepath, no_header, delimiter, skip, db_col_names):
    """
    Open a CSV file and return (generator_of_rows, column_list).
    The generator streams rows so large files are never fully loaded into RAM.
    Unquoted empty fields are yielded as None (maps to SQL NULL); a quoted
    empty field ("") is yielded as "" (see _iter_csv_records).
    (See read_json_data: JSON Lines streams the same way, but a plain JSON
    array does not — it is fully parsed into memory before rows are yielded.)
    """
    f = open(filepath, newline="", encoding="utf-8-sig")  # utf-8-sig strips BOM
    try:
        reader = _iter_csv_records(f, delimiter)

        if no_header:
            columns = list(db_col_names)
        else:
            header_row = next(reader)
            columns = [(h or "").strip() for h in header_row]
            seen = set()
            duplicates = []
            for col in columns:
                key = col.lower()
                if key in seen and col not in duplicates:
                    duplicates.append(col)
                seen.add(key)
            if duplicates:
                raise ValueError(f"CSV header contains duplicate column(s): {duplicates}")
    except StopIteration:
        f.close()
        raise ValueError("CSV file is empty")
    except BaseException:
        f.close()
        raise

    def _gen():
        skipped = 0
        try:
            for row in reader:
                if skipped < skip:
                    skipped += 1
                    continue
                values = row
                if not values and len(columns) == 1:
                    # A one-column row holding NULL is written as a blank line.
                    values = [None]
                if no_header:
                    yield tuple(values)
                else:
                    yield {
                        "__row__": tuple(values),
                        "__row_len__": len(values),
                        "__data__": dict(zip(columns, values)),
                    }
        finally:
            f.close()

    return _RowSource(_gen(), f), columns


def read_json_data(filepath, skip, fmt=""):
    """
    Read a JSON file (array of objects) or JSON Lines file (.jsonl).
    Returns (generator_of_rows, column_list).
    Columns are taken from the keys of the first object.
    JSON null → None (SQL NULL).

    JSON Lines streams one object per line and never loads the whole file into
    RAM. A plain JSON array does NOT stream: the entire file is parsed with
    json.load() up front (the format requires seeing the closing ']' to know
    the array is well-formed), so very large .json array imports should be
    converted to .jsonl first if memory is a concern.
    """
    path = Path(filepath)
    ext = path.suffix.lower()
    effective_fmt = fmt or ("jsonl" if ext == ".jsonl" else "json")

    if effective_fmt == "jsonl":
        # JSON Lines: one JSON object per line — fully streamable
        f = open(filepath, encoding="utf-8")
        try:
            first_obj = None
            skipped = 0
            for raw_line in f:
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                if skipped < skip:
                    skipped += 1
                    continue
                first_obj = json.loads(raw_line)
                break
            if first_obj is None:
                # Empty file (e.g. an export of an empty table): no rows.
                f.close()
                return iter(()), []
            if not isinstance(first_obj, dict):
                raise ValueError("JSONL elements must be JSON objects")
            columns = list(first_obj.keys())
            f.seek(0)
        except BaseException:
            f.close()
            raise

        def _gen_jsonl():
            skipped = 0
            try:
                for raw_line in f:
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    if skipped < skip:
                        skipped += 1
                        continue
                    obj = json.loads(raw_line)
                    yield obj
            finally:
                f.close()

        return _RowSource(_gen_jsonl(), f), columns

    else:
        # Standard JSON array — can't stream, so cap the read the same way
        # MAX_SQL_INPUT_BYTES bounds SQL text, instead of letting json.load()
        # exhaust memory on a huge or malformed file before reporting anything.
        with open(filepath, "rb") as f:
            raw = f.read(MAX_JSON_ARRAY_IMPORT_BYTES + 1)
        if len(raw) > MAX_JSON_ARRAY_IMPORT_BYTES:
            raise ValueError(
                f"JSON array import exceeds the "
                f"{MAX_JSON_ARRAY_IMPORT_BYTES // (1024 * 1024)} MiB limit; "
                "convert to .jsonl to stream large imports"
            )
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError("JSON file must contain a top-level array of objects")
        if skip >= len(data):  # includes [] (an export of an empty table)
            return iter(()), []
        if not isinstance(data[skip], dict):
            raise ValueError("JSON array elements must be objects")
        columns = list(data[skip].keys())

        def _gen_json():
            skipped = 0
            for obj in data:
                if skipped < skip:
                    skipped += 1
                    continue
                yield obj

        return _gen_json(), columns


def _split_dot_command_args(arg):
    """Split shell-style arguments, treating Windows backslashes literally."""
    lexer = shlex.shlex(arg, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    if os.name == "nt":
        lexer.escape = ""
    return list(lexer)


def _close_row_source(row_gen):
    """Close a streaming import generator when validation exits early."""
    close = getattr(row_gen, "close", None)
    if close is not None:
        try:
            close()
        except Exception:
            pass


def _safe_rollback(conn, ctx, reason):
    """Attempt rollback without allowing a dead connection to mask *reason*."""
    try:
        if ((ctx.get("driver_name") == "pymssql"
             and getattr(conn, "autocommit_state", False) is True)
                or (ctx.get("driver_name") == "pyodbc"
                    and conn.autocommit is True)):
            # A failed batch may have executed BEGIN before we could update
            # ctx. Driver rollback() is a no-op in autocommit mode.
            cur = conn.cursor()
            try:
                cur.execute("IF @@TRANCOUNT > 0 ROLLBACK TRAN")
            finally:
                try:
                    cur.close()
                except DBError:
                    pass
        else:
            conn.rollback()
    except DBError as rollback_error:
        ctx["connection_usable"] = False
        print(
            f"Rollback failed after {reason}: {rollback_error}. "
            "Reconnect before issuing more SQL.",
            file=sys.stderr,
        )
        return False
    ctx["transaction_depth"] = 0
    ctx["transaction_open"] = False
    ctx["savepoints"] = set()
    return True


def _connection_ready(ctx):
    if ctx.get("connection_usable", True):
        return True
    print("Connection is unusable. Run .reconnect before issuing more SQL.",
          file=sys.stderr)
    return False


def _restore_autocommit(conn, ctx, reason):
    # Discard pending work before restoring autocommit, including pymssql's
    # empty driver transaction after commit()/rollback().
    try:
        conn.rollback()
        _set_autocommit(conn, ctx, True)
    except DBError as error:
        ctx["connection_usable"] = False
        print(f"Error restoring autocommit after {reason}: {error}. "
              "Reconnect before issuing more SQL.", file=sys.stderr)
        return False
    return True


def _set_autocommit(conn, ctx, enabled):
    if ctx.get("driver_name") == "pyodbc":
        conn.autocommit = enabled
    else:
        conn.autocommit(enabled)


def _mssql_owned_work(ctx, operation):
    """Give ordinary SQL/import work a driver transaction, then return to idle."""
    conn = ctx["conn"]
    if ctx.get("implicit_transactions"):
        # With SET IMPLICIT_TRANSACTIONS ON the driver's BEGIN would nest
        # inside the session's own transaction, so its COMMIT would make
        # nothing durable and restoring autocommit would roll everything
        # back. Leave commits to the user, as sqlcmd does.
        result = operation()
        _resync_transaction_state(conn, ctx)
        return result
    try:
        _set_autocommit(conn, ctx, False)
    except DBError as error:
        ctx["connection_usable"] = False
        print(f"Error starting transaction: {error}. Reconnect.", file=sys.stderr)
        return False
    try:
        result = operation()
    finally:
        restored = _restore_autocommit(conn, ctx, "database operation")
    return result if restored else False


# Reserved T-SQL words that can only begin another statement (or a syntax
# error) when they appear unquoted in a single SELECT. SQL Server needs no ";"
# between statements, and SQLGlot reads some of them as aliases: it parses
# "SELECT v FROM t EXEC('DELETE FROM t')" as table t aliased EXEC, while SQL
# Server runs the EXEC.
_TSQL_STATEMENT_KEYWORDS = frozenset({
    "ALTER", "BACKUP", "BEGIN", "BULK", "CHECKPOINT", "COMMIT", "CREATE", "DBCC",
    "DECLARE", "DELETE", "DENY", "DROP", "EXEC", "EXECUTE", "GRANT", "IF", "INSERT",
    "KILL", "MERGE", "PRINT", "RAISERROR", "RECONFIGURE", "RESTORE", "REVERT",
    "REVOKE", "ROLLBACK", "SAVE", "SET", "SETUSER", "SHUTDOWN", "TRUNCATE",
    "UPDATE", "USE", "WHILE",
})


def _validate_export_query(sql, dialect):
    """Validate the complete query, including boundaries without semicolons."""
    try:
        from sqlglot import exp, parse, tokenize
        from sqlglot.errors import ParseError, TokenError
    except ImportError:
        raise ValueError(
            "Custom export queries require SQLGlot. Run: "
            "pip install -r requirements-query.txt"
        ) from None
    read_dialect = "tsql" if dialect == "mssql" else "mysql"
    if dialect == "mssql":
        keywords = set(_sql_parser_tokens(sql, dialect)) & _TSQL_STATEMENT_KEYWORDS
        if keywords:
            raise ValueError(
                f"--query must be a read-only SELECT statement (found {min(keywords)}; "
                "quote it as [name] if it is an identifier)"
            )
    try:
        tokens = tokenize(sql, read=read_dialect)
        # MySQL executes these comments as SQL, while parsers may discard
        # their contents. Token comments exclude lookalikes inside strings.
        if dialect == "mysql" and any(
            comment.startswith(("!", "M!"))
            for token in tokens for comment in token.comments
        ):
            raise ValueError("--query must not contain executable comments")
        # SQLGlot permits DELETE as an unquoted SELECT alias. SQL Server can
        # instead execute it as the next statement without a semicolon.
        if any(token.token_type.name == "DELETE" for token in tokens):
            raise ValueError("--query must be a read-only SELECT statement")
        statements = [node for node in parse(sql, read=read_dialect)
                      if node is not None]
    except (ParseError, TokenError) as error:
        raise ValueError("--query must contain a single read-only SELECT") from error
    query = statements[0] if len(statements) == 1 else None
    while isinstance(query, exp.Subquery):  # "(SELECT ...)" is a valid statement
        query = query.this
    if not isinstance(query, (exp.Select, exp.SetOperation)):
        raise ValueError("--query must contain a single read-only SELECT")
    writes = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create,
              exp.Drop, exp.Alter, exp.Command, exp.Into)
    if statements[0].find(*writes) is not None:
        raise ValueError("--query must be a read-only SELECT statement")


def export_data(arg, ctx):
    """
    Handle the .export dot command.
    Syntax: .export TABLE FILE [OPTIONS]
            .export - FILE --query "SELECT ..."
    """
    if not _connection_ready(ctx):
        return
    conn = ctx["conn"]
    dialect = _dialect(ctx)

    try:
        parts = _split_dot_command_args(arg)
    except ValueError as e:
        print(f".export parse error: {e}", file=sys.stderr)
        return

    p = argparse.ArgumentParser(prog=".export", add_help=False, exit_on_error=False)
    p.add_argument("table",    nargs="?", default="",
                   help="Table to export (use - with --query for arbitrary SELECT)")
    p.add_argument("file",
                   help="Destination file path, or - for stdout")
    p.add_argument("--format",    choices=["csv", "json", "jsonl"], default="",
                   metavar="FMT")
    p.add_argument("--query",     default="", metavar="SQL",
                   help="Custom SELECT statement (overrides TABLE)")
    p.add_argument("--where",     default="", metavar="CONDITION")
    p.add_argument("--columns",   default="", metavar="col1,col2,...",
                   help="Columns to include (default: *)")
    p.add_argument("--order-by",  default="", metavar="EXPR", dest="order_by")
    p.add_argument("--delimiter", default=",", metavar="CHAR",
                   help="CSV field delimiter (default: ,)")
    p.add_argument("--no-header", action="store_true",
                   help="Omit header row (CSV only)")
    p.add_argument("--null-as",   default=None, metavar="STR", dest="null_as",
                   help="Represent NULL as this string (CSV default: empty; JSON default: null)")
    p.add_argument("--chunk",     type=int, default=500, metavar="N",
                   help="Rows fetched per round-trip (default: 500)")
    p.add_argument("--append",    action="store_true",
                   help="Append to existing file instead of overwriting")

    try:
        opts = p.parse_args(parts)
    except (argparse.ArgumentError, SystemExit):
        print(
            "Usage: .export TABLE FILE [--format csv|json|jsonl] "
            "[--query SQL] [--where COND] [--columns col1,...] "
            "[--order-by EXPR] [--delimiter CHAR] [--no-header] "
            "[--null-as STR] [--chunk N] [--append]",
            file=sys.stderr,
        )
        return
    if opts.chunk <= 0:
        print("Error: --chunk must be greater than 0.", file=sys.stderr)
        return
    if len(opts.delimiter) != 1:
        print("Error: --delimiter must be exactly one character.", file=sys.stderr)
        return

    # ── Require either a table name or --query ────────────────────────────────
    if not opts.table and not opts.query:
        print("Error: provide a TABLE name or --query SQL.", file=sys.stderr)
        return
    if opts.table == "-" and not opts.query:
        print("Error: use - only with --query SQL.", file=sys.stderr)
        return
    if opts.table and opts.table != "-" and opts.query:
        print("Error: use TABLE or --query, not both.", file=sys.stderr)
        return

    # ── Auto-detect format from file extension ────────────────────────────────
    fmt = opts.format
    if not fmt and opts.file not in ("-", "stdout"):
        ext = Path(opts.file).suffix.lower()
        fmt = {".csv": "csv", ".json": "json", ".jsonl": "jsonl"}.get(ext, "")
    if not fmt:
        print(
            f"Cannot detect format from '{opts.file}'. Add --format csv|json|jsonl",
            file=sys.stderr,
        )
        return
    if opts.append and fmt == "json":
        print(
            "Error: --append is not supported for JSON array output. "
            "Use --format jsonl or overwrite the file.",
            file=sys.stderr,
        )
        return

    # ── Normalize table name before opening anything ──────────────────────────
    # Use the same dbo default as .import so a bare table name resolves the
    # same way in both commands regardless of the session's default schema.
    # (MySQL has no schemas: a bare name means the current database.)
    table_ref = None
    if not opts.query:
        try:
            _db, _schema, _tbl = _qualify_table_parts(
                opts.table, default_schema="dbo", dialect=dialect
            )
            table_ref = _table_ref(_db, _schema, _tbl, dialect)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return

    # ── Open output file (or stdout) before running the query ───────────────────
    to_stdout = opts.file in ("-", "stdout")
    should_write_header = not opts.no_header
    start_offset = 0  # file position before this export wrote anything (for rollback)
    replace_target = None
    temp_output_path = None
    if to_stdout:
        out_f = sys.stdout
    else:
        file_mode = "a" if opts.append else "w"
        try:
            # newline="" on all formats: csv.writer needs it; JSON/JSONL use it
            # to keep \n line endings consistent across platforms.
            open_kwargs = {"newline": "", "encoding": "utf-8"}
            if opts.append:
                out_f = open(opts.file, file_mode, **open_kwargs)
            else:
                target_path = Path(opts.file)
                target_dir = target_path.parent if str(target_path.parent) else Path(".")
                tmp = tempfile.NamedTemporaryFile(
                    "w",
                    delete=False,
                    dir=target_dir,
                    prefix=f".{target_path.name}.",
                    suffix=".tmp",
                    **open_kwargs,
                )
                out_f = tmp
                temp_output_path = Path(tmp.name)
                replace_target = target_path
            try:
                start_offset = out_f.tell()
            except (OSError, ValueError):
                start_offset = 0
            if fmt == "csv" and opts.append:
                should_write_header = start_offset == 0 and not opts.no_header
        except OSError as e:
            print(f"Error opening '{opts.file}': {e}", file=sys.stderr)
            return

    def _cleanup_output():
        if not to_stdout:
            try:
                out_f.close()
            except OSError:
                pass
            if temp_output_path is not None:
                try:
                    temp_output_path.unlink()
                except OSError:
                    pass

    def _abort(msg):
        _cleanup_output()
        print(msg, file=sys.stderr)

    # ── Build the SELECT statement ────────────────────────────────────────────
    # --query is intentionally operator-supplied SQL, but must be a single SELECT.
    # Table export quotes --columns and rejects stacked --where/--order-by batches.
    # --where and --order-by remain SQL fragments; do not pass untrusted input here.
    if opts.query:
        select_sql = opts.query.strip().rstrip(";")
        if not select_sql:
            _abort("Error: --query must not be empty.")
            return
        if ";" in _sql_parser_tokens(select_sql, dialect):  # not inside strings/comments
            _abort("Error: --query must contain a single SELECT statement.")
            return
        first_token = _first_sql_token(select_sql, dialect)
        if (
            first_token not in ("SELECT", "WITH")
            or _starts_with_data_change_statement(select_sql, dialect)
            or _has_top_level_select_into(select_sql, dialect)
        ):
            _abort("Error: --query must be a read-only SELECT statement.")
            return
        try:
            _validate_export_query(select_sql, dialect)
        except ValueError as error:
            _abort(f"Error: {error}")
            return
    else:
        try:
            col_expr = _quote_column_list(opts.columns, dialect) if opts.columns else "*"
            _reject_stacked_sql_fragment("--where", opts.where)
            _reject_stacked_sql_fragment("--order-by", opts.order_by)
        except ValueError as e:
            _abort(f"Error: {e}")
            return
        where     = f" WHERE {opts.where}" if opts.where else ""
        order     = f" ORDER BY {opts.order_by}" if opts.order_by else ""
        select_sql = f"SELECT {col_expr} FROM {table_ref}{where}{order}"

    # ── Run the query ─────────────────────────────────────────────────────────
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(select_sql)
    except DBError as e:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        _abort(f"Query error: {e}")
        return
    except KeyboardInterrupt:
        # Interrupted while the query was still running server-side (a real
        # possibility on a large/slow table). Clean up quietly — the output
        # file/temp file was already opened above but no rows were written —
        # and let the caller's own interrupt handling take over.
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        _cleanup_output()
        raise

    if not cur.description:
        try:
            cur.close()
        except Exception:
            pass
        _abort("Query returned no columns.")
        return

    headers = [d[0] for d in cur.description]
    if fmt in ("json", "jsonl"):
        try:
            _validate_json_headers(headers)
        except ResultFormatError as error:
            try:
                cur.close()
            except DBError:
                pass
            _abort(f"Export error: {error}")
            return

    # ── Null sentinel per format ──────────────────────────────────────────────
    # For CSV: default null → empty string
    # For JSON: default null → Python None (becomes JSON null)
    csv_null  = opts.null_as if opts.null_as is not None else ""
    json_null = opts.null_as  # None means keep as Python None → JSON null

    def _json_val(v):
        v = _json_safe(v)
        if v is None:
            return json_null
        return v

    # ── Stream and write ──────────────────────────────────────────────────────
    label = opts.file if not to_stdout else "stdout"
    if not to_stdout:
        print(
            f"Exporting to '{label}' ({fmt}) ...",
            file=sys.stderr,
        )

    t0      = time.perf_counter()
    total   = 0
    success = False
    close_error = None

    try:
        if fmt == "csv":
            writer = csv.writer(out_f, delimiter=opts.delimiter)
            if should_write_header:
                writer.writerow(headers)
            while True:
                chunk = cur.fetchmany(opts.chunk)
                if not chunk:
                    break
                for row in chunk:
                    out_f.write(_csv_export_line(row, opts.delimiter, csv_null))
                total += len(chunk)
                if not to_stdout:
                    print(f"\r  {total:,} rows...", end="", flush=True,
                          file=sys.stderr)

        elif fmt == "jsonl":
            while True:
                chunk = cur.fetchmany(opts.chunk)
                if not chunk:
                    break
                for row in chunk:
                    obj = {h: _json_val(v) for h, v in zip(headers, row)}
                    out_f.write(json.dumps(obj, default=str) + "\n")
                total += len(chunk)
                if not to_stdout:
                    print(f"\r  {total:,} rows...", end="", flush=True,
                          file=sys.stderr)

        else:  # json array — stream with manual bracketing
            out_f.write("[\n")
            first = True
            while True:
                chunk = cur.fetchmany(opts.chunk)
                if not chunk:
                    break
                for row in chunk:
                    obj = {h: _json_val(v) for h, v in zip(headers, row)}
                    sep = "" if first else ",\n"
                    out_f.write(sep + json.dumps(obj, default=str))
                    first = False
                total += len(chunk)
                if not to_stdout:
                    print(f"\r  {total:,} rows...", end="", flush=True,
                          file=sys.stderr)
            out_f.write("\n]\n")

        success = True

    except (*DBError, OSError) as e:
        # Roll back whatever this run wrote (back to start_offset, which honors
        # --append) so a failed export never leaves a partial file that looks
        # complete — for any format, not just the JSON array. stdout can't seek.
        # Temp-file (overwrite-mode) cleanup happens in the finally block below.
        if not to_stdout and temp_output_path is None:
            try:
                out_f.seek(start_offset)
                out_f.truncate()
            except OSError:
                pass
        print(f"\nExport error after {total:,} rows: {e}", file=sys.stderr)

    except KeyboardInterrupt:
        # Same truncate-back as a reported error above, so --append never
        # leaves a partially-written record in an existing file. No message
        # here — the caller's own interrupt handling reports the interruption.
        if not to_stdout and temp_output_path is None:
            try:
                out_f.seek(start_offset)
                out_f.truncate()
            except OSError:
                pass
        raise

    finally:
        try:
            cur.close()
        except Exception:
            pass
        if not to_stdout:
            try:
                out_f.close()
            except OSError as e:
                close_error = e
                success = False
        if not success and temp_output_path is not None:
            try:
                temp_output_path.unlink()
            except OSError:
                pass

    if close_error is not None:
        print(f"\nExport error closing '{label}': {close_error}", file=sys.stderr)
        return

    if success:
        if temp_output_path is not None:
            try:
                os.replace(temp_output_path, replace_target)
            except OSError as e:
                try:
                    temp_output_path.unlink()
                except OSError:
                    pass
                print(f"\nExport error replacing '{replace_target}': {e}", file=sys.stderr)
                return
        elapsed = time.perf_counter() - t0
        rate    = total / elapsed if elapsed > 0 else 0
        if not to_stdout:
            print(
                f"\nExported {total:,} rows to '{label}' in {elapsed:.2f}s "
                f"({rate:,.0f} rows/sec).",
                file=sys.stderr,
            )


def _truncate_table_sql(table_ref, ctx):
    """SQL for .import --truncate. MySQL's TRUNCATE TABLE is DDL: it commits
    implicitly (including any explicit transaction the user has open) and
    can't roll back with a failed first batch, so use DELETE there to keep the
    same all-or-nothing behavior SQL Server's transactional TRUNCATE gives."""
    if _dialect(ctx) == "mysql":
        return f"DELETE FROM {table_ref}"
    return f"TRUNCATE TABLE {table_ref}"


def import_data(arg, ctx):
    """
    Handle the .import dot command.
    Syntax: .import FILE TABLE [--format csv|json|jsonl] [--batch N] [--skip N]
                               [--truncate] [--no-header] [--delimiter CHAR]
                               [--columns col1,col2,...] [--on-error skip|abort]
                               [--keep-identity]
    """
    if not _connection_ready(ctx):
        return
    if (ctx.get("driver_name") in ("pymssql", "pyodbc")
            and ctx.get("transaction_depth", 0) <= 0):
        return _mssql_owned_work(ctx, lambda: _import_data(arg, ctx))
    # MySQL connections run with server-side autocommit (see _connect_mysql),
    # but import relies on batches being atomic and on rollback after a
    # failed batch, exactly as with SQL Server. So turn autocommit off for the
    # duration — unless the user already has an explicit transaction open, in
    # which case the import joins it and never commits on its own.
    # If the user turned autocommit off themselves (SET autocommit = 0),
    # leave it off: restoring it afterwards would silently change their
    # session. get_autocommit() reads cached server status (no round trip).
    conn = ctx["conn"]
    if _dialect(ctx) == "mysql":
        try:
            _refresh_mysql_transaction_status(conn, ctx)
        except DBError as error:
            ctx["connection_usable"] = False
            print(f"Error checking import transaction: {error}. Reconnect.",
                  file=sys.stderr)
            return
    suspend_autocommit = (
        _dialect(ctx) == "mysql"
        and ctx.get("transaction_depth", 0) <= 0
        and conn.get_autocommit()
    )
    if suspend_autocommit:
        try:
            conn.autocommit(False)
        except DBError as e:
            print(f"Error starting import transaction: {e}", file=sys.stderr)
            return
    try:
        _import_data(arg, ctx)
    finally:
        if suspend_autocommit:
            _restore_autocommit(conn, ctx, "import")


def _import_data(arg, ctx):
    conn = ctx["conn"]
    dialect = _dialect(ctx)

    # Parse sub-command arguments with shlex so quoted paths work
    try:
        parts = _split_dot_command_args(arg)
    except ValueError as e:
        print(f".import parse error: {e}", file=sys.stderr)
        return

    p = argparse.ArgumentParser(prog=".import", add_help=False, exit_on_error=False)
    p.add_argument("file")
    p.add_argument("table")
    p.add_argument("--format",    choices=["csv", "json", "jsonl"], default="")
    p.add_argument("--batch",     type=int, default=1000, metavar="N")
    p.add_argument("--skip",      type=int, default=0,    metavar="N")
    p.add_argument("--truncate",  action="store_true")
    p.add_argument("--no-header", action="store_true")
    p.add_argument("--delimiter", default=",", metavar="CHAR")
    p.add_argument("--columns",   default="",  metavar="col1,col2,...")
    p.add_argument("--on-error",  choices=["skip", "abort"], default="abort",
                   dest="on_error")
    p.add_argument("--keep-identity", action="store_true", dest="keep_identity")

    try:
        opts = p.parse_args(parts)
    except (argparse.ArgumentError, SystemExit):
        print(
            "Usage: .import FILE TABLE [--format csv|json|jsonl] [--batch N] "
            "[--skip N] [--truncate] [--no-header] [--delimiter CHAR] "
            "[--columns col1,...] [--on-error skip|abort] [--keep-identity]",
            file=sys.stderr,
        )
        return
    if opts.batch <= 0:
        print("Error: --batch must be greater than 0.", file=sys.stderr)
        return
    if opts.skip < 0:
        print("Error: --skip must be greater than or equal to 0.", file=sys.stderr)
        return
    if len(opts.delimiter) != 1:
        print("Error: --delimiter must be exactly one character.", file=sys.stderr)
        return

    # ── Auto-detect format ────────────────────────────────────────────────────
    fmt = opts.format
    if not fmt:
        ext = Path(opts.file).suffix.lower()
        fmt = {".csv": "csv", ".json": "json", ".jsonl": "jsonl"}.get(ext, "")
        if not fmt:
            print(
                f"Cannot detect format from '{opts.file}'. "
                "Add --format csv|json|jsonl",
                file=sys.stderr,
            )
            return

    # ── Resolve table → schema + table (strip any user-supplied brackets) ────
    try:
        database_name, schema_name, table_name = _qualify_table_parts(
            opts.table, default_schema="dbo", dialect=dialect
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return

    # ── Fetch table column metadata ───────────────────────────────────────────
    try:
        cur = conn.cursor()
    except DBError as e:
        print(f"Error opening database cursor: {e}", file=sys.stderr)
        return
    columns_view = (
        f"{_quote_identifier_part(database_name)}.INFORMATION_SCHEMA.COLUMNS"
        if database_name else
        "INFORMATION_SCHEMA.COLUMNS"
    )
    table_ref = _table_ref(database_name, schema_name, table_name, dialect)
    if schema_name is None:
        # MySQL bare table name: look it up in the current database.
        schema_filter, meta_params = "DATABASE()", (table_name,)
    else:
        schema_filter, meta_params = PARAM_PLACEHOLDER, (table_name, schema_name)
    # The 4th column flags generated columns (rowversion/computed on SQL
    # Server, VIRTUAL/STORED GENERATED on MySQL), whose values the server
    # computes and refuses to accept in an INSERT; the 5th flags SQL Server
    # IDENTITY columns, which need SET IDENTITY_INSERT (--keep-identity).
    if dialect == "mysql":
        generated_expr = "CASE WHEN c.EXTRA IN ('VIRTUAL GENERATED', 'STORED GENERATED') THEN 1 ELSE 0 END, 0"
        generated_join = ""
    else:
        sys_columns = (
            f"{_quote_identifier_part(database_name)}.sys.columns" if database_name else "sys.columns"
        )
        generated_expr = (
            "CASE WHEN c.DATA_TYPE = 'timestamp' OR sc.is_computed = 1 THEN 1 ELSE 0 END, "
            "CASE WHEN sc.is_identity = 1 THEN 1 ELSE 0 END"
        )
        generated_join = (
            f"LEFT JOIN {sys_columns} sc ON sc.object_id = OBJECT_ID(QUOTENAME(c.TABLE_CATALOG) "
            "+ '.' + QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)) "
            "AND sc.name = c.COLUMN_NAME "
        )
    try:
        cur.execute(
            _adapt_placeholders(
                f"SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, {generated_expr} "
                f"FROM {columns_view} c {generated_join}"
                f"WHERE c.TABLE_NAME = {PARAM_PLACEHOLDER} AND c.TABLE_SCHEMA = {schema_filter} "
                f"ORDER BY c.ORDINAL_POSITION",
                ctx,
            ),
            _adapt_params(meta_params, ctx),
        )
        db_cols = cur.fetchall()
    except DBError as e:
        try: cur.close()
        except Exception: pass
        print(f"Error reading table metadata: {e}", file=sys.stderr)
        return
    if not db_cols:
        try: cur.close()
        except Exception: pass
        print(
            f"Error: table '{table_ref}' not found "
            "or has no columns.",
            file=sys.stderr,
        )
        return
    if opts.truncate and dialect == "mysql":
        # --truncate is DELETE FROM on MySQL (see _truncate_table_sql), which
        # would apply other tables' ON DELETE CASCADE / SET NULL rules. Refuse
        # wherever TRUNCATE TABLE itself would: when another table's foreign
        # key references this one (self-references are allowed) and
        # foreign_key_checks is on — with checks off, DELETE doesn't cascade.
        try:
            cur.execute(
                _adapt_placeholders(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS "
                    f"WHERE REFERENCED_TABLE_NAME = {PARAM_PLACEHOLDER} "
                    f"AND UNIQUE_CONSTRAINT_SCHEMA = {schema_filter} "
                    "AND NOT (TABLE_NAME = REFERENCED_TABLE_NAME "
                    "AND CONSTRAINT_SCHEMA = UNIQUE_CONSTRAINT_SCHEMA) "
                    "AND @@foreign_key_checks = 1",
                    ctx,
                ),
                meta_params,
            )
            fk_rows = cur.fetchall()
        except DBError as e:
            try: cur.close()
            except Exception: pass
            print(f"Error reading foreign key metadata: {e}", file=sys.stderr)
            return
        if fk_rows and fk_rows[0][0]:
            try: cur.close()
            except Exception: pass
            print(
                f"Error: cannot --truncate {table_ref}: it is referenced by a "
                "FOREIGN KEY constraint from another table.",
                file=sys.stderr,
            )
            return

    db_col_names  = [r[0] for r in db_cols]
    db_col_lower  = {c.lower(): c for c in db_col_names}   # lower → canonical

    # ── Open file and build row generator ─────────────────────────────────────
    row_gen = None
    try:
        if fmt == "csv":
            row_gen, file_cols = read_csv_data(
                opts.file, opts.no_header, opts.delimiter, opts.skip, db_col_names
            )
        else:  # json / jsonl
            row_gen, file_cols = read_json_data(opts.file, opts.skip, fmt=fmt)
    except json.JSONDecodeError as e:
        try: cur.close()
        except Exception: pass
        print(f"\nError reading '{opts.file}': invalid JSON: {e}", file=sys.stderr)
        return
    except OSError as e:
        try: cur.close()
        except Exception: pass
        print(f"Error opening '{opts.file}': {e}", file=sys.stderr)
        return
    except (ValueError, csv.Error) as e:
        try: cur.close()
        except Exception: pass
        print(f"Error reading '{opts.file}': {e}", file=sys.stderr)
        return

    if fmt in ("json", "jsonl") and not file_cols and opts.skip > 0:
        print(
            f"Warning: --skip {opts.skip} leaves no rows in '{opts.file}' "
            "to import.",
            file=sys.stderr,
        )

    # ── Resolve final column list ─────────────────────────────────────────────
    if opts.columns:
        insert_cols = [
            _normalize_identifier_part(c.strip(), dialect)
            for c in _split_column_list(opts.columns, dialect)
        ]
    elif fmt in ("json", "jsonl") and not file_cols:
        insert_cols = db_col_names
    else:
        insert_cols = file_cols

    # Validate every requested column actually exists in the table
    bad = [c for c in insert_cols if c.lower() not in db_col_lower]
    if bad:
        _close_row_source(row_gen)
        try: cur.close()
        except Exception: pass
        print(
            f"Error: column(s) not found in {table_ref}: "
            f"{bad}\nUse --columns to specify valid columns.",
            file=sys.stderr,
        )
        return

    # Generated columns (e.g. rowversion or computed columns, which a full
    # export includes) can't be inserted. Skip them unless named in --columns.
    generated_cols = {row[0].lower() for row in db_cols if len(row) > 3 and row[3]}
    generated_positions = set()
    if not opts.columns and generated_cols:
        skipped = [c for c in insert_cols if c.lower() in generated_cols]
        if skipped:
            generated_positions = {
                i for i, c in enumerate(file_cols) if c.lower() in generated_cols
            }
            insert_cols = [c for c in insert_cols if c.lower() not in generated_cols]
            print(
                f"Skipping generated column(s) {skipped}: the server computes their values.",
                file=sys.stderr,
            )
        if not insert_cols:
            _close_row_source(row_gen)
            try: cur.close()
            except Exception: pass
            print(f"Error: no insertable columns in {table_ref}.", file=sys.stderr)
            return

    identity_cols = {row[0].lower() for row in db_cols if len(row) > 4 and row[4]}
    identity_insert = [c for c in insert_cols if c.lower() in identity_cols]
    if identity_insert and not opts.keep_identity:
        _close_row_source(row_gen)
        try: cur.close()
        except Exception: pass
        print(
            f"Error: {identity_insert[0]} is an IDENTITY column of {table_ref}. Add "
            "--keep-identity to insert the file's values, or leave it out with "
            "--columns to let SQL Server assign new ones.",
            file=sys.stderr,
        )
        return

    # Use the canonical (server-cased) column names for the INSERT
    canonical_cols = [db_col_lower[c.lower()] for c in insert_cols]
    # SQL Server CLR types export as their 0x... serialization, which converts
    # back implicitly from binary (a non-0x value such as WKT stays text).
    binary_types = {"binary", "varbinary", "image", "blob", "tinyblob",
                    "mediumblob", "longblob", "geography", "geometry", "hierarchyid"}
    binary_cols = {row[0].lower() for row in db_cols
                   if row[1].lower() in binary_types}
    # A quoted empty CSV field ("") is an empty string only for text columns;
    # anywhere else it means NULL, as an unquoted empty field always does.
    text_types = {"char", "varchar", "nchar", "nvarchar", "text", "ntext", "sysname",
                  "xml", "tinytext", "mediumtext", "longtext", "enum", "set"}
    text_cols = {row[0].lower() for row in db_cols
                 if row[1].lower() in text_types}
    csv_input_cols = None
    if fmt == "csv" and not opts.no_header:
        file_col_lower = {c.lower(): c for c in file_cols}
        missing_file_cols = [c for c in insert_cols if c.lower() not in file_col_lower]
        if missing_file_cols:
            _close_row_source(row_gen)
            try: cur.close()
            except Exception: pass
            print(
                f"Error: column(s) not found in CSV header: {missing_file_cols}",
                file=sys.stderr,
            )
            return
        csv_input_cols = [file_col_lower[c.lower()] for c in insert_cols]
    n_cols   = len(canonical_cols)
    col_list = ", ".join(_quote_identifier_part(c, dialect) for c in canonical_cols)
    placeholders = ", ".join([PARAM_PLACEHOLDER] * n_cols)
    truncate_sql = _truncate_table_sql(table_ref, ctx)

    # ── Optional TRUNCATE ─────────────────────────────────────────────────────
    insert_sql = _adapt_placeholders(
        f"INSERT INTO {table_ref} ({col_list}) "
        f"VALUES ({placeholders})",
        ctx,
    )
    stacked_sql_cache = {}

    def _stacked_insert_sql(count):
        """count single-row INSERTs in one request. Separate statements, not
        one multi-row VALUES list, so each row converts to the column types on
        its own (a VALUES list unifies types across rows: 5 and 'abc' in an
        nvarchar column would fail). TRY/CATCH stops at the first failing row
        and re-raises its error, like inserting the rows one by one."""
        if count not in stacked_sql_cache:
            row_sql = f"INSERT INTO {table_ref} ({col_list}) VALUES ({placeholders});"
            stacked_sql_cache[count] = _adapt_placeholders(
                "BEGIN TRY\n" + "\n".join([row_sql] * count)
                + "\nEND TRY\nBEGIN CATCH\n    THROW;\nEND CATCH",
                ctx,
            )
        return stacked_sql_cache[count]

    def _send_rows(rows):
        """Insert rows, using one round trip per request-sized chunk on SQL Server."""
        if dialect == "mysql":
            # pymysql's executemany already rewrites INSERT ... VALUES into
            # multi-row statements.
            cur.executemany(insert_sql, [_adapt_params(r, ctx) for r in rows])
            return
        rows_per_request = max(1, min(IMPORT_ROWS_PER_REQUEST, IMPORT_PARAMS_PER_REQUEST // n_cols))
        run_count, run_params = 0, []
        i = 0
        while i < len(rows):
            chunk_end, chars = i, 0
            while (chunk_end < len(rows) and chunk_end - i < rows_per_request
                   and (chunk_end == i or chars < IMPORT_CHARS_PER_REQUEST)):
                chars += sum(len(v) if isinstance(v, (str, bytes, bytearray)) else 8
                             for v in rows[chunk_end])
                chunk_end += 1
            chunk = rows[i:chunk_end]
            if run_params and len(chunk) != run_count:
                cur.executemany(_stacked_insert_sql(run_count), run_params)
                run_params = []
            run_count = len(chunk)
            run_params.append(_adapt_params(tuple(v for row in chunk for v in row), ctx))
            i = chunk_end
        if run_params:
            # Consecutive same-sized chunks share one statement text.
            cur.executemany(_stacked_insert_sql(run_count), run_params)

    if identity_insert:
        try:
            cur.execute(f"SET IDENTITY_INSERT {table_ref} ON")
        except DBError as e:
            _close_row_source(row_gen)
            try: cur.close()
            except Exception: pass
            print(f"Error enabling IDENTITY_INSERT on {table_ref}: {e}", file=sys.stderr)
            return

    # ── Batch insert loop ─────────────────────────────────────────────────────
    print(
        f"Importing '{opts.file}' → {table_ref} "
        f"({n_cols} columns, batch={opts.batch}) ...",
        file=sys.stderr,
    )

    t0 = time.perf_counter()
    total_inserted = 0
    total_errors   = 0
    batch          = []
    row_num        = 0
    truncated      = False
    truncate_announced = False
    warned_extra_keys = False
    file_col_set   = set(file_cols)
    transaction_depth = ctx.get("transaction_depth", 0)
    transaction_open = transaction_depth > 0

    def _reopen_cursor(context, row_num):
        """Close and recreate cur after an error; raise RuntimeError if the
        reopen itself fails (e.g. the connection dropped), instead of letting
        a bare driver error escape uncaught past this function's callers."""
        nonlocal cur
        try:
            cur.close()
        except Exception:
            pass
        try:
            cur = conn.cursor()
        except DBError as reopen_err:
            raise RuntimeError(
                f"Could not reopen cursor after {context} near row {row_num}: {reopen_err}"
            ) from reopen_err

    def _flush_batch(batch, row_num):
        """Commit one batch of (source_row_number, row_tuple). Returns (inserted, errors)."""
        nonlocal total_inserted, total_errors, truncated, cur
        try:
            if opts.truncate and not truncated:
                cur.execute(truncate_sql)
            _send_rows([r for _, r in batch])
            if not transaction_open:
                conn.commit()
            total_inserted += len(batch)
            if opts.truncate and not truncated:
                truncated = True
            print(
                f"\r  {total_inserted:,} rows inserted...",
                end="", flush=True, file=sys.stderr,
            )
            return True
        except KeyboardInterrupt:
            # Interrupted between executemany and commit. pymssql runs without
            # autocommit, so the batch may be inserted-but-uncommitted on the
            # server; roll it back rather than leaving it dangling on the
            # connection for the next statement to silently inherit.
            if not transaction_open:
                _safe_rollback(conn, ctx, "import interrupted mid-batch")
            raise
        except DBError as batch_err:
            if not transaction_open:
                if not _safe_rollback(conn, ctx, "import batch error"):
                    raise RuntimeError(
                        "Import batch error left the connection unusable"
                    ) from batch_err
            else:
                # A failed statement can leave an explicit SQL Server
                # transaction uncommittable.  Retrying rows in that state is
                # unsafe, so refresh the state and require an operator to
                # decide whether to roll back the surrounding transaction.
                xact_state = _resync_transaction_state(conn, ctx)
                if opts.on_error == "skip":
                    state_text = (
                        f" (XACT_STATE={xact_state})"
                        if xact_state is not None else ""
                    )
                    raise RuntimeError(
                        "Batch error in an explicit transaction; --on-error skip "
                        f"cannot safely retry rows{state_text}: {batch_err}"
                    ) from batch_err
            if opts.on_error == "abort":
                raise RuntimeError(
                    f"Batch error ending near row {row_num}: {batch_err}"
                ) from batch_err
            # on_error == "skip": recreate cursor (post-error state is undefined)
            # then retry in halves to isolate bad rows: a group that inserts
            # cleanly is committed in one request, a failing group is rolled
            # back and split, and only single rows are retried on their own.
            _reopen_cursor("batch error", row_num)
            print(
                f"\n  Batch error near row {row_num} — retrying in smaller groups: {batch_err}",
                file=sys.stderr,
            )
            if opts.truncate and not truncated:
                try:
                    cur.execute(truncate_sql)
                    if not transaction_open:
                        conn.commit()
                    truncated = True
                except DBError as truncate_err:
                    if not transaction_open:
                        if not _safe_rollback(conn, ctx, "truncate error"):
                            raise RuntimeError(
                                "Truncate error left the connection unusable"
                            ) from truncate_err
                    raise RuntimeError(f"Truncate failed: {truncate_err}") from truncate_err

            def _retry(group):
                nonlocal total_inserted, total_errors
                if len(group) > 1:
                    try:
                        _send_rows([r for _, r in group])
                        if not transaction_open:
                            conn.commit()
                        total_inserted += len(group)
                        return
                    except DBError as group_err:
                        if not transaction_open:
                            if not _safe_rollback(conn, ctx, "import retry error"):
                                raise RuntimeError(
                                    "Import retry error left the connection unusable"
                                ) from group_err
                        _reopen_cursor("retry error", group[0][0])
                    _split_retry(group)
                    return
                abs_row, r = group[0]
                try:
                    cur.execute(insert_sql, _adapt_params(r, ctx))
                    if not transaction_open:
                        conn.commit()
                    total_inserted += 1
                except DBError as row_err:
                    if not transaction_open:
                        if not _safe_rollback(conn, ctx, "import row error"):
                            raise RuntimeError(
                                "Import row error left the connection unusable"
                            ) from row_err
                    _reopen_cursor("row error", abs_row)
                    total_errors += 1
                    print(f"  Skipped row {abs_row}: {row_err}", file=sys.stderr)

            def _split_retry(group):
                mid = len(group) // 2
                _retry(group[:mid])
                _retry(group[mid:])

            try:
                # The whole batch already failed, so start from its halves.
                if len(batch) > 1:
                    _split_retry(batch)
                else:
                    _retry(batch)
            except KeyboardInterrupt:
                # Interrupted mid-retry: the current row's execute() may have
                # left it inserted-but-uncommitted on the connection (same
                # autocommit-off hazard as the whole-batch attempt above), so
                # roll it back before the interrupt propagates.
                if not transaction_open:
                    _safe_rollback(conn, ctx, "import interrupted mid-row-retry")
                raise
            return False

    def _explicit_truncate():
        nonlocal truncated, truncate_announced
        if opts.truncate and not truncated:
            try:
                cur.execute(truncate_sql)
                if not transaction_open:
                    conn.commit()
                truncated = True
                if not truncate_announced:
                    print(f"Truncated {table_ref}", file=sys.stderr)
                    truncate_announced = True
            except DBError as e:
                print(f"Truncate failed: {e}", file=sys.stderr)
                if transaction_open:
                    _resync_transaction_state(conn, ctx)
                else:
                    _safe_rollback(conn, ctx, "truncate error")
                raise RuntimeError(f"Truncate failed: {e}") from e

    try:
        for row_data in row_gen:
            row_num += 1

            if fmt in ("csv", "json", "jsonl"):
                if fmt == "csv" and opts.no_header:
                    if generated_positions and len(row_data) == len(file_cols):
                        row_data = tuple(
                            v for i, v in enumerate(row_data) if i not in generated_positions
                        )
                else:
                    if not isinstance(row_data, dict):
                        msg = f"Row {row_num}: expected a {fmt.upper()} object/record"
                        if opts.on_error == "abort":
                            print(f"\nError: {msg}", file=sys.stderr)
                            if not transaction_open:
                                _safe_rollback(conn, ctx, "input validation error")
                            return
                        print(f"\n  Skipped — {msg}", file=sys.stderr)
                        total_errors += 1
                        continue
                    if fmt == "csv":
                        raw_row = row_data["__row__"]
                        row_len = row_data["__row_len__"]
                        row_map = row_data["__data__"]
                        if row_len != len(file_cols):
                            msg = (
                                f"Row {row_num}: expected {len(file_cols)} columns "
                                f"from CSV header, got {row_len}"
                            )
                            if opts.on_error == "abort":
                                print(f"\nError: {msg}", file=sys.stderr)
                                if not transaction_open:
                                    _safe_rollback(conn, ctx, "input validation error")
                                return
                            print(f"\n  Skipped — {msg}", file=sys.stderr)
                            total_errors += 1
                            continue
                        source_cols = csv_input_cols
                        row_data = tuple(row_map.get(c) for c in source_cols)
                    else:
                        missing_keys = [c for c in insert_cols if c not in row_data]
                        if missing_keys:
                            msg = (
                                f"Row {row_num}: missing key(s) in {fmt.upper()} record: "
                                f"{missing_keys}"
                            )
                            if opts.on_error == "abort":
                                print(f"\nError: {msg}", file=sys.stderr)
                                if not transaction_open:
                                    _safe_rollback(conn, ctx, "input validation error")
                                return
                            print(f"\n  Skipped — {msg}", file=sys.stderr)
                            total_errors += 1
                            continue
                        if not opts.columns and not warned_extra_keys:
                            # Columns come from the first record, so a key it
                            # lacked is dropped from every later record too.
                            extra_keys = [k for k in row_data if k not in file_col_set]
                            if extra_keys:
                                warned_extra_keys = True
                                print(
                                    f"\nWarning: Row {row_num} has key(s) {extra_keys} that the "
                                    "first record lacks; they are not imported. Use --columns "
                                    "to choose the columns.",
                                    file=sys.stderr,
                                )
                        source_cols = insert_cols
                        row_data = tuple(row_data.get(c) for c in source_cols)

            # Column count mismatch check
            if len(row_data) < n_cols:
                msg = (
                    f"Row {row_num}: expected {n_cols} columns, "
                    f"got {len(row_data)}"
                )
                if opts.on_error == "abort":
                    print(f"\nError: {msg}", file=sys.stderr)
                    if not transaction_open:
                        _safe_rollback(conn, ctx, "input validation error")
                    return
                print(f"\n  Skipped — {msg}", file=sys.stderr)
                total_errors += 1
                continue

            if len(row_data) > n_cols:
                msg = (
                    f"Row {row_num}: expected {n_cols} columns, "
                    f"got {len(row_data)}"
                )
                if opts.on_error == "abort":
                    print(f"\nError: {msg}", file=sys.stderr)
                    if not transaction_open:
                        _safe_rollback(conn, ctx, "input validation error")
                    return
                print(f"\n  Skipped — {msg}", file=sys.stderr)
                total_errors += 1
                continue

            try:
                decoded = []
                for column, value in zip(canonical_cols, row_data):
                    if fmt == "csv" and value == "" and column.lower() not in text_cols:
                        value = None
                    if (ctx.get("driver_name") == "pymssql" and isinstance(value, str)
                            and "\x00" in value):
                        # pymssql inlines parameters into the SQL text, which
                        # it cuts off at a NUL.
                        raise ValueError(
                            f"Column {column}: text contains NUL (U+0000), "
                            "which pymssql cannot send")
                    if (column.lower() in binary_cols and isinstance(value, str)
                            and value.startswith("0x")):
                        if not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value):
                            raise ValueError(f"Invalid binary hex for column {column}")
                        value = bytes.fromhex(value[2:])
                        if ctx.get("driver_name") == "pymssql":
                            # pymssql quotes bytes as a varchar literal, which
                            # SQL Server won't implicitly convert to binary;
                            # bytearray is sent as a 0x... binary literal.
                            value = bytearray(value)
                    decoded.append(value)
                row_data = tuple(decoded)
            except ValueError as error:
                if opts.on_error == "abort":
                    if not transaction_open:
                        _safe_rollback(conn, ctx, "binary input validation error")
                    print(f"\nError: Row {row_num}: {error}", file=sys.stderr)
                    return
                total_errors += 1
                print(f"\n  Skipped row {row_num}: {error}", file=sys.stderr)
                continue

            # Keep the source row number with each buffered row so error
            # messages stay accurate when validation-skipped rows leave gaps.
            batch.append((row_num, tuple(row_data)))

            if len(batch) >= opts.batch:
                committed = _flush_batch(batch, row_num)
                if committed and opts.truncate and truncated and not truncate_announced:
                    print(f"\nTruncated {table_ref}", file=sys.stderr)
                    truncate_announced = True
                batch = []

        # Final partial batch
        if batch:
            committed = _flush_batch(batch, row_num)
            if committed and opts.truncate and truncated and not truncate_announced:
                print(f"\nTruncated {table_ref}", file=sys.stderr)
                truncate_announced = True
        elif opts.truncate and not truncated:
            # No batch ever reached _flush_batch — either the source had no
            # rows, or --on-error skip discarded every row that was read.
            # --truncate is documented as unconditional ("TRUNCATE TABLE
            # before importing"), so honor it even when nothing ended up
            # insertable, rather than only when the source was empty.
            _explicit_truncate()

    except RuntimeError as e:
        print(f"\nAborted: {e}", file=sys.stderr)
        return
    except json.JSONDecodeError as e:
        if not transaction_open:
            _safe_rollback(conn, ctx, "JSON input error")
        print(f"\nError reading '{opts.file}': invalid JSON: {e}", file=sys.stderr)
        return
    except (csv.Error, OSError, ValueError) as e:
        if not transaction_open:
            _safe_rollback(conn, ctx, "input I/O error")
        print(f"\nError reading '{opts.file}': {e}", file=sys.stderr)
        return
    finally:
        _close_row_source(row_gen)
        try: cur.close()
        except Exception: pass
        if identity_insert:
            try:
                off_cur = conn.cursor()
                try:
                    off_cur.execute(f"SET IDENTITY_INSERT {table_ref} OFF")
                finally:
                    off_cur.close()
            except DBError:
                pass  # connection lost; the setting dies with the session

    elapsed = time.perf_counter() - t0
    rate    = total_inserted / elapsed if elapsed > 0 else 0
    print(
        f"\nDone. {total_inserted:,} rows imported in {elapsed:.2f}s "
        f"({rate:,.0f} rows/sec).",
        file=sys.stderr,
    )
    if total_errors:
        print(f"  {total_errors:,} row(s) skipped due to errors.", file=sys.stderr)


# ── Dot Command Handlers ───────────────────────────────────────────────────────

DOT_HELP = """\
Dot commands:
  .tables [PATTERN]          List tables (optionally filter by LIKE pattern)
  .schema [TABLE]            Show CREATE-equivalent column info for a table
  .databases                 List databases on the server
  .columns TABLE             Show column names/types for TABLE
  .views [PATTERN]           List views
  .procs [PATTERN]           List stored procedures
  .export TABLE FILE [OPTS]  Dump a table (or query) to CSV, JSON, or JSONL
      --format csv|json|jsonl  Override auto-detection from file extension
      --query SQL              Use a custom SELECT instead of SELECT * FROM TABLE
      --where CONDITION        Add a WHERE clause (simple table export only)
      --columns col1,col2,...  Export specific columns (simple table export only)
      --order-by EXPR          Add an ORDER BY clause (simple table export only)
      --delimiter CHAR         CSV field delimiter (default: ,)
      --no-header              Omit header row (CSV only)
      --null-as STR            Represent NULL as STR (CSV default: empty; JSON default: null)
      --chunk N                Rows fetched per round-trip (default: 500)
      --append                 Append to existing file instead of overwriting
      -                        Use - as FILE to write to stdout
  .import FILE TABLE [OPTS]  Import CSV or JSON into a table (bulk insert)
      --format csv|json|jsonl  Override auto-detection from file extension
      --batch N                Rows per commit batch (default: 1000)
      --skip N                 Skip first N data rows
      --truncate               TRUNCATE TABLE before importing (DELETE FROM on MySQL)
      --no-header              CSV has no header row (use table column order)
      --delimiter CHAR         CSV field delimiter (default: ,)
      --columns col1,col2,...  Explicit column list (overrides file header)
      --on-error skip|abort    Row error handling (default: abort)
      --keep-identity          Insert the file's IDENTITY column values (SQL Server)
  .mode MODE [TABLE]         Set output mode: table, csv, json, vertical, line, insert
                               insert mode requires TABLE name: .mode insert dbo.users
  .headers on|off            Toggle column headers
  .output FILE [OPTS]        Redirect output to file or stdout
      -                        Reset to stdout
      --insert TABLE           Also switch to insert mode targeting TABLE
      --append                 Append to file instead of overwriting
  .read FILE                 Execute SQL from a file
  .reconnect                 Reconnect using the original connection parameters
                               (use after a dropped/lost connection)
  .timer on|off              Toggle query execution time display
  .rowcount on|off           Toggle row count display
  .quit / .exit              Exit the program
  .help                      Show this help
"""


# Dot commands that take an optional LIKE pattern and run a single query.
# The argument also matches as a plain substring, so names containing LIKE
# metacharacters ([, %, _) are found when typed literally.
_PATTERN_CMDS = {
    ".tables": (
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
        "FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE (TABLE_NAME LIKE {PARAM_PLACEHOLDER} OR CHARINDEX({PARAM_PLACEHOLDER}, TABLE_NAME) > 0) "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME"
    ),
    ".views": (
        "SELECT TABLE_SCHEMA, TABLE_NAME "
        "FROM INFORMATION_SCHEMA.VIEWS "
        f"WHERE (TABLE_NAME LIKE {PARAM_PLACEHOLDER} OR CHARINDEX({PARAM_PLACEHOLDER}, TABLE_NAME) > 0) "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME"
    ),
    ".procs": (
        "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE "
        "FROM INFORMATION_SCHEMA.ROUTINES "
        f"WHERE (ROUTINE_NAME LIKE {PARAM_PLACEHOLDER} OR CHARINDEX({PARAM_PLACEHOLDER}, ROUTINE_NAME) > 0) "
        "ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME"
    ),
}


# MySQL equivalents. INFORMATION_SCHEMA spans every database on a MySQL
# server, so scope listings to the current database — or, when no database is
# selected, to every non-system database.
_MYSQL_SYSTEM_SCHEMAS = "('mysql', 'sys', 'performance_schema', 'information_schema')"


def _mysql_schema_scope(column):
    return (
        f"({column} = DATABASE() OR "
        f"(DATABASE() IS NULL AND {column} NOT IN {_MYSQL_SYSTEM_SCHEMAS}))"
    )


_MYSQL_PATTERN_CMDS = {
    ".tables": (
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
        "FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE (TABLE_NAME LIKE {PARAM_PLACEHOLDER} OR LOCATE({PARAM_PLACEHOLDER}, TABLE_NAME) > 0) "
        f"AND {_mysql_schema_scope('TABLE_SCHEMA')} "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME"
    ),
    ".views": (
        "SELECT TABLE_SCHEMA, TABLE_NAME "
        "FROM INFORMATION_SCHEMA.VIEWS "
        f"WHERE (TABLE_NAME LIKE {PARAM_PLACEHOLDER} OR LOCATE({PARAM_PLACEHOLDER}, TABLE_NAME) > 0) "
        f"AND {_mysql_schema_scope('TABLE_SCHEMA')} "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME"
    ),
    ".procs": (
        "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE "
        "FROM INFORMATION_SCHEMA.ROUTINES "
        f"WHERE (ROUTINE_NAME LIKE {PARAM_PLACEHOLDER} OR LOCATE({PARAM_PLACEHOLDER}, ROUTINE_NAME) > 0) "
        f"AND {_mysql_schema_scope('ROUTINE_SCHEMA')} "
        "ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME"
    ),
}


def run_dot_command(cmd_line, ctx):
    """Process a dot command. Returns True if handled, False if unknown."""
    parts = cmd_line.strip().split(None, 1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    conn = ctx["conn"]
    dialect = _dialect(ctx)

    def query(sql, params=()):
        if not _connection_ready(ctx):
            return None, None
        try:
            cur = conn.cursor()
        except DBError as e:
            print(f"Query error opening cursor: {e}", file=sys.stderr)
            return None, None
        try:
            cur.execute(_adapt_placeholders(sql, ctx), _adapt_params(params, ctx))
            return cur.fetchall(), [d[0] for d in cur.description] if cur.description else []
        except DBError as e:
            print(f"Query error: {e}", file=sys.stderr)
            return None, None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            # A metadata read can open an implicit transaction, and a failed
            # one can roll back the user's transaction under XACT_ABORT ON.
            if dialect != "mysql" and (ctx.get("implicit_transactions")
                                       or ctx.get("transaction_depth", 0) > 0):
                _resync_transaction_state(conn, ctx)

    if cmd == ".help":
        print(DOT_HELP)

    elif cmd in (".quit", ".exit"):
        raise SystemExit(0)

    elif cmd == ".databases":
        if dialect == "mysql":
            rows, headers = query(
                "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME "
                "FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
            )
        else:
            rows, headers = query("SELECT name, state_desc, compatibility_level FROM sys.databases ORDER BY name")
        if rows is None:
            return True
        print_results(rows, headers, ctx)

    elif cmd in _PATTERN_CMDS:
        pattern = f"%{arg}%" if arg else "%"
        pattern_cmds = _MYSQL_PATTERN_CMDS if dialect == "mysql" else _PATTERN_CMDS
        rows, headers = query(pattern_cmds[cmd], (pattern, arg))
        if rows is None:
            return True
        print_results(rows, headers, ctx)

    elif cmd == ".export":
        if not arg:
            print(
                "Usage: .export TABLE FILE [--format csv|json|jsonl] "
                "[--query SQL] [--where COND] [--columns col1,...] "
                "[--order-by EXPR] [--delimiter CHAR] [--no-header] "
                "[--null-as STR] [--chunk N] [--append]",
                file=sys.stderr,
            )
            return True
        export_data(arg, ctx)

    elif cmd == ".import":
        if not arg:
            print(
                "Usage: .import FILE TABLE [--format csv|json|jsonl] "
                "[--batch N] [--skip N] [--truncate] [--no-header] "
                "[--delimiter CHAR] [--columns col1,...] "
                "[--on-error skip|abort] [--keep-identity]",
                file=sys.stderr,
            )
            return True
        import_data(arg, ctx)

    elif cmd in (".schema", ".columns"):
        if not arg:
            print("Usage: .schema TABLE  or  .columns TABLE", file=sys.stderr)
            return True
        name_parts = _split_sql_identifier(arg, dialect=dialect)
        max_parts = 2 if dialect == "mysql" else 3
        if len(name_parts) == 1:
            database_name, schema, table = None, None, name_parts[0]
        elif len(name_parts) == 2:
            database_name, schema, table = None, name_parts[0], name_parts[1]
        elif len(name_parts) == 3 and max_parts == 3:
            database_name, schema, table = name_parts
        else:
            print("Usage: .schema TABLE  or  .columns TABLE", file=sys.stderr)
            return True
        columns_view = (
            f"{_quote_identifier_part(database_name)}.INFORMATION_SCHEMA.COLUMNS"
            if database_name else
            "INFORMATION_SCHEMA.COLUMNS"
        )
        where = f"WHERE TABLE_NAME = {PARAM_PLACEHOLDER}"
        params = [table]
        if schema is not None:
            where += f" AND TABLE_SCHEMA = {PARAM_PLACEHOLDER}"
            params.append(schema)
        elif dialect == "mysql":
            # A MySQL schema *is* a database: a bare name means the current one.
            where += " AND TABLE_SCHEMA = DATABASE()"
        select_cols = [
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "IS_NULLABLE",
            "COLUMN_DEFAULT",
            "ORDINAL_POSITION",
        ]
        rows, headers = query(
            f"SELECT {', '.join(select_cols)} "
            f"FROM {columns_view} "
            f"{where} "
            f"ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
            tuple(params)
        )
        if rows is None:
            return True
        if not rows:
            print(f"Table '{arg}' not found.", file=sys.stderr)
        elif schema is None:
            schemas = sorted({row[0] for row in rows})
            if len(schemas) > 1:
                qualifier = "database.schema.table" if database_name else "schema.table"
                print(
                    f"Table name '{table}' is ambiguous across schemas: "
                    f"{', '.join(schemas)}. Use {qualifier}.",
                    file=sys.stderr,
                )
            else:
                print_results([row[2:] for row in rows], headers[2:], ctx)
        else:
            print_results([row[2:] for row in rows], headers[2:], ctx)

    elif cmd == ".mode":
        mode_parts = arg.split(None, 1)
        mode_name  = mode_parts[0] if mode_parts else ""
        mode_arg   = mode_parts[1].strip() if len(mode_parts) > 1 else ""

        if mode_name not in ALL_MODES:
            print(f"Unknown mode '{mode_name}'. Available: {', '.join(ALL_MODES)}", file=sys.stderr)
        elif mode_name == "insert":
            if not mode_arg:
                print("Usage: .mode insert TABLE", file=sys.stderr)
            else:
                ctx["mode"]         = "insert"
                ctx["insert_table"] = _bracket_table(mode_arg, _dialect(ctx))
                print(f"Output mode: insert  (table: {ctx['insert_table']})", file=sys.stderr)
        else:
            ctx["mode"] = mode_name
            print(f"Output mode: {mode_name}", file=sys.stderr)

    elif cmd == ".headers":
        if arg.lower() in ("on", "off"):
            ctx["headers"] = arg.lower() == "on"
            print(f"Headers: {arg.lower()}", file=sys.stderr)
        else:
            print("Usage: .headers on|off", file=sys.stderr)

    elif cmd == ".timer":
        if arg.lower() in ("on", "off"):
            ctx["timer"] = arg.lower() == "on"
            print(f"Timer: {arg.lower()}", file=sys.stderr)
        else:
            print("Usage: .timer on|off", file=sys.stderr)

    elif cmd == ".rowcount":
        if arg.lower() in ("on", "off"):
            ctx["rowcount"] = arg.lower() == "on"
            print(f"Row count: {arg.lower()}", file=sys.stderr)
        else:
            print("Usage: .rowcount on|off", file=sys.stderr)

    elif cmd == ".output":
        # Parse: .output FILE [--insert TABLE] [--append]
        try:
            out_parts = _split_dot_command_args(arg)
        except ValueError as e:
            print(f".output parse error: {e}", file=sys.stderr)
            return True

        op = argparse.ArgumentParser(prog=".output", add_help=False, exit_on_error=False)
        op.add_argument("file")
        op.add_argument("--insert", default="", metavar="TABLE",
                        help="Switch to insert mode targeting TABLE")
        op.add_argument("--append", action="store_true",
                        help="Append to file instead of overwriting")

        try:
            out_opts = op.parse_args(out_parts)
        except (argparse.ArgumentError, SystemExit):
            print("Usage: .output FILE [--insert TABLE] [--append] | .output -", file=sys.stderr)
            return True

        prev = ctx.get("output", sys.stdout)

        if out_opts.file in ("-", "stdout"):
            if prev is not sys.stdout:
                try:
                    prev.close()
                except OSError as close_err:
                    # close() is where a buffered writer's final flush
                    # surfaces failures (e.g. disk full, dropped network
                    # share); swallowing it would leave the user thinking
                    # the previous output file is complete when it may be
                    # truncated or corrupt.
                    print(
                        f"Warning: error closing previous output: {close_err}",
                        file=sys.stderr,
                    )
            ctx["output"] = sys.stdout
            print("Output: stdout", file=sys.stderr)
        else:
            file_mode = "a" if out_opts.append else "w"
            try:
                prev_name = getattr(prev, "name", None)
                if prev is not sys.stdout and isinstance(prev_name, (str, bytes, os.PathLike)):
                    try:
                        same_target = os.path.samefile(prev_name, out_opts.file)
                    except FileNotFoundError:
                        same_target = False
                    if same_target:
                        prev.flush()
                new_out = open(out_opts.file, file_mode,
                               newline="", encoding="utf-8")
                action = "Appending to" if out_opts.append else "Output:"
                if prev is not sys.stdout:
                    try:
                        prev.close()
                    except OSError as close_err:
                        print(
                            f"Warning: error closing previous output: {close_err}",
                            file=sys.stderr,
                        )
                ctx["output"] = new_out
                print(f"{action} {out_opts.file}", file=sys.stderr)
            except OSError as e:
                print(f"Error opening '{out_opts.file}': {e}", file=sys.stderr)
                return True

        if out_opts.insert:
            ctx["mode"]         = "insert"
            ctx["insert_table"] = _bracket_table(out_opts.insert, _dialect(ctx))
            print(f"  Insert mode: {ctx['insert_table']}", file=sys.stderr)

    elif cmd == ".reconnect":
        conn_args = ctx.get("conn_args")
        if conn_args is None:
            print(
                "Error: .reconnect is not available (connection parameters "
                "were not recorded for this session).",
                file=sys.stderr,
            )
            return True
        try:
            new_conn, new_conn_info, new_driver = _connect(conn_args)
        except (ValueError, *DBError) as e:
            print(f"Reconnect failed: {e}", file=sys.stderr)
            return True
        old_conn = ctx.get("conn")
        if old_conn is not None:
            try:
                old_conn.close()
            except Exception:
                pass
        ctx["conn"]              = new_conn
        ctx["conn_info"]         = new_conn_info
        ctx["driver_name"]       = new_driver["name"]
        ctx["driver_version"]    = new_driver["version"]
        ctx["paramstyle"]        = new_driver["paramstyle"]
        ctx["dialect"]           = new_driver.get("dialect", "mssql")
        ctx["transaction_depth"] = 0
        ctx["transaction_open"]  = False
        ctx["savepoints"]        = set()
        ctx["connection_usable"] = True
        ctx["implicit_transactions"] = False
        ctx.pop("mysql_interrupted_connection", None)
        print(f"Reconnected to: {new_conn_info}", file=sys.stderr)

    elif cmd == ".read":
        if not arg:
            print("Usage: .read FILE", file=sys.stderr)
            return True
        try:
            read_parts = _split_dot_command_args(arg)
        except ValueError as e:
            print(f".read parse error: {e}", file=sys.stderr)
            return True
        if len(read_parts) != 1:
            print("Usage: .read FILE", file=sys.stderr)
            return True
        execute_file(read_parts[0], ctx)

    else:
        return False  # unknown command

    return True


# ── Result Printing ────────────────────────────────────────────────────────────

# Modes whose output is row-independent, so result sets can be written in
# chunks instead of materializing every row in memory first. table/vertical
# need all rows for column widths; json needs them for array bracketing.
STREAMING_MODES = ("csv", "line", "insert")

RESULT_CHUNK_ROWS = 500


def print_results_stream(cursor, headers, ctx):
    """Stream one result set to output in chunks; returns the row count.

    Only valid for STREAMING_MODES. Output for each chunk matches what
    print_results would produce for the same rows, with the CSV header
    emitted once and the row count printed from the running total.
    """
    mode         = ctx.get("mode", "table")
    show_headers = ctx.get("headers", True)
    out          = ctx.get("output", sys.stdout)
    table_ref    = ctx.get("insert_table", "<table>")

    total = 0
    first = True
    while True:
        chunk = cursor.fetchmany(RESULT_CHUNK_ROWS)
        if not chunk:
            break
        if mode == "insert":
            print(format_insert(chunk, headers, table_ref, _dialect(ctx),
                                _insert_datetime_timespec(ctx)), file=out)
        elif mode == "csv":
            print(format_csv(chunk, headers, show_headers=show_headers and first), file=out)
        else:  # line
            print(format_line(chunk, headers), file=out)
        first = False
        total += len(chunk)

    if total == 0:
        # Match the non-streamed empty-result rendering exactly.
        if mode == "insert":
            print(format_insert([], headers, table_ref, _dialect(ctx)), file=out)
        elif mode == "csv":
            print(format_csv([], headers, show_headers=show_headers), file=out)
        else:
            print(format_line([], headers), file=out)

    if ctx.get("rowcount", True):
        print(f"({total} row{'s' if total != 1 else ''})", file=sys.stderr)
    return total


def _insert_datetime_timespec(ctx):
    return "microseconds" if ctx.get("insert_microseconds") else "milliseconds"


def print_results(rows, headers, ctx):
    mode         = ctx.get("mode", "table")
    show_headers = ctx.get("headers", True)
    out          = ctx.get("output", sys.stdout)

    if mode == "insert":
        table_ref = ctx.get("insert_table", "<table>")
        print(format_insert(rows, headers, table_ref, _dialect(ctx),
                            _insert_datetime_timespec(ctx)), file=out)
        if ctx.get("rowcount", True) and rows is not None:
            n = len(rows)
            print(f"({n} row{'s' if n != 1 else ''})", file=sys.stderr)
        return

    # table and csv support header suppression; other modes embed headers in their structure
    if mode in ("table", "csv"):
        formatter = FORMATTERS[mode]
        print(formatter(rows, headers, show_headers=show_headers), file=out)
    else:
        formatter = FORMATTERS.get(mode, format_table)
        print(formatter(rows, headers), file=out)

    if ctx.get("rowcount", True) and rows is not None:
        n = len(rows)
        print(f"({n} row{'s' if n != 1 else ''})", file=sys.stderr)


# ── SQL Execution ──────────────────────────────────────────────────────────────

# MySQL protocol constants (stable parts of the wire protocol, defined here
# rather than read from pymysql.constants so the fake drivers used in tests
# don't need to mirror pymysql's package layout).
MYSQL_CLIENT_MULTI_STATEMENTS = 1 << 16
MYSQL_SERVER_STATUS_IN_TRANS = 1
MYSQL_CR_SERVER_LOST = 2013  # "Lost connection to MySQL server during query"


def _mysql_kill_running_query(ctx):
    """Cancel the statement a lost MySQL connection was running.

    --query-timeout is pymysql's socket read timeout: when it fires, pymysql
    just closes the socket. The server doesn't notice until it next talks to
    the client, so the statement keeps running and, under autocommit, commits.
    KILL QUERY from a second connection stops it (and rolls it back) the way
    SQL Server's timeout does. Returns True if KILL QUERY was sent.
    """
    conn_args = ctx.get("conn_args")
    try:
        # The thread id comes from the handshake and survives the close.
        thread_id = ctx["conn"].thread_id()
    except Exception:
        return False
    if conn_args is None or not isinstance(thread_id, int):
        return False
    try:
        side_conn, _, _ = _connect_mysql(conn_args)
    except (ValueError, *DBError):
        return False
    try:
        side_cur = side_conn.cursor()
        try:
            side_cur.execute("KILL QUERY %s", (thread_id,))
        finally:
            side_cur.close()
        return True
    except DBError:
        return False  # e.g. the statement already finished ("Unknown thread id")
    finally:
        try:
            side_conn.close()
        except Exception:
            pass


def _cancel_mysql_interrupt(ctx):
    """Cancel a closed MySQL session once across SQL and REPL handlers."""
    conn = ctx.get("conn")
    if _dialect(ctx) != "mysql" or conn is None or conn.open:
        return False
    if ctx.get("mysql_interrupted_connection") is conn:
        return True
    ctx["mysql_interrupted_connection"] = conn
    ctx["connection_usable"] = False
    if _mysql_kill_running_query(ctx):
        print("Sent KILL QUERY to stop the interrupted statement.", file=sys.stderr)
    else:
        print("Warning: interrupted query could not be cancelled; "
              "it may still be running and may commit.", file=sys.stderr)
    print("MySQL connection closed. Run .reconnect.", file=sys.stderr)
    return True


def _install_mysql_status_tracking(conn):
    """Copy SELECT's EOF status without issuing SQL that resets diagnostics."""
    read_packet = getattr(conn, "_read_packet", None)
    if not callable(read_packet):
        return  # Minimal connection adapters may expose only server_status.

    def read_with_status(*args, **kwargs):
        packet = read_packet(*args, **kwargs)
        if packet.is_eof_packet():
            conn.server_status = struct.unpack_from("<H", packet.get_all_data(), 3)[0]
        return packet

    conn._read_packet = read_with_status


def _apply_mysql_transaction_status(conn, ctx):
    """Set transaction tracking from the MySQL server's own IN_TRANS status
    flag. _install_mysql_status_tracking captures the EOF status that
    PyMySQL omits, and error recovery explicitly obtains a fresh OK packet.
    Returns True if the flag was available.

    MySQL connections start with autocommit on. The flag covers explicit
    transactions and implicit ones if the user disables autocommit.
    Reading it is exact, so MySQL needs none of the lexical
    BEGIN/COMMIT/SAVE TRAN tracking T-SQL relies on — MySQL's transaction
    syntax differs (START TRANSACTION, SAVEPOINT, RELEASE SAVEPOINT) and DDL
    commits implicitly, so lexing would get it wrong anyway. MySQL
    transactions don't nest, so depth is only ever 0 or 1.
    """
    status = getattr(conn, "server_status", None)
    if not isinstance(status, int):
        return False
    in_trans = bool(status & MYSQL_SERVER_STATUS_IN_TRANS)
    ctx["transaction_depth"] = 1 if in_trans else 0
    ctx["transaction_open"] = in_trans
    if not in_trans:
        ctx["savepoints"] = set()
    return True


def _refresh_mysql_transaction_status(conn, ctx):
    """Get fresh status without starting or ending the user's transaction."""
    cur = conn.cursor()
    try:
        cur.execute("DO 0")
        return _apply_mysql_transaction_status(conn, ctx)
    finally:
        try:
            cur.close()
        except DBError:
            pass


def _resync_transaction_state(conn, ctx):
    """Refresh transaction tracking from the server; return XACT_STATE or None.

    Used after transaction controls and errors: lexical tracking cannot see
    skipped branches or how far a failed batch executed. MySQL has no
    XACT_STATE, so this always returns None there.
    """
    ctx["transaction_state_verified"] = False
    if _dialect(ctx) == "mysql":
        try:
            ctx["transaction_state_verified"] = _refresh_mysql_transaction_status(conn, ctx)
        except DBError:
            pass  # can't resync; leave depth as-is
        return None
    try:
        sync_cur = conn.cursor()
    except DBError:
        return  # connection itself may be dead; leave depth as-is
    try:
        sync_cur.execute("SELECT XACT_STATE(), @@TRANCOUNT")
        rows = sync_cur.fetchall()
        if rows:
            if len(rows[0]) == 1:
                # Keep the helper tolerant of minimal cursor adapters that
                # expose only @@TRANCOUNT.
                xact_state, trancount = None, rows[0][0]
            else:
                xact_state, trancount = rows[0]
            ctx["transaction_depth"] = trancount
            ctx["transaction_open"] = trancount > 0
            ctx["transaction_state_verified"] = True
            if trancount == 0:
                ctx["savepoints"] = set()
            return xact_state
    except DBError:
        pass  # can't resync; leave depth as-is
    finally:
        try:
            sync_cur.close()
        except Exception:
            pass
    return None


def _resync_transaction_depth(conn, ctx):
    """Backward-compatible wrapper for callers that need only @@TRANCOUNT."""
    _resync_transaction_state(conn, ctx)


# Statements SQL Server refuses inside a user transaction, matched as
# consecutive words anywhere in a batch (so "IF ... CREATE DATABASE" counts).
_NO_TRANSACTION_WORDS = {"BACKUP", "RESTORE", "RECONFIGURE"}
_NO_TRANSACTION_PAIRS = {
    (verb, noun)
    for verb in ("CREATE", "ALTER", "DROP")
    for noun in ("DATABASE", "FULLTEXT", "ENDPOINT", "AVAILABILITY")
} | {("ALTER", "SERVER"), ("DBCC", "SHRINKDATABASE"), ("DBCC", "SHRINKFILE")}
# Batches that inspect or change the session's own transaction state must not
# run inside the tool's driver transaction, which would show up in them.
_SESSION_TRANSACTION_WORDS = {"TRANCOUNT", "XACT_STATE", "IMPLICIT_TRANSACTIONS", "ANSI_DEFAULTS"}
_IMPLICIT_TRANSACTION_WORDS = {"IMPLICIT_TRANSACTIONS", "ANSI_DEFAULTS"}


def _sql_words(sql):
    return [tok for tok in _sql_parser_tokens(sql) if tok not in (";", "(", ")")]


def _requires_autocommit(sql):
    """True when a SQL Server batch cannot run inside a client-owned transaction."""
    words = _sql_words(sql)
    if any(w in _NO_TRANSACTION_WORDS or w in _SESSION_TRANSACTION_WORDS for w in words):
        return True
    return any(pair in _NO_TRANSACTION_PAIRS for pair in zip(words, words[1:]))


def _refresh_implicit_transactions(sql, ctx):
    """Track SET IMPLICIT_TRANSACTIONS / ANSI_DEFAULTS from the server's @@OPTIONS."""
    if not ctx.get("implicit_transactions") and not any(
            w in _IMPLICIT_TRANSACTION_WORDS for w in _sql_words(sql)):
        return
    if not _connection_ready(ctx):
        return
    try:
        cur = ctx["conn"].cursor()
        try:
            cur.execute("SELECT @@OPTIONS & 2")
            rows = cur.fetchall()
        finally:
            cur.close()
    except DBError:
        return
    if rows:
        ctx["implicit_transactions"] = bool(rows[0][0])
    # Even if the batch ended with the option OFF, a transaction it opened
    # while the option was ON is still open.
    _resync_transaction_state(ctx["conn"], ctx)


def execute_sql(sql, ctx):
    """Execute one SQL statement and display results."""
    if not sql.strip():
        return True
    if not _connection_ready(ctx):
        return False
    if ctx.get("driver_name") not in ("pymssql", "pyodbc"):
        return _execute_sql(sql, ctx)
    if ctx.get("driver_name") == "pymssql" and "\x00" in sql:
        # pymssql sends SQL text as a C string, so everything after a NUL is
        # silently dropped -- "DELETE FROM t<NUL> WHERE id = 1" would run as
        # "DELETE FROM t". Refuse rather than run a truncated statement.
        print(
            "Error: SQL text contains a NUL character (U+0000), which pymssql "
            "cannot send intact; use NCHAR(0) or CHAR(0) instead.",
            file=sys.stderr,
        )
        return False
    if ctx.get("implicit_transactions"):
        # Dot-command metadata queries may have opened an implicit transaction.
        _resync_transaction_state(ctx["conn"], ctx)
    try:
        if (ctx.get("transaction_depth", 0) <= 0
                and not ctx.get("implicit_transactions")
                and not _transaction_control_flags(sql, ctx.get("savepoints", set()))["has_control"]
                and not _requires_autocommit(sql)):
            return _mssql_owned_work(ctx, lambda: _execute_sql(sql, ctx))
        return _execute_sql(sql, ctx)
    finally:
        # Also after an output error: the batch's SET already ran server-side.
        _refresh_implicit_transactions(sql, ctx)


def _execute_sql(sql, ctx):
    sql = sql.strip()
    if not sql:
        return True

    conn = ctx["conn"]
    out = ctx.get("output", sys.stdout)
    cursor = None
    mysql = _dialect(ctx) == "mysql"
    saw_non_resultset = False

    try:
        t0 = time.perf_counter()
        cursor = conn.cursor()
        cursor.execute(sql)
        txn_flags = (
            {} if mysql  # tracked from server status instead; see below
            else _transaction_control_flags(sql, ctx.get("savepoints", set()))
        )
        txn_ops = txn_flags.get("ops", [])
        has_explicit_txn_control = txn_flags.get("has_control", False)
        transaction_depth = ctx.get("transaction_depth", 0)
        transaction_open = transaction_depth > 0

        # Fetch and print all result sets. A write failure here (most often
        # a broken pipe, e.g. `| head`) must not skip the commit below: the
        # statements in this batch already ran server-side, and closing the
        # connection without committing would silently roll them back even
        # though nothing reported an error for the DML itself. So catch it,
        # let the commit/bookkeeping run as normal, then re-raise afterward.
        output_error = None
        try:
            while True:
                if ctx.get("driver_name") == "pyodbc":
                    # pyodbc resets cursor.messages per result set; a trailing
                    # PRINT arrives as its own (column-less) result set.
                    _print_odbc_messages(cursor)
                if cursor.description:
                    headers = [d[0] for d in cursor.description]
                    if ctx.get("mode", "table") in STREAMING_MODES:
                        # Row-independent modes stream in chunks so large result
                        # sets are never fully materialized in memory.
                        print_results_stream(cursor, headers, ctx)
                    else:
                        rows = cursor.fetchall()
                        print_results(rows, headers, ctx)
                else:
                    # DML/DDL
                    saw_non_resultset = True
                    affected = cursor.rowcount
                    if affected >= 0 and ctx.get("rowcount", True):
                        print(f"({affected} row{'s' if affected != 1 else ''} affected)", file=sys.stderr)

                if not cursor.nextset():
                    break
        except (OSError, ResultFormatError) as e:
            output_error = e

        if mysql:
            # Server-side autocommit already committed anything outside an
            # explicit transaction; just pick up the resulting state.
            if not _apply_mysql_transaction_status(conn, ctx):
                _resync_transaction_state(conn, ctx)
        else:
            if (
                (saw_non_resultset or _starts_with_data_change_statement(sql))
                and not has_explicit_txn_control
                and not transaction_open
                and not ctx.get("implicit_transactions")
            ):
                conn.commit()

            savepoints = set(ctx.get("savepoints", set()))
            for op, name in txn_ops:
                if op == "begin":
                    transaction_depth += 1
                elif op == "commit" and transaction_depth > 0:
                    transaction_depth = max(0, transaction_depth - 1)
                    if transaction_depth == 0:
                        savepoints.clear()
                elif op == "rollback":
                    transaction_depth = 0
                    savepoints.clear()
                elif op == "save" and name and transaction_depth > 0:
                    savepoints.add(name)
                elif op == "rollback_to_savepoint":
                    pass
            ctx["transaction_depth"] = transaction_depth
            ctx["transaction_open"] = transaction_depth > 0
            if transaction_depth == 0:
                savepoints.clear()
            ctx["savepoints"] = savepoints
            # Conditional controls and named rollbacks need the executed
            # server state, rather than the number of tokens in the batch.
            # Ordinary work within a transaction avoids this extra SELECT
            # so the next command can still inspect its @@ROWCOUNT.
            if has_explicit_txn_control:
                _resync_transaction_state(conn, ctx)
                if not ctx.get("transaction_state_verified"):
                    ctx["connection_usable"] = False
                    print("Cannot verify transaction state. Run .reconnect.", file=sys.stderr)
                    return False

        if output_error is not None:
            raise output_error

        if ctx.get("timer", False):
            elapsed = time.perf_counter() - t0
            print(f"Elapsed: {elapsed*1000:.2f} ms", file=sys.stderr)
        return True

    except KeyboardInterrupt:
        _cancel_mysql_interrupt(ctx)
        raise
    except DBError as e:
        if mysql and e.args and e.args[0] == MYSQL_CR_SERVER_LOST:
            if _mysql_kill_running_query(ctx):
                print(
                    "Connection lost mid-statement (e.g. --query-timeout); sent "
                    "KILL QUERY to stop it on the server.",
                    file=sys.stderr,
                )
            else:
                print(
                    "Warning: connection lost mid-statement (e.g. --query-timeout) "
                    "and it could not be cancelled; it may still be running on "
                    "the server and may commit.",
                    file=sys.stderr,
                )
        if mysql and saw_non_resultset and ctx.get("transaction_depth", 0) <= 0:
            # A multi-statement batch runs under server-side autocommit, so a
            # data-changing statement that finished before the failing one is
            # already committed; the rollback below can't undo it.
            print(
                "Warning: statements in this batch that completed before the "
                "error were already committed (MySQL autocommit) unless they "
                "ran inside START TRANSACTION.",
                file=sys.stderr,
            )
        if ctx.get("transaction_depth", 0) <= 0:
            _safe_rollback(conn, ctx, "SQL execution error")
        else:
            # Inside an explicit transaction: clear savepoints since they may be
            # invalid after a server error (e.g. XACT_ABORT ON, deadlock victim).
            ctx["savepoints"] = set()
            # A statement earlier in this same batch may have opened a nested
            # transaction (BEGIN TRAN) before a later statement failed the whole
            # batch. Our lexical BEGIN/COMMIT counting only advances on the
            # success path, so it can understate the server's real @@TRANCOUNT
            # here. Resync against the server instead of guessing.
            _resync_transaction_state(conn, ctx)
            if not ctx.get("transaction_state_verified"):
                # Usually a dropped connection (with pymssql, --query-timeout
                # kills it); the server rolls back the open transaction then,
                # so a later COMMIT must not look like it could still succeed.
                ctx["connection_usable"] = False
                print(f"Error: {e}", file=sys.stderr)
                print(
                    "Cannot verify transaction state: the connection may be lost, "
                    "and the server rolls back an open transaction when it drops. "
                    "Run .reconnect.",
                    file=sys.stderr,
                )
                return False
        print(f"Error: {e}", file=sys.stderr)
        return False
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass


def _scan_sql_line_state(line, block_comment_depth=0, in_string=False, in_double_quote=False, last_code_char="", dialect="mssql", in_identifier=False):
    """Update simple T-SQL (or MySQL) lexical state for one line and emit parser tokens.

    Also tracks last_code_char: the last non-whitespace character seen outside
    any quoting or comment context.  Used by _ends_with_statement_terminator and
    interactive_loop to detect a trailing semicolon without a separate scan pass.

    With dialect="mysql": backslash escapes the next character inside '...'
    and "..." strings, `backticks` (not [brackets]) quote identifiers, "#"
    starts a line comment, "--" only starts a comment when followed by
    whitespace or end of line, and block comments don't nest.
    """
    mysql = dialect == "mysql"
    ident_open, ident_close = IDENTIFIER_QUOTES[dialect]
    i = 0
    tokens = []
    while i < len(line):
        ch = line[i]
        nxt = line[i + 1] if i + 1 < len(line) else ""

        if block_comment_depth > 0:
            if ch == "/" and nxt == "*" and not mysql:
                block_comment_depth += 1
                i += 2
            elif ch == "*" and nxt == "/":
                block_comment_depth -= 1
                i += 2
            else:
                i += 1
            continue

        if in_string:
            if mysql and ch == "\\":
                i += 2
            elif ch == "'":
                if nxt == "'":
                    i += 2
                else:
                    in_string = False
                    i += 1
            else:
                i += 1
            continue

        if in_double_quote:
            if mysql and ch == "\\":
                i += 2
            elif ch == '"':
                if nxt == '"':
                    i += 2
                else:
                    in_double_quote = False
                    i += 1
            else:
                i += 1
            continue

        if in_identifier:
            if ch == ident_close:
                if nxt == ident_close:
                    i += 2
                else:
                    in_identifier = False
                    i += 1
            else:
                i += 1
            continue

        if ch == "-" and nxt == "-":
            if not mysql or i + 2 >= len(line) or line[i + 2].isspace():
                break
        if mysql and ch == "#":
            break
        if ch == "/" and nxt == "*":
            block_comment_depth += 1
            i += 2
            continue
        if ch == "'":
            in_string = True
            i += 1
            continue
        if ch == '"':
            in_double_quote = True
            i += 1
            continue
        if ch == ident_open:
            in_identifier = True
            i += 1
            continue
        if ch == ";":
            tokens.append(";")  # statement separator (consumers may ignore it)
            last_code_char = ";"
            i += 1
            continue
        if ch in "()":
            tokens.append(ch)  # condition/grouping marker (consumers may ignore it)
            last_code_char = ch
            i += 1
            continue
        if ch == "=":
            tokens.append(ch)  # assignment/alias marker (consumers may ignore it)
            last_code_char = ch
            i += 1
            continue
        if ch.isalpha() or ch == "_":
            start = i
            i += 1
            while i < len(line) and (line[i].isalnum() or line[i] == "_"):
                i += 1
            tokens.append(line[start:i].upper())
            last_code_char = line[i - 1]
            continue
        if not ch.isspace():
            last_code_char = ch
        i += 1

    return block_comment_depth, in_string, in_double_quote, tokens, last_code_char, in_identifier


def _transaction_control_flags(sql_text, known_savepoints=None):
    """Count explicit transaction-control statements outside strings/comments."""
    block_comment_depth = 0
    in_string = False
    in_double_quote = False
    in_identifier = False
    raw_tokens = []
    simple_txn_line_indices = set()
    savepoint_names = set(known_savepoints or ())
    flags = {
        "begin": 0,
        "commit": 0,
        "rollback": 0,
        "rollback_to_savepoint": 0,
        "save": 0,
        "ops": [],
        "has_control": False,
    }

    for line in _sql_lines(sql_text):
        line_started_clean = not (block_comment_depth or in_string or in_double_quote or in_identifier)
        block_comment_depth, in_string, in_double_quote, line_tokens, _, in_identifier = _scan_sql_line_state(
            line, block_comment_depth, in_string, in_double_quote,
            in_identifier=in_identifier
        )
        if line_started_clean and line_tokens and _is_simple_transaction_control_line(line):
            simple_txn_line_indices.add(len(raw_tokens))
        raw_tokens.extend(line_tokens)

    if _is_module_definition_tokens(raw_tokens):
        flags["savepoint_names"] = savepoint_names
        return flags

    # Strip ";" separators into a clean word list, recording which words begin a
    # new statement. BEGIN, COMMIT, ROLLBACK, SAVE and TRAN are reserved words,
    # so SQL Server runs them as statements wherever they appear -- including
    # mid-line after a SELECT with no ";" ("SELECT 1 AS x BEGIN TRAN"). The
    # exceptions are "KEYWORD = expr" alias forms (a syntax error the server
    # rejects before running anything, so they must not move the depth) and
    # ALTER DATABASE's "WITH ROLLBACK IMMEDIATE | AFTER n" termination option.
    tokens = []
    stmt_initial = []
    eq_after = set()  # indexes into tokens of words directly followed by "="
    prev_was_separator = True
    current_stmt_has_if = False
    current_stmt_if_control_seen = False
    current_stmt_has_select = False
    current_stmt_if_paren_depth = 0
    for idx, tok in enumerate(raw_tokens):
        if tok == "=":
            if tokens:
                eq_after.add(len(tokens) - 1)
            continue
        if tok == ";":
            prev_was_separator = True
            current_stmt_has_if = False
            current_stmt_if_control_seen = False
            current_stmt_has_select = False
            current_stmt_if_paren_depth = 0
            continue
        if tok == "(":
            if current_stmt_has_if and not current_stmt_if_control_seen:
                current_stmt_if_paren_depth += 1
            continue
        if tok == ")":
            if current_stmt_has_if and not current_stmt_if_control_seen:
                current_stmt_if_paren_depth = max(0, current_stmt_if_paren_depth - 1)
            continue
        tokens.append(tok)
        is_stmt_initial = prev_was_separator or idx in simple_txn_line_indices
        if (
            current_stmt_has_if
            and not current_stmt_if_control_seen
            and not current_stmt_has_select
            and tok in ("COMMIT", "ROLLBACK")
        ):
            is_stmt_initial = True
            current_stmt_if_control_seen = True
        stmt_initial.append(is_stmt_initial)
        if prev_was_separator:
            current_stmt_has_if = tok == "IF"
            current_stmt_if_control_seen = False
            current_stmt_has_select = tok == "SELECT"
            current_stmt_if_paren_depth = 0
        elif tok == "IF":
            current_stmt_has_if = True
            current_stmt_if_paren_depth = 0
        elif tok == "SELECT" and current_stmt_if_paren_depth == 0:
            current_stmt_has_select = True
        elif tok in ("END", "ELSE"):
            current_stmt_has_select = False
        prev_was_separator = False

    saw_begin_transaction = False
    transaction_names = set()
    control_words = {"BEGIN", "COMMIT", "ROLLBACK", "SAVE"}

    def _is_alias(i):
        """True for "KEYWORD = ..." or "KEYWORD TRAN[SACTION] = ..." select-list aliases."""
        if i in eq_after:
            return True
        return (i + 1 < len(tokens) and tokens[i + 1] in ("TRAN", "TRANSACTION")
                and i + 1 in eq_after)

    for i, token in enumerate(tokens):
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        nxt2 = tokens[i + 2] if i + 2 < len(tokens) else ""
        nxt3 = tokens[i + 3] if i + 3 < len(tokens) else ""
        prev = tokens[i - 1] if i > 0 else ""
        is_statement = stmt_initial[i] or not _is_alias(i)
        if token == "COMMIT" and is_statement:
            flags["has_control"] = True
            flags["commit"] += 1
            flags["ops"].append(("commit", None))
        if token == "ROLLBACK" and is_statement and (stmt_initial[i] or prev != "WITH"):
            flags["has_control"] = True
            if nxt in ("TRAN", "TRANSACTION"):
                rollback_target = nxt2
                if rollback_target and rollback_target not in ("TRAN", "TRANSACTION", "WORK"):
                    if rollback_target in transaction_names:
                        flags["rollback"] += 1
                        flags["ops"].append(("rollback", rollback_target))
                    elif rollback_target in savepoint_names:
                        flags["rollback_to_savepoint"] += 1
                        flags["ops"].append(("rollback_to_savepoint", rollback_target))
                    # Unknown name: SQL Server rejects it and keeps the transaction
                    # open, so don't update depth tracking as if it succeeded.
                else:
                    flags["rollback"] += 1
                    flags["ops"].append(("rollback", None))
            else:
                flags["rollback"] += 1
                flags["ops"].append(("rollback", None))
        begins_transaction = (
            token == "BEGIN"
            and is_statement
            and (
                nxt in ("TRAN", "TRANSACTION")
                or (nxt == "DISTRIBUTED" and nxt2 in ("TRAN", "TRANSACTION"))
            )
        )
        if begins_transaction:
            flags["has_control"] = True
            flags["begin"] += 1
            flags["ops"].append(("begin", None))
            saw_begin_transaction = True
            name_token = nxt3 if nxt == "DISTRIBUTED" else nxt2
            mark_token = tokens[i + 4] if nxt == "DISTRIBUTED" and i + 4 < len(tokens) else nxt3
            name_idx = (i + 3) if nxt == "DISTRIBUTED" else (i + 2)
            name_is_new_stmt = name_idx < len(stmt_initial) and stmt_initial[name_idx]
            if (name_token and not name_is_new_stmt
                    and name_token not in ("TRAN", "TRANSACTION", "WITH", "MARK")
                    and name_token not in control_words):
                transaction_names.add(name_token)
            elif name_token == "WITH" and mark_token == "MARK":
                saw_begin_transaction = False
        if token == "SAVE" and is_statement and nxt in ("TRAN", "TRANSACTION"):
            flags["has_control"] = True
            flags["save"] += 1
            flags["ops"].append(("save", nxt2 if nxt2 else None))
            if nxt2 and nxt2 not in ("TRAN", "TRANSACTION"):
                savepoint_names.add(nxt2)
        elif saw_begin_transaction and token not in ("TRAN", "TRANSACTION"):
            if not stmt_initial[i] and token not in ("WITH", "MARK") and token not in control_words:
                transaction_names.add(token)
            saw_begin_transaction = False
    flags["savepoint_names"] = savepoint_names
    return flags


def _is_module_definition_tokens(tokens):
    """Return True for CREATE/ALTER module batches whose body is not executed now."""
    words = [tok for tok in tokens if tok not in (";", "(", ")", "=")]
    if not words:
        return False
    module_keywords = {"PROC", "PROCEDURE", "FUNCTION", "TRIGGER", "VIEW"}
    if words[0] == "ALTER":
        return len(words) > 1 and words[1] in module_keywords
    if words[0] == "CREATE":
        if len(words) > 1 and words[1] in module_keywords:
            return True
        return len(words) > 3 and words[1] == "OR" and words[2] == "ALTER" and words[3] in module_keywords
    return False


def _is_simple_transaction_control_line(line):
    line = line.split("--", 1)[0]
    # Strip block comments with a character scan so nested /* /* */ */ is handled.
    stripped, depth, i = [], 0, 0
    while i < len(line):
        if i + 1 < len(line) and line[i] == "/" and line[i + 1] == "*":
            depth += 1; i += 2
        elif i + 1 < len(line) and line[i] == "*" and line[i + 1] == "/" and depth:
            depth -= 1; i += 2
        elif not depth:
            stripped.append(line[i]); i += 1
        else:
            i += 1
    line = "".join(stripped).strip()
    if line.endswith(";"):
        line = line[:-1].strip()
    return re.match(
        r"^(?:"
        r"BEGIN(?:\s+DISTRIBUTED)?\s+(?:TRAN|TRANSACTION)(?:\s+\S+)?"
        r"|"
        r"COMMIT(?:\s+(?:TRAN|TRANSACTION|WORK)(?:\s+\S+)?)?"
        r"|"
        r"ROLLBACK(?:\s+(?:(?:TRAN|TRANSACTION)(?:\s+\S+)?|WORK))?"
        r"|"
        r"SAVE\s+(?:TRAN|TRANSACTION)(?:\s+\S+)?"
        r")$",
        line,
        re.IGNORECASE,
    ) is not None


def _first_sql_token(sql_text, dialect="mssql"):
    block_comment_depth = 0
    in_string = False
    in_double_quote = False
    in_identifier = False
    for line in _sql_lines(sql_text):
        block_comment_depth, in_string, in_double_quote, tokens, _, in_identifier = _scan_sql_line_state(
            line, block_comment_depth, in_string, in_double_quote, dialect=dialect,
            in_identifier=in_identifier
        )
        for tok in tokens:
            if tok not in (";", "(", ")", "="):  # skip bare separators/operators
                return tok
    return ""


def _sql_parser_tokens(sql_text, dialect="mssql"):
    block_comment_depth = 0
    in_string = False
    in_double_quote = False
    in_identifier = False
    tokens = []
    for line in _sql_lines(sql_text):
        block_comment_depth, in_string, in_double_quote, line_tokens, _, in_identifier = _scan_sql_line_state(
            line, block_comment_depth, in_string, in_double_quote, dialect=dialect,
            in_identifier=in_identifier
        )
        tokens.extend(tok for tok in line_tokens if tok != "=")
    return tokens


def _starts_with_cte_data_change_statement(tokens):
    depth = 0
    data_change = {"INSERT", "UPDATE", "DELETE", "MERGE"}
    for tok in tokens[1:]:
        if tok == "(":
            depth += 1
            continue
        if tok == ")":
            depth = max(0, depth - 1)
            continue
        if depth != 0 or tok == ";":
            continue
        if tok in data_change:
            return True
        if tok == "SELECT":
            return False
    return False


def _starts_with_data_change_statement(sql_text, dialect="mssql"):
    # EXEC counts as a data change: a procedure that modifies data and returns
    # a result set produces no non-resultset state in execute_sql, so without
    # this its writes would be silently rolled back at connection close.
    # export_data's --query guard is unaffected — it requires SELECT/WITH as
    # the first token before consulting this predicate.
    raw = _sql_parser_tokens(sql_text, dialect)
    tokens = [tok for tok in raw if tok not in (";", "(", ")")]
    if not tokens:
        return False
    if tokens[0] in {"INSERT", "UPDATE", "DELETE", "MERGE", "EXEC", "EXECUTE"}:
        return True
    if tokens[0] == "WITH":
        return _starts_with_cte_data_change_statement(raw)
    return False


def _has_top_level_select_into(sql_text, dialect="mssql"):
    """Return True for SELECT ... INTO table creation at statement level."""
    depth = 0
    in_select = False
    for tok in _sql_parser_tokens(sql_text, dialect):
        if tok == "(":
            depth += 1
            continue
        if tok == ")":
            depth = max(0, depth - 1)
            continue
        if depth != 0:
            continue
        if tok == ";":
            in_select = False
            continue
        if tok == "SELECT":
            in_select = True
            continue
        if in_select and tok == "INTO":
            return True
        if in_select and tok == "FROM":
            in_select = False
    return False


_SQL_LINE_BREAK = re.compile(r"\r\n|\n|\r")


def _sql_lines(sql_text):
    """Split SQL into physical lines on CR, LF, or CRLF only.

    str.splitlines() also breaks on U+2028, VT, FF, NEL and similar characters,
    which are ordinary data inside string literals."""
    return _SQL_LINE_BREAK.split(sql_text)


def split_batches(sql_text, dialect="mssql"):
    """Split SQL text on GO statements (like SSMS does).

    Lines inside a batch keep their original terminators so CR/CRLF inside
    string literals reach the server unchanged."""
    batches = []
    current = []
    block_comment_depth = 0
    in_string = False
    in_double_quote = False
    in_identifier = False

    pieces = re.split(r"(\r\n|\n|\r)", sql_text)
    for line, line_end in zip(pieces[0::2], pieces[1::2] + [""]):
        m = None
        if not (block_comment_depth or in_string or in_double_quote or in_identifier):
            m = re.match(r"^\s*GO\s*(\d*)\s*(?:--.*)?$", line, re.IGNORECASE)
        if m:
            batch = "".join(current).strip()
            if batch:
                count = int(m.group(1)) if m.group(1) else 1
                if count > MAX_GO_REPEAT_COUNT:
                    raise ValueError(
                        f"GO repeat count {count:,} exceeds the limit of "
                        f"{MAX_GO_REPEAT_COUNT:,}"
                    )
                for _ in range(count):
                    batches.append(batch)
            current = []
        else:
            block_comment_depth, in_string, in_double_quote, _, _, in_identifier = _scan_sql_line_state(
                line, block_comment_depth, in_string, in_double_quote, dialect=dialect,
                in_identifier=in_identifier
            )
            current.append(line + line_end)
    remaining = "".join(current).strip()
    if remaining:
        batches.append(remaining)
    return batches


def _ends_with_statement_terminator(lines, dialect="mssql"):
    """Return True when the buffered SQL ends with a real statement terminator."""
    bcd, is_, idq, lcc, ident = 0, False, False, "", False
    for line in lines:
        bcd, is_, idq, _, lcc, ident = _scan_sql_line_state(line, bcd, is_, idq, lcc, dialect, ident)
    return not (bcd or is_ or idq or ident) and lcc == ";"


def _script_path(filepath):
    """Normalize a user-supplied SQL script path."""
    return Path(filepath).expanduser()


def execute_file(filepath, ctx):
    """Execute SQL from a file. Returns False when the script cannot be read."""
    path = _script_path(filepath)
    try:
        with path.open("rb") as f:
            data = f.read(MAX_SQL_INPUT_BYTES + 1)
        if len(data) > MAX_SQL_INPUT_BYTES:
            print(
                f"Error reading '{path}': SQL input exceeds the "
                f"{MAX_SQL_INPUT_BYTES // (1024 * 1024)} MiB limit.",
                file=sys.stderr,
            )
            return False
        text = data.decode("utf-8")
    except OSError as e:
        print(f"Error reading '{path}': {e}", file=sys.stderr)
        return False
    except UnicodeDecodeError as e:
        print(f"Error reading '{path}': invalid UTF-8: {e}", file=sys.stderr)
        return False
    ok = True
    try:
        batches = split_batches(text, _dialect(ctx))
    except ValueError as e:
        print(f"Error reading '{path}': {e}", file=sys.stderr)
        return False
    for batch in batches:
        if not execute_sql(batch, ctx):
            ok = False
    return ok


# ── Interactive REPL ───────────────────────────────────────────────────────────

if os.name == "nt":
    HISTORY_FILE = Path(os.environ.get("APPDATA", Path.home())) / "mssql_query_history.txt"
else:
    HISTORY_FILE = Path.home() / ".mssql_query_history"


def setup_readline():
    """Configure readline with history and tab completion."""
    if readline is None:
        return

    read_history_file = getattr(readline, "read_history_file", None)
    write_history_file = getattr(readline, "write_history_file", None)
    set_history_length = getattr(readline, "set_history_length", None)
    set_completer = getattr(readline, "set_completer", None)
    parse_and_bind = getattr(readline, "parse_and_bind", None)

    if read_history_file is not None:
        try:
            read_history_file(HISTORY_FILE)
        except (FileNotFoundError, OSError):
            pass
    if set_history_length is not None:
        set_history_length(1000)

    def _write_history():
        if write_history_file is not None:
            try:
                write_history_file(HISTORY_FILE)
            except OSError:
                pass

    if write_history_file is not None:
        atexit.register(_write_history)

    SQL_KEYWORDS = [
        "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
        "ON", "GROUP", "BY", "ORDER", "HAVING", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "ALTER", "DROP", "INDEX",
        "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER", "WITH", "AS", "DISTINCT",
        "TOP", "EXEC", "EXECUTE", "BEGIN", "END", "TRANSACTION", "COMMIT",
        "ROLLBACK", "DECLARE", "PRINT", "IF", "ELSE", "WHILE", "CASE", "WHEN",
        "THEN", "AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE", "BETWEEN",
        "EXISTS", "UNION", "ALL", "COUNT", "SUM", "AVG", "MIN", "MAX",
    ]
    DOT_COMMANDS = [
        ".tables", ".schema", ".databases", ".columns", ".views", ".procs",
        ".export", ".import", ".mode", ".headers", ".output", ".read",
        ".reconnect", ".timer", ".rowcount", ".quit", ".exit", ".help",
    ]

    completer_items = SQL_KEYWORDS + [k.lower() for k in SQL_KEYWORDS] + DOT_COMMANDS

    # Cache the last match list — readline calls completer(text, 0), (text, 1), …
    # for each candidate, so recomputing for every state is wasteful.
    _cache = [None, []]   # [last_text, last_matches]

    def completer(text, state):
        if text != _cache[0]:
            _cache[0] = text
            _cache[1] = [c for c in completer_items if c.startswith(text)]
        matches = _cache[1]
        return matches[state] if state < len(matches) else None

    if set_completer is not None:
        set_completer(completer)
    if parse_and_bind is not None:
        bind_command = (
            "bind ^I rl_complete"
            if "libedit" in (readline.__doc__ or "")
            else "tab: complete"
        )
        parse_and_bind(bind_command)


def _handle_repl_interrupt(ctx):
    """Recover from Ctrl-C during statement execution without killing the REPL.

    The interrupted statement's server-side effect is unknown (it may have
    completed, partially run, or never started), so resync transaction
    tracking from the server instead of trusting the lexical counters.
    """
    print("\nInterrupted.", file=sys.stderr)
    conn = ctx.get("conn")
    if _cancel_mysql_interrupt(ctx):
        return
    if conn is not None:
        _resync_transaction_state(conn, ctx)


def _run_repl_action(action, ctx):
    """Keep the session available after interruption or output failure."""
    try:
        return action()
    except KeyboardInterrupt:
        _handle_repl_interrupt(ctx)
    except OSError as error:
        print(f"Output error: {error}. Change .output before retrying.", file=sys.stderr)
    except ResultFormatError as error:
        print(f"Result format error: {error}", file=sys.stderr)
    return None


def interactive_loop(ctx):
    """Run the interactive SQL REPL."""
    dialect = _dialect(ctx)
    conn_info = ctx.get("conn_info", dialect)
    driver_name = ctx.get("driver_name", "pymssql")
    driver_version = ctx.get("driver_version", getattr(pymssql, "__version__", "?"))
    product = "MySQL" if dialect == "mysql" else "MSSQL"
    print(f"{product} Query Tool  ({driver_name} {driver_version})")
    print(f"Connected to: {conn_info}")
    print("Type .help for help, .quit to exit. Terminate SQL with ; or GO.")
    print()

    setup_readline()

    buffer = []
    continuation = False
    block_comment_depth = 0
    in_string = False
    in_double_quote = False
    in_identifier = False
    last_code_char = ""

    def _reset():
        nonlocal buffer, continuation, block_comment_depth, in_string, in_double_quote, last_code_char, in_identifier
        buffer = []
        continuation = False
        block_comment_depth = 0
        in_string = False
        in_double_quote = False
        in_identifier = False
        last_code_char = ""

    while True:
        try:
            prompt = "   ...> " if continuation else f"{dialect}> "
            line = input(prompt)
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            _reset()
            continue

        # Dot commands (only at start of a fresh statement)
        stripped = line.strip()
        if stripped.startswith(".") and not continuation:
            known = _run_repl_action(lambda: run_dot_command(stripped, ctx), ctx)
            if known is False:
                print(f"Unknown command '{stripped.split()[0]}'. Try .help", file=sys.stderr)
            continue

        # Accumulate lines
        buffer.append(line)

        # Check for GO batch separator on its own line
        go_m = None
        if not (block_comment_depth or in_string or in_double_quote or in_identifier):
            go_m = re.match(r"^\s*GO\s*(\d*)\s*(?:--.*)?$", stripped, re.IGNORECASE)
        if go_m:
            sql = "\n".join(buffer[:-1]).strip()
            if sql:
                count = int(go_m.group(1)) if go_m.group(1) else 1
                if count > MAX_GO_REPEAT_COUNT:
                    print(
                        f"Error: GO repeat count {count:,} exceeds the limit of "
                        f"{MAX_GO_REPEAT_COUNT:,}",
                        file=sys.stderr,
                    )
                    _reset()
                    continue
                for _ in range(count):
                    if _run_repl_action(lambda: execute_sql(sql, ctx), ctx) is None:
                        break
            _reset()
            continue

        # Advance incremental lexer state — single pass tracks both token
        # structure (for GO detection next turn) and last_code_char.
        block_comment_depth, in_string, in_double_quote, _, last_code_char, in_identifier = \
            _scan_sql_line_state(line, block_comment_depth, in_string, in_double_quote, last_code_char, dialect, in_identifier)

        # Check if the accumulated buffer ends with ;
        combined = "\n".join(buffer).strip()
        if not combined:
            # All whitespace — discard to keep the buffer from accumulating blank lines
            _reset()
        elif not (block_comment_depth or in_string or in_double_quote or in_identifier) and last_code_char == ";":
            # Keep terminators that SQL Server requires, for example on MERGE.
            sql = combined
            if sql:
                try:
                    batches = split_batches(sql, dialect)
                except ValueError as e:
                    print(f"Error: {e}", file=sys.stderr)
                    _reset()
                    continue
                for batch in batches:
                    if _run_repl_action(lambda: execute_sql(batch, ctx), ctx) is None:
                        break
            _reset()
        else:
            continuation = True


# ── Connection ─────────────────────────────────────────────────────────────────

DEFAULT_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"

MAX_ENV_FILE_BYTES = 1024 * 1024

# Backend-neutral names accepted in an --env-file (the common .env
# convention), mapped onto whichever backend is active: DB_HOST becomes
# MYSQL_HOST with --mysql and MSSQL_HOST otherwise.
ENV_FILE_GENERIC_KEYS = {
    "DB_HOST": "HOST",
    "DB_PORT": "PORT",
    "DB_USER": "USER",
    "DB_PASSWORD": "PASSWORD",
    "DB_NAME": "DATABASE",
    "DB_DATABASE": "DATABASE",
}


def load_env_file(path):
    """Parse a dotenv-style file into a dict of KEY -> value.

    Supports blank lines, # comments, an optional leading "export ", single-
    or double-quoted values, and inline " #" comments after unquoted values.
    No variable expansion. Errors cite the line number only — never the line
    itself, which may hold a password. Raises OSError / ValueError.
    """
    path = Path(path).expanduser()
    with path.open("rb") as f:
        data = f.read(MAX_ENV_FILE_BYTES + 1)
    if len(data) > MAX_ENV_FILE_BYTES:
        raise ValueError(f"{path}: exceeds the {MAX_ENV_FILE_BYTES // 1024} KiB limit")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise ValueError(f"{path}: invalid UTF-8: {e}") from None

    values = {}
    for lineno, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if re.match(r"export\s", line):
            line = line[len("export"):].lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"{path}:{lineno}: expected KEY=VALUE")
        value = value.strip()
        if value[:1] in ("'", '"'):
            quote = value[0]
            end = value.find(quote, 1)
            if end == -1:
                raise ValueError(f"{path}:{lineno}: unterminated quoted value")
            rest = value[end + 1:].strip()
            if rest and not rest.startswith("#"):
                raise ValueError(f"{path}:{lineno}: unexpected text after quoted value")
            value = value[1:end]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0]
        values[key] = value
    return values


def _resolve_env_file_values(file_values, prefix):
    """Map an env file's keys onto PREFIX_* connection settings. A
    backend-specific key (e.g. MYSQL_HOST) beats its generic DB_* alias."""
    resolved = {}
    for generic_key, suffix in ENV_FILE_GENERIC_KEYS.items():
        if generic_key in file_values:
            resolved.setdefault(f"{prefix}_{suffix}", file_values[generic_key])
    for key, value in file_values.items():
        if key.startswith(prefix + "_"):
            resolved[key] = value
    return resolved


def _conn_setting(args, name, default=""):
    """Look up a connection env setting: --env-file values first, then the
    process environment. (Command-line flags are checked by the caller.)"""
    env_file_values = getattr(args, "env_file_values", None) or {}
    if name in env_file_values:
        return env_file_values[name]
    return os.environ.get(name, default)


def _connect_sql_auth(args):
    """Build a pymssql connection (standard SQL login) from args + env vars.

    Raises ValueError for bad parameters (including a missing pymssql
    install) and pymssql.Error if the server rejects the connection.
    """
    if pymssql is None:
        raise ValueError(f"pymssql is not installed. Run:\n  {PYMSSQL_INSTALL_HINT}")
    host     = args.host     or _conn_setting(args, "MSSQL_HOST",     "localhost")
    port_env  = _conn_setting(args, "MSSQL_PORT", "1433")
    try:
        port = args.port or int(port_env)
    except ValueError:
        raise ValueError(f"invalid MSSQL_PORT '{port_env}'")
    user     = args.user     or _conn_setting(args, "MSSQL_USER",     "")
    password = (args.password if args.password is not None
                else _conn_setting(args, "MSSQL_PASSWORD", ""))
    database = args.database or _conn_setting(args, "MSSQL_DATABASE", "")

    if not user:
        raise ValueError("no user specified (--user or MSSQL_USER)")

    conn_info = f"{user}@{host}:{port}/{database}" if database else f"{user}@{host}:{port}"

    conn = pymssql.connect(
        server=host,
        port=port,
        user=user,
        password=password,
        database=database if database else None,
        login_timeout=args.timeout,
        timeout=getattr(args, "query_timeout", 30),
        charset="UTF-8",
        # User BEGIN/COMMIT must start from @@TRANCOUNT=0. Ordinary work
        # temporarily enables pymssql's driver-owned transaction instead.
        autocommit=True,
    )

    driver = {
        "name": "pymssql", "version": getattr(pymssql, "__version__", "?"),
        "paramstyle": "format", "dialect": "mssql",
    }
    _warn_old_pymssql(driver["version"])
    _install_pymssql_message_handler(conn)
    return conn, conn_info, driver


def _install_pymssql_message_handler(conn):
    """Show PRINT / RAISERROR (severity <= 10) messages, which pymssql drops.

    Installed after login so the server's login messages stay hidden. Errors
    (severity > 10) also raise exceptions and are reported there instead."""
    set_handler = getattr(getattr(conn, "_conn", None), "set_msghandler", None)
    if set_handler is None:
        return

    def handler(msgstate, severity, srvname, procname, line, msgtext):
        if severity > 10:
            return
        if isinstance(msgtext, bytes):
            msgtext = msgtext.decode("utf-8", "replace")
        print(msgtext, file=sys.stderr)

    set_handler(handler)


_ODBC_MESSAGE_PREFIX = re.compile(r"^(?:\[[^\]]*\])+")


def _print_odbc_messages(cursor):
    """Show PRINT / RAISERROR info messages that pyodbc collects per result set."""
    for message in getattr(cursor, "messages", None) or ():
        text = message[1] if isinstance(message, (tuple, list)) and len(message) > 1 else message
        print(_ODBC_MESSAGE_PREFIX.sub("", str(text)), file=sys.stderr)


def _warn_old_pymssql(version):
    """Warn once per connection when pymssql is known to corrupt temporal values."""
    match = re.match(r"(\d+)\.(\d+)", str(version))
    if match and tuple(int(p) for p in match.groups()) < PYMSSQL_MIN_RELIABLE_VERSION:
        print(
            f"Warning: pymssql {version} returns datetimeoffset values as raw "
            "bytes and misreads the milliseconds of time/datetime2 values. "
            "Upgrade with: pip install -U 'pymssql>=2.4'",
            file=sys.stderr,
        )


def _odbc_escape_braced(value):
    """Quote a value for an ODBC connection-string field using ODBC's own
    {...} quoting, so an embedded ';' (the connection string's own key
    separator) can't leak into — or inject — an adjacent field. A literal
    '}' inside the value must be doubled, per ODBC's escaping convention."""
    return "{" + str(value).replace("}", "}}") + "}"


def _decode_datetimeoffset(raw):
    if raw is None:
        return None
    parts = struct.unpack("<6hI2h", raw)
    offset = datetime.timedelta(hours=parts[7], minutes=parts[8])
    return datetime.datetime(*parts[:6], microsecond=parts[6] // 1000,
                             tzinfo=datetime.timezone(offset))


def _connect_trusted(args):
    """Build a pyodbc connection using Windows Integrated Authentication
    (Trusted_Connection=yes) from args + env vars. No username/password is
    sent — the OS's current login (Kerberos/NTLM/SSO) authenticates instead.

    Raises ValueError for bad parameters (including a missing pyodbc
    install) and pyodbc.Error if the server rejects the connection.
    """
    if pyodbc is None:
        raise ValueError(
            "pyodbc is not installed (required for --trusted-connection). Run:\n"
            "  pip install pyodbc\n"
            "  Also install the 'ODBC Driver for SQL Server' from Microsoft:\n"
            "  https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
        )
    host     = args.host or _conn_setting(args, "MSSQL_HOST", "localhost")
    port_env = _conn_setting(args, "MSSQL_PORT", "1433")
    try:
        port = args.port or int(port_env)
    except ValueError:
        raise ValueError(f"invalid MSSQL_PORT '{port_env}'")
    database = args.database or _conn_setting(args, "MSSQL_DATABASE", "")
    driver_name = (
        getattr(args, "odbc_driver", "")
        or _conn_setting(args, "MSSQL_ODBC_DRIVER", "")
        or DEFAULT_ODBC_DRIVER
    )

    # host/database/driver_name come from flags or env vars, not literal SQL,
    # but they still flow into a ';'-delimited connection string, so a stray
    # ';' or '}' in any of them could inject extra keys (e.g. a database
    # named "mydb;Encrypt=no") if passed through unquoted.
    conn_str_parts = [
        f"DRIVER={_odbc_escape_braced(driver_name)}",
        f"SERVER={_odbc_escape_braced(f'{host},{port}')}",
        "Trusted_Connection=yes",
    ]
    if database:
        conn_str_parts.append(f"DATABASE={_odbc_escape_braced(database)}")
    if getattr(args, "trust_server_certificate", False):
        conn_str_parts.append("TrustServerCertificate=yes")
    conn_str = ";".join(conn_str_parts)

    conn = pyodbc.connect(conn_str, timeout=args.timeout, autocommit=True)
    try:
        conn.add_output_converter(-155, _decode_datetimeoffset)
        conn.timeout = getattr(args, "query_timeout", 30)
    except BaseException:
        try:
            conn.close()
        except Exception:
            pass
        raise

    conn_info = f"(trusted)@{host}:{port}/{database}" if database else f"(trusted)@{host}:{port}"
    driver = {
        "name": "pyodbc", "version": getattr(pyodbc, "version", "?"),
        "paramstyle": "qmark", "dialect": "mssql",
    }
    return conn, conn_info, driver


DEFAULT_MYSQL_PORT = 3306


def _connect_mysql(args):
    """Build a pymysql connection to a MySQL server from args + MYSQL_* env vars.

    The connection uses server-side autocommit (like the mysql CLI): each
    statement commits on its own unless the user explicitly opens a
    transaction, and execute_sql reads the server's IN_TRANS flag to track
    that (see _apply_mysql_transaction_status). Multi-statement batches are
    enabled because the REPL and script runner send everything between GO
    separators as one batch, exactly as with SQL Server.

    Raises ValueError for bad parameters (including a missing pymysql
    install) and pymysql.Error if the server rejects the connection.
    """
    if pymysql is None:
        raise ValueError('pymysql is not installed (required for --mysql). Run:\n  pip install "pymysql[rsa]"')
    host     = args.host or _conn_setting(args, "MYSQL_HOST", "localhost")
    port_env = _conn_setting(args, "MYSQL_PORT", str(DEFAULT_MYSQL_PORT))
    try:
        port = args.port or int(port_env)
    except ValueError:
        raise ValueError(f"invalid MYSQL_PORT '{port_env}'")
    user     = args.user     or _conn_setting(args, "MYSQL_USER",     "")
    password = (args.password if args.password is not None
                else _conn_setting(args, "MYSQL_PASSWORD", ""))
    database = args.database or _conn_setting(args, "MYSQL_DATABASE", "")

    if not user:
        raise ValueError("no user specified (--user or MYSQL_USER)")

    conn_info = f"{user}@{host}:{port}/{database}" if database else f"{user}@{host}:{port}"

    query_timeout = getattr(args, "query_timeout", 30)
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password.encode("utf-8"),
            database=database if database else None,
            connect_timeout=args.timeout,
            read_timeout=query_timeout,
            write_timeout=query_timeout,
            charset="utf8mb4",
            autocommit=True,
            client_flag=MYSQL_CLIENT_MULTI_STATEMENTS,
        )
    except RuntimeError as error:
        if "'cryptography' package is required" not in str(error):
            raise
        raise ValueError('MySQL authentication requires RSA support. Run:\n'
                         '  pip install "pymysql[rsa]"') from error

    _install_mysql_status_tracking(conn)
    driver = {
        "name": "pymysql", "version": getattr(pymysql, "__version__", "?"),
        "paramstyle": "format", "dialect": "mysql",
    }
    return conn, conn_info, driver


def _connect(args):
    """Build a database connection from args + env vars, dispatching to
    MySQL (pymysql) when args.mysql is set, otherwise to standard SQL Server
    login (pymssql) or Windows Integrated / trusted-connection auth (pyodbc)
    based on args.trusted_connection.

    Returns (conn, conn_info, driver) where driver is a dict with name,
    version, and paramstyle (used to adapt internally-generated SQL's
    placeholder style — pymssql accepts %s, pyodbc requires ?).

    Raises ValueError for bad parameters and the active driver's Error type
    if the server rejects the connection. Callers decide how to handle
    failure — the CLI startup path exits the process (see build_connection);
    .reconnect reports the error and keeps the REPL alive.
    """
    if getattr(args, "mysql", False):
        return _connect_mysql(args)
    if getattr(args, "trusted_connection", False):
        return _connect_trusted(args)
    return _connect_sql_auth(args)


def build_connection(args):
    """Build a database connection from args + env vars; exits the process on failure."""
    try:
        return _connect(args)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except DBError as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        sys.exit(1)


def _normalize_cli_args(args, parser):
    """Resolve positional DATABASE/SQL_FILE forms and reject ambiguous execution sources."""
    if args.database:
        if args.sql_file:
            parser.error("when --database is used, provide at most one positional SQL_FILE")
        args.sql_file = args.database_arg
    else:
        args.database = args.database_arg

    explicit_sources = [
        ("--command", bool(args.command)),
        ("--file", bool(args.file)),
        ("SQL_FILE", bool(args.sql_file)),
    ]
    selected = [name for name, present in explicit_sources if present]
    if len(selected) > 1:
        parser.error(
            "choose only one execution source: "
            "--command, --file, or positional SQL_FILE"
        )

    if args.trusted_connection and (args.user or args.password is not None):
        parser.error(
            "--trusted-connection cannot be combined with --user/-U or --password/-W"
        )
    if args.mysql and (args.trusted_connection or args.odbc_driver or args.trust_server_certificate):
        parser.error(
            "--mysql cannot be combined with --trusted-connection, --odbc-driver, "
            "or --trust-server-certificate"
        )


# ── CLI Entry Point ────────────────────────────────────────────────────────────

def _harden_console_encoding():
    """Force UTF-8 (with lossy fallback) and disable newline translation.

    Status and result text use non-ASCII characters (box-drawing dividers,
    em dashes, arrows). Python only uses the Windows console's native
    Unicode API when stdout/stderr are an actual console; redirecting
    either to a file or pipe drops back to the locale's default text
    encoding (often cp1252 on Windows) with strict error handling, which
    can raise UnicodeEncodeError and kill the process mid-command — often
    while trying to print an error. Disabling newline translation prevents
    CSV's CRLF records and multiline fields from acquiring extra carriage
    returns on Windows. reconfigure() isn't available on plain
    file-like test doubles, so this is a no-op there.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace", newline="")
            except (OSError, ValueError):
                pass


def main():
    _harden_console_encoding()
    parser = argparse.ArgumentParser(
        prog="mssql_query",
        description="Command-line MSSQL (or MySQL) query tool (like sqlite3 CLI)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Set MSSQL_PASSWORD in your environment before running these commands.
  %(prog)s -H myserver -U sa -d mydb
  %(prog)s -H myserver -U sa -d mydb -c "SELECT TOP 5 * FROM users"
  %(prog)s -H myserver -U sa mydb
  %(prog)s -H myserver -U sa -d mydb query.sql
  %(prog)s -H myserver -U sa mydb query.sql --mode csv
  %(prog)s -H myserver -U sa -f query.sql
  echo "SELECT 1" | %(prog)s -H myserver -U sa
  # Windows Integrated Authentication (no username/password; requires pyodbc
  # and the ODBC Driver for SQL Server):
  %(prog)s -H myserver -E -d mydb
  # MySQL (requires pymysql; set MYSQL_PASSWORD first):
  %(prog)s --mysql -H myserver -U app -d mydb

  # Read connection settings from a dotenv-style file:
  %(prog)s --mysql --env-file .env-mysql

Environment variables: MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE, MSSQL_ODBC_DRIVER
With --mysql instead:  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
--env-file accepts those names or generic DB_HOST, DB_PORT, DB_USER, DB_PASSWORD,
DB_NAME; precedence is command-line flags, then the env file, then the environment.
        """,
    )

    # Connection
    conn_grp = parser.add_argument_group("connection")
    conn_grp.add_argument("-H", "--host",     default="", metavar="HOST",     help="Server hostname (default: localhost)")
    conn_grp.add_argument("-P", "--port",     default=0,  type=int, metavar="PORT", help="Port (default: 1433, or 3306 with --mysql)")
    conn_grp.add_argument("-U", "--user",     default="", metavar="USER",     help="Login username")
    conn_grp.add_argument(
        "-W", "--password", default=None, metavar="PASSWORD",
        help="Login password (prefer MSSQL_PASSWORD/MYSQL_PASSWORD; avoid putting secrets on command lines)",
    )
    conn_grp.add_argument("-d", "--database", default="", metavar="DATABASE", help="Database name")
    conn_grp.add_argument(
        "--env-file", default="", metavar="FILE", dest="env_file",
        help="Read connection settings from a dotenv-style KEY=VALUE file "
             "(overrides environment variables; flags still win)",
    )
    conn_grp.add_argument("--timeout",        default=30, type=int, metavar="SECS", help="Login timeout in seconds (default: 30)")
    conn_grp.add_argument("--query-timeout",  default=30, type=int, metavar="SECS", help="Query timeout in seconds (default: 30)")
    conn_grp.add_argument(
        "--mysql", action="store_true",
        help="Connect to a MySQL server instead of SQL Server (requires pymysql; "
             "reads MYSQL_* env vars instead of MSSQL_*)",
    )
    conn_grp.add_argument(
        "-E", "--trusted-connection", action="store_true", dest="trusted_connection",
        help="Use Windows Integrated Authentication (SSO) instead of a SQL login. "
             "Requires pyodbc and the ODBC Driver for SQL Server; not compatible with -U/-W.",
    )
    conn_grp.add_argument(
        "--odbc-driver", default="", metavar="DRIVER", dest="odbc_driver",
        help=f"ODBC driver name for --trusted-connection (default: '{DEFAULT_ODBC_DRIVER}'; "
             "or MSSQL_ODBC_DRIVER)",
    )
    conn_grp.add_argument(
        "--trust-server-certificate", action="store_true", dest="trust_server_certificate",
        help="Skip TLS certificate validation for --trusted-connection "
             "(needed for self-signed certs, e.g. local/dev SQL Server with ODBC Driver 18+)",
    )

    # Execution
    exec_grp = parser.add_argument_group("execution")
    exec_grp.add_argument("-c", "--command",  default="", metavar="SQL",      help="Execute SQL and exit")
    exec_grp.add_argument("-f", "--file",     default="", metavar="FILE",     help="Execute SQL file and exit")
    exec_grp.add_argument("database_arg",     nargs="?",  metavar="DATABASE", help="Database name (positional)")
    exec_grp.add_argument("sql_file",         nargs="?",  metavar="SQL_FILE", help="SQL file to execute after DATABASE")

    # Output
    out_grp = parser.add_argument_group("output")
    out_grp.add_argument("--mode",    default="table", choices=ALL_MODES, help="Output mode (default: table)")
    out_grp.add_argument("--insert-table", default="", metavar="TABLE", help="Table name for --mode insert")
    out_grp.add_argument("--insert-microseconds", action="store_true",
                         help="In insert mode, keep sub-millisecond digits of datetime values "
                              "(datetime2/datetimeoffset) instead of truncating to milliseconds; "
                              "a legacy datetime target column rejects such values")
    out_grp.add_argument("--no-headers", action="store_true", help="Suppress column headers")
    out_grp.add_argument("--timer",   action="store_true", help="Show query execution time")
    out_grp.add_argument("--rowcount", action="store_true", help="Show row count (default on in interactive)")
    out_grp.add_argument("-o", "--output", default="", metavar="FILE", help="Write output to file")

    args = parser.parse_args()
    _normalize_cli_args(args, parser)

    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0")
    if args.query_timeout <= 0:
        parser.error("--query-timeout must be greater than 0")

    # Validate insert mode requires a table name
    if args.mode == "insert" and not args.insert_table:
        parser.error("--mode insert requires --insert-table TABLE")

    args.env_file_values = {}
    if args.env_file:
        try:
            args.env_file_values = _resolve_env_file_values(
                load_env_file(args.env_file), "MYSQL" if args.mysql else "MSSQL"
            )
        except (OSError, ValueError) as e:
            print(f"Error reading env file: {e}", file=sys.stderr)
            sys.exit(1)

    conn = None
    out_file = sys.stdout
    ctx = None
    try:
        # Connect
        conn, conn_info, driver = build_connection(args)

        # Output destination
        if args.output:
            try:
                out_file = open(args.output, "w", newline="", encoding="utf-8")
            except OSError as e:
                print(f"Error opening output file: {e}", file=sys.stderr)
                sys.exit(1)

        # Context dict shared across all operations
        ctx = {
            "conn":              conn,
            "conn_info":         conn_info,
            "conn_args":         args,
            "driver_name":       driver["name"],
            "driver_version":    driver["version"],
            "paramstyle":        driver["paramstyle"],
            "dialect":           driver.get("dialect", "mssql"),
            "mode":              args.mode,
            "insert_table":      (
                _bracket_table(args.insert_table, driver.get("dialect", "mssql"))
                if args.insert_table else "<table>"
            ),
            "insert_microseconds": args.insert_microseconds,
            "headers":           not args.no_headers,
            "timer":             args.timer,
            "rowcount":          args.rowcount or (not args.command and not args.file and not args.sql_file and sys.stdin.isatty()),
            "output":            out_file,
            "transaction_depth": 0,
            "savepoints":        set(),
        }

        # Priority: -c > -f > positional file > stdin pipe > interactive
        if args.command:
            ok = True
            try:
                batches = split_batches(args.command, _dialect(ctx))
            except ValueError as e:
                parser.error(str(e))
            for batch in batches:
                if not execute_sql(batch, ctx):
                    ok = False
            if not ok:
                sys.exit(1)

        elif args.file:
            if not execute_file(args.file, ctx):
                sys.exit(1)

        elif args.sql_file:
            if not execute_file(args.sql_file, ctx):
                sys.exit(1)

        elif not sys.stdin.isatty():
            # Piped input. Read raw bytes and decode as UTF-8 explicitly
            # (mirrors execute_file) so this doesn't depend on the
            # platform/console locale — e.g. on Windows sys.stdin defaults to
            # the console codepage (often cp1252), which would otherwise
            # raise an uncaught UnicodeDecodeError or mis-decode non-ASCII
            # SQL text piped in from a UTF-8 source.
            data = sys.stdin.buffer.read(MAX_SQL_INPUT_BYTES + 1)
            if len(data) > MAX_SQL_INPUT_BYTES:
                print(
                    f"Error reading stdin: SQL input exceeds the "
                    f"{MAX_SQL_INPUT_BYTES // (1024 * 1024)} MiB limit.",
                    file=sys.stderr,
                )
                sys.exit(1)
            try:
                sql_text = data.decode("utf-8")
            except UnicodeDecodeError as e:
                print(f"Error reading stdin: invalid UTF-8: {e}", file=sys.stderr)
                sys.exit(1)
            ok = True
            try:
                batches = split_batches(sql_text, _dialect(ctx))
            except ValueError as e:
                print(f"Error reading stdin: {e}", file=sys.stderr)
                sys.exit(1)
            for batch in batches:
                if not execute_sql(batch, ctx):
                    ok = False
            if not ok:
                sys.exit(1)

        else:
            # Interactive mode
            interactive_loop(ctx)

    except (OSError, ResultFormatError) as error:
        print(f"Output error: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    finally:
        final_conn = ctx.get("conn", conn) if ctx is not None else conn
        if final_conn is not None:
            try:
                final_conn.close()
            except DBError as e:
                print(f"Error closing database connection: {e}", file=sys.stderr)
        final_out = ctx.get("output", out_file) if ctx is not None else out_file
        if final_out is not sys.stdout:
            try:
                final_out.close()
            except OSError as e:
                print(f"Error closing output file: {e}", file=sys.stderr)
                raise SystemExit(1) from None


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        # The reading end of a pipe closed early (e.g. `mssql.py ... | head`).
        # execute_sql/export_data already make sure any DML in the same batch
        # is committed before this propagates; this just avoids dumping a raw
        # traceback and Python's own "Exception ignored" cleanup message for
        # what is normal, expected Unix pipeline behavior.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)

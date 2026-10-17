import importlib.util
import csv
import datetime
import io
import json
import os
import re
import shlex
import struct
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


FakePymssqlError = type("FakePymssqlError", (Exception,), {})
FakePyodbcError = type("FakePyodbcError", (Exception,), {})
FakePymysqlError = type("FakePymysqlError", (Exception,), {})


def load_mssql_module(module_name="mssql_under_test", **pymssql_attrs):
    fake_pymssql = types.SimpleNamespace(
        Error=FakePymssqlError,
        __version__="test",
        connect=lambda **kwargs: None,
        **pymssql_attrs,
    )
    # Distinct driver errors keep RuntimeError, OSError, etc. from being
    # accidentally swallowed as database errors by tests.
    fake_pyodbc = types.SimpleNamespace(
        Error=FakePyodbcError,
        version="test-pyodbc",
        connect=lambda *args, **kwargs: None,
    )
    fake_pymysql = types.SimpleNamespace(
        Error=FakePymysqlError,
        __version__="test-pymysql",
        connect=lambda **kwargs: None,
    )

    old_module = sys.modules.get(module_name)
    old_pymssql = sys.modules.get("pymssql")
    old_pyodbc = sys.modules.get("pyodbc")
    old_pymysql = sys.modules.get("pymysql")
    try:
        sys.modules["pymssql"] = fake_pymssql
        sys.modules["pyodbc"] = fake_pyodbc
        sys.modules["pymysql"] = fake_pymysql
        spec = importlib.util.spec_from_file_location(
            module_name, Path(__file__).with_name("mssql.py")
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        if old_pymssql is not None:
            sys.modules["pymssql"] = old_pymssql
        else:
            sys.modules.pop("pymssql", None)

        if old_pyodbc is not None:
            sys.modules["pyodbc"] = old_pyodbc
        else:
            sys.modules.pop("pyodbc", None)

        if old_pymysql is not None:
            sys.modules["pymysql"] = old_pymysql
        else:
            sys.modules.pop("pymysql", None)

        if old_module is not None:
            sys.modules[module_name] = old_module
        else:
            sys.modules.pop(module_name, None)


MSSQL = load_mssql_module()


def fake_piped_stdin(data):
    """Build a stdin stand-in matching real sys.stdin's shape: a text stream
    backed by a .buffer of raw bytes, so code reading via sys.stdin.buffer
    (as main() does for piped SQL) works the same as it would on a real
    pipe. `data` may be str (encoded as UTF-8) or bytes."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return io.TextIOWrapper(io.BytesIO(data), encoding="utf-8")


def quote_dot_arg(value):
    """Quote a path or value passed through a dot-command string."""
    return shlex.quote(str(value))


def split_stacked_insert(sql, params):
    """Decode one stacked-INSERT import request into (row_sql, [row_params]).

    SQL Server imports send many single-row INSERTs per request inside
    BEGIN TRY ... END TRY; fakes record/execute them as the logical rows.
    Returns None for any other statement."""
    if not sql.startswith("BEGIN TRY\n"):
        return None
    statements = [line for line in sql.splitlines() if line.startswith("INSERT")]
    row_sql = statements[0].rstrip(";")
    if isinstance(params, dict):
        values = [params[f"p{i}"] for i in range(len(params))]
    else:
        values = list(params)
    width = len(values) // len(statements)
    rows = [tuple(values[i * width:(i + 1) * width]) for i in range(len(statements))]
    if isinstance(params, dict):
        rows = [{f"p{j}": v for j, v in enumerate(row)} for row in rows]
    return row_sql, rows


class FakeCursor:
    def __init__(self, states=None):
        self.states = states or []
        self.index = 0
        self.executed = []
        self.close_count = 0

    def execute(self, sql, params=()):
        self.executed.append((sql, params))

    def executemany(self, sql, params_seq):
        params_seq = list(params_seq)
        decoded = [split_stacked_insert(sql, params) for params in params_seq]
        if decoded and decoded[0]:
            self.executed.append((decoded[0][0], [row for _, rows in decoded for row in rows]))
        else:
            self.executed.append((sql, params_seq))

    @property
    def description(self):
        return self.states[self.index].get("description")

    def fetchall(self):
        return self.states[self.index].get("rows", [])

    def fetchmany(self, size):
        return []

    @property
    def rowcount(self):
        return self.states[self.index].get("rowcount", -1)

    def nextset(self):
        self.index += 1
        return self.index < len(self.states)

    def close(self):
        self.close_count += 1


class FakeConn:
    def __init__(self, cursor=None):
        self._cursor = cursor or FakeCursor()
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False
        self.autocommit_calls = []
        self.output_converters = {}

    def autocommit(self, value):
        self.autocommit_calls.append(value)

    def add_output_converter(self, sql_type, converter):
        self.output_converters[sql_type] = converter

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


class FreshCursorConn(FakeConn):
    def __init__(self, states_factory):
        super().__init__(cursor=None)
        self._states_factory = states_factory

    def cursor(self):
        return FakeCursor(self._states_factory())


FAKE_DRIVER = {"name": "pymssql", "version": "test", "paramstyle": "format"}


class TransactionCursor:
    """Small server model: rows become durable only at the outer COMMIT."""

    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self.rowcount = -1
        self.rows = []

    def execute(self, sql, params=()):
        decoded = split_stacked_insert(sql, params)
        if decoded:  # run each row; the first failure stops the rest (THROW)
            row_sql, rows = decoded
            for row in rows:
                self.execute(row_sql, row)
            return
        self.conn.executed.append((sql, params))
        if "%(p0)s" in sql and not isinstance(params, dict):
            raise self.conn.error_type("format requires a mapping")  # as pymssql does
        if isinstance(params, dict):  # mssql.py sends pymssql named %(pN)s params
            params = tuple(params[f"p{i}"] for i in range(len(params)))
        self.description = None
        self.rows = []
        for statement in re.split(r";|\n(?=BEGIN\s)", sql, flags=re.IGNORECASE):
            upper = statement.strip().upper()
            if upper.startswith("IF 1 = 0") or upper.startswith("IF 1=0"):
                continue
            if upper.startswith(("IF @@TRANCOUNT > 0", "IF EXISTS (SELECT 1)")):
                if upper.startswith("IF @@TRANCOUNT") and not self.conn.depth:
                    continue
                upper = re.search(r"\b(COMMIT|ROLLBACK)\b.*", upper)[0]
            if upper.startswith("BEGIN"):
                self.conn.begin()
            elif upper.startswith("COMMIT"):
                self.conn.end()
            elif upper.startswith("SAVE TRAN"):
                self.conn.savepoint_rows[upper.split()[-1]] = list(self.conn.pending)
            elif upper.startswith("ROLLBACK"):
                name = upper.split()[-1]
                if name in self.conn.savepoint_rows:
                    self.conn.pending = list(self.conn.savepoint_rows[name])
                else:
                    self.conn.discard()
            elif upper.startswith("FAIL"):
                raise self.conn.error_type("bad statement")
            elif upper.startswith("INSERT"):
                value = params[0] if params else statement.split("(")[-1].rstrip(") ")
                if value == "bad":
                    raise self.conn.error_type("bad row")
                target = self.conn.pending if self.conn.depth else self.conn.durable
                target.append(int(value))
                self.rowcount = 1
            elif upper.startswith("TRUNCATE"):
                target = self.conn.pending if self.conn.depth else self.conn.durable
                target.clear()
            elif "INFORMATION_SCHEMA.COLUMNS" in upper:
                self.description = [("COLUMN_NAME",), ("DATA_TYPE",), ("IS_NULLABLE",)]
                self.rows = [("id", "int", "NO")]
            elif "FOREIGN_KEYS" in upper:
                self.description = [("count",)]
                self.rows = [(0,)]
            elif upper.startswith("SELECT XACT_STATE"):
                self.description = [("state",), ("depth",)]
                self.rows = [(int(self.conn.depth > 0), self.conn.depth)]
            elif upper.startswith("SELECT"):
                self.description = [("id",)]
                self.rows = [(v,) for v in self.conn.pending or self.conn.durable]

    def executemany(self, sql, params_seq):
        for params in params_seq:
            self.execute(sql, params)

    def fetchall(self):
        return self.rows

    def nextset(self):
        return None

    def close(self):
        pass


class PymssqlTransactionConn(FakeConn):
    """Model pymssql's eager BEGIN and the BEGIN after commit/rollback."""

    error_type = FakePymssqlError

    def __init__(self, autocommit=True):
        super().__init__()
        self.depth = 0
        self.pending = []
        self.durable = []
        self.executed = []
        self.savepoint_rows = {}
        self.autocommit_state = autocommit
        if not autocommit:
            self.begin()

    def cursor(self):
        return TransactionCursor(self)

    def begin(self):
        if self.depth == 0:
            self.pending = list(self.durable)
            self.savepoint_rows.clear()
        self.depth += 1

    def end(self):
        if self.depth == 0:
            raise self.error_type("COMMIT without BEGIN")
        self.depth -= 1
        if self.depth == 0:
            self.durable = list(self.pending)
            self.pending = []
            self.savepoint_rows.clear()

    def discard(self):
        self.depth = 0
        self.pending = []
        self.savepoint_rows.clear()

    def autocommit(self, enabled):
        self.autocommit_calls.append(enabled)
        if enabled == self.autocommit_state:
            return
        if enabled:
            self.discard()
        else:
            self.begin()
        self.autocommit_state = enabled

    def commit(self):
        self.commit_count += 1
        if not self.autocommit_state:
            self.end()
            self.begin()

    def rollback(self):
        self.rollback_count += 1
        if not self.autocommit_state:
            self.discard()
            self.begin()

    def close(self):
        self.discard()
        super().close()


class ImplicitTransactionCursor(TransactionCursor):
    """Adds SET IMPLICIT_TRANSACTIONS: INSERT/BEGIN at depth 0 open a transaction."""

    def execute(self, sql, params=()):
        upper = sql.strip().upper()
        if upper.startswith("SET IMPLICIT_TRANSACTIONS"):
            self.conn.executed.append((sql, params))
            self.conn.implicit = upper.endswith("ON")
            self.description, self.rows = None, []
            return
        if upper == "SELECT @@OPTIONS & 2":
            self.conn.executed.append((sql, params))
            self.description, self.rows = [("options",)], [(2 if self.conn.implicit else 0,)]
            return
        if self.conn.implicit and self.conn.depth == 0 and upper.startswith(("INSERT", "BEGIN")):
            self.conn.begin()
        super().execute(sql, params)


class ImplicitTransactionConn(PymssqlTransactionConn):
    """pymssql's autocommit(False) BEGIN nests inside the implicit transaction."""

    def __init__(self):
        self.implicit = False
        super().__init__()

    def cursor(self):
        return ImplicitTransactionCursor(self)

    def autocommit(self, enabled):
        if not enabled and self.autocommit_state and self.implicit and self.depth == 0:
            self.begin()
        super().autocommit(enabled)


class MidLineControlConn(PymssqlTransactionConn):
    """Runs BEGIN TRAN even mid-line after another statement, as SQL Server does."""

    def cursor(self):
        cursor = TransactionCursor(self)
        execute = cursor.execute
        cursor.execute = lambda sql, params=(): execute(
            re.sub(r"\s+(?=BEGIN\s+TRAN)", ";", sql), params)
        return cursor


class OdbcTransactionConn(PymssqlTransactionConn):
    """ODBC uses a property and starts its implicit transaction on first SQL."""

    error_type = FakePyodbcError

    def __init__(self):
        super().__init__()

    @property
    def autocommit(self):
        return self.autocommit_state

    @autocommit.setter
    def autocommit(self, enabled):
        self.autocommit_calls.append(enabled)
        self.autocommit_state = enabled

    def cursor(self):
        cursor = TransactionCursor(self)
        execute = cursor.execute
        def implicit_execute(sql, params=()):
            if not self.autocommit and self.depth == 0:
                self.begin()
            return execute(sql, params)
        cursor.execute = implicit_execute
        return cursor

    def commit(self):
        self.commit_count += 1
        if not self.autocommit and self.depth:
            while self.depth:
                self.end()

    def rollback(self):
        self.rollback_count += 1
        if not self.autocommit:
            self.discard()


class MysqlTransactionCursor(TransactionCursor):
    def execute(self, sql, params=()):
        upper = sql.strip().upper()
        if upper == "DO 0":
            self.conn.executed.append((sql, params))
            self.conn.refresh_status()
            self.description = None
            return
        if upper == "SET AUTOCOMMIT = 0":
            self.conn.autocommit(False)
            self.description = None
            return
        if upper == "DEADLOCK":
            self.conn.discard()
            # Error packets and SELECT EOF packets leave the cached status stale.
            raise FakePymysqlError(1213, "deadlock")
        if (upper.startswith("SELECT") and "FROM T" in upper
                and not self.conn.autocommit_state and not self.conn.depth):
            self.conn.begin()
        if upper.startswith("INSERT") and not self.conn.autocommit_state and not self.conn.depth:
            self.conn.begin()
        super().execute(sql, params)
        if upper.startswith("SELECT"):
            status = int(self.conn.depth > 0) | (2 if self.conn.autocommit_state else 0)
            self.conn.packets.append(types.SimpleNamespace(
                is_eof_packet=lambda: True,
                get_all_data=lambda: struct.pack("<BHH", 0xfe, 0, status),
            ))
            self.conn._read_packet()
        else:
            self.conn.refresh_status()


class MysqlTransactionConn(PymssqlTransactionConn):
    error_type = FakePymysqlError

    def __init__(self):
        super().__init__()
        self.server_status = 2
        self.packets = []

    def _read_packet(self):
        # The original driver drops EOF status. The connector's packet hook
        # must supply it, without another SQL statement changing ROW_COUNT().
        return self.packets.pop(0)

    @property
    def open(self):
        return not self.closed

    def cursor(self):
        if self.closed:
            raise FakePymysqlError(0, "connection closed")
        return MysqlTransactionCursor(self)

    def refresh_status(self):
        self.server_status = int(self.depth > 0) | (2 if self.autocommit_state else 0)

    def autocommit(self, enabled):
        self.autocommit_calls.append(enabled)
        self.autocommit_state = enabled
        self.refresh_status()

    def get_autocommit(self):
        return bool(self.server_status & 2)

    def commit(self):
        self.commit_count += 1
        if self.depth:
            self.end()
        self.refresh_status()

    def rollback(self):
        self.rollback_count += 1
        self.discard()
        self.refresh_status()

    def thread_id(self):
        return 42


class MssqlRegressionTests(unittest.TestCase):
    def test_bracket_table_supports_three_part_names(self):
        self.assertEqual(
            MSSQL._bracket_table("db.dbo.users"),
            "[db].[dbo].[users]",
        )
        self.assertEqual(
            MSSQL._bracket_table("[db.name].[dbo].users"),
            "[db.name].[dbo].[users]",
        )
        self.assertEqual(
            MSSQL._bracket_table("dbo.weird]name"),
            "[dbo].[weird]]name]",
        )

    def test_qualify_table_parts_parses_database_schema_table(self):
        self.assertEqual(
            MSSQL._qualify_table_parts("[db.name].[audit].[events]"),
            ("db.name", "audit", "events"),
        )
        self.assertEqual(
            MSSQL._qualify_table_parts("users", default_schema="dbo"),
            (None, "dbo", "users"),
        )
        self.assertEqual(
            MSSQL._split_sql_identifier("[dbo].[weird]]name]"),
            ["dbo", "weird]name"],
        )

    def test_quote_column_list_quotes_identifiers(self):
        self.assertEqual(
            MSSQL._quote_column_list("id,weird]name,[already.quoted]"),
            "[id], [weird]]name], [already.quoted]",
        )

    def test_quote_column_list_keeps_commas_inside_quoted_names(self):
        self.assertEqual(MSSQL._quote_column_list("id,[a,b],[x]],y]"), "[id], [a,b], [x]],y]")
        self.assertEqual(MSSQL._quote_column_list("`a,b`,c", "mysql"), "`a,b`, `c`")
        self.assertEqual(MSSQL._split_column_list("[a,b],c"), ["[a,b]", "c"])

    def test_pattern_commands_also_match_name_literally(self):
        # "[" opens a LIKE character class, so "t%b[" alone would never match.
        cursor = FakeCursor([{"description": [("TABLE_SCHEMA",), ("TABLE_NAME",), ("TABLE_TYPE",)],
                              "rows": [("dbo", "t%b[", "BASE TABLE")]}])
        ctx = {"conn": FakeConn(cursor), "output": io.StringIO()}
        MSSQL.run_dot_command(".tables t%b[", ctx)
        sql, params = cursor.executed[0]
        self.assertIn("CHARINDEX(%s, TABLE_NAME) > 0", sql)
        self.assertEqual(params, ("%t%b[%", "t%b["))
        self.assertIn("t%b[", ctx["output"].getvalue())

    def test_execute_sql_commits_once_after_multi_statement_batch(self):
        cursor = FakeCursor(
            [
                {"description": None, "rowcount": 1},
                {"description": None, "rowcount": 2},
            ]
        )
        conn = FakeConn(cursor)
        out = io.StringIO()

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.execute_sql("UPDATE t SET x = 1; DELETE FROM t;", {"conn": conn, "output": out})

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(out.getvalue(), "")
        self.assertIn("(1 row affected)", stderr.getvalue())
        self.assertIn("(2 rows affected)", stderr.getvalue())

    def test_execute_sql_commits_successful_ddl_with_negative_rowcount(self):
        cursor = FakeCursor([{"description": None, "rowcount": -1}])
        conn = FakeConn(cursor)

        MSSQL.execute_sql("CREATE TABLE t (id int)", {"conn": conn, "output": io.StringIO()})

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_commits_create_procedure_with_transaction_body(self):
        cursor = FakeCursor([{"description": None, "rowcount": -1}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        MSSQL.execute_sql(
            "CREATE PROCEDURE dbo.p AS BEGIN TRANSACTION; COMMIT",
            ctx,
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])

    def test_execute_sql_commits_create_or_alter_trigger_with_transaction_body(self):
        cursor = FakeCursor([{"description": None, "rowcount": -1}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        MSSQL.execute_sql(
            "CREATE OR ALTER TRIGGER dbo.tr ON dbo.t AFTER INSERT AS ROLLBACK",
            ctx,
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])

    def test_execute_sql_commits_data_change_statement_that_returns_rows(self):
        cursor = FakeCursor(
            [
                {
                    "description": [("id",)],
                    "rows": [(1,)],
                    "rowcount": 1,
                }
            ]
        )
        conn = FakeConn(cursor)

        MSSQL.execute_sql(
            "INSERT INTO users(name) OUTPUT inserted.id VALUES ('Alice')",
            {"conn": conn, "output": io.StringIO()},
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_commits_cte_data_change_statement_that_returns_rows(self):
        cursor = FakeCursor(
            [
                {
                    "description": [("id",)],
                    "rows": [(1,)],
                    "rowcount": 1,
                }
            ]
        )
        conn = FakeConn(cursor)

        MSSQL.execute_sql(
            "WITH cte AS (SELECT 1 AS id) "
            "UPDATE dbo.users SET name = 'Alice' OUTPUT inserted.id FROM cte",
            {"conn": conn, "output": io.StringIO()},
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_does_not_autocommit_explicit_transaction_batch(self):
        conn = PymssqlTransactionConn()

        MSSQL.execute_sql("BEGIN TRANSACTION", {"conn": conn, "output": io.StringIO()})

        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_tracks_distributed_transaction_begin(self):
        conn = PymssqlTransactionConn()
        ctx = {"conn": conn, "output": io.StringIO()}

        MSSQL.execute_sql("BEGIN DISTRIBUTED TRANSACTION", ctx)

        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_tracks_line_start_begin_after_select(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        MSSQL.execute_sql("SELECT 1\nBEGIN TRANSACTION", ctx)

        self.assertEqual(ctx["transaction_depth"], 2)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_respects_transaction_opened_in_prior_command(self):
        conn = FakeConn(FakeCursor([{"description": None, "rowcount": 1}]))
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        MSSQL.execute_sql("UPDATE t SET x = 1", ctx)

        self.assertEqual(conn.commit_count, 0)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_tracks_nested_transaction_depth(self):
        conn = PymssqlTransactionConn()
        ctx = {"conn": conn, "output": io.StringIO()}

        MSSQL.execute_sql("BEGIN TRANSACTION", ctx)
        MSSQL.execute_sql("BEGIN TRANSACTION", ctx)
        MSSQL.execute_sql("COMMIT", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])
        self.assertEqual(conn.commit_count, 0)

    def test_execute_sql_counts_multiple_transaction_controls_in_one_batch(self):
        conn = PymssqlTransactionConn()
        ctx = {"conn": conn, "output": io.StringIO()}

        MSSQL.execute_sql("BEGIN TRANSACTION; BEGIN TRANSACTION; COMMIT;", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])
        self.assertEqual(conn.commit_count, 0)

    def test_execute_sql_applies_transaction_controls_in_order(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("ROLLBACK; BEGIN TRANSACTION", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])
        self.assertEqual(ctx["savepoints"], set())

    def test_execute_sql_uses_transaction_depth_from_prior_command(self):
        conn = FakeConn(FakeCursor([{"description": None, "rowcount": 1}]))
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        MSSQL.execute_sql("UPDATE t SET x = 1", ctx)

        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_clears_transaction_state_on_error(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("boom")

        cursor = ErrorCursor()
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        ok = MSSQL.execute_sql("UPDATE t SET x = 1", ctx)

        self.assertFalse(ok)
        self.assertEqual(conn.rollback_count, 1)
        self.assertFalse(ctx["transaction_open"])
        self.assertEqual(cursor.close_count, 1)

    def test_execute_sql_returns_true_and_closes_cursor_on_success(self):
        cursor = FakeCursor([{"description": None, "rowcount": 1}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        ok = MSSQL.execute_sql("UPDATE t SET x = 1", ctx)

        self.assertTrue(ok)
        self.assertEqual(cursor.close_count, 1)

    def test_execute_sql_does_not_force_rollback_when_transaction_is_open(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("boom")

        conn = FakeConn(ErrorCursor())
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        MSSQL.execute_sql("UPDATE t SET x = 1", ctx)

        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_blocks_sql_when_connection_dies_inside_transaction(self):
        # With pymssql a --query-timeout kills the connection; the server then
        # rolls back the open transaction, so a later COMMIT can't succeed.
        class DeadCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("DBPROCESS is dead or not enabled")

        conn = FakeConn(DeadCursor())
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            self.assertFalse(MSSQL.execute_sql("WAITFOR DELAY '00:00:05'", ctx))
            self.assertFalse(MSSQL.execute_sql("COMMIT", ctx))
        self.assertFalse(ctx["connection_usable"])
        self.assertIn("rolls back an open transaction", stderr.getvalue())
        self.assertIn("Connection is unusable. Run .reconnect", stderr.getvalue())

    def test_execute_sql_error_on_live_connection_keeps_transaction_usable(self):
        ctx = self._transaction_ctx()
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertTrue(MSSQL.execute_sql("BEGIN TRAN", ctx))
            self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
            self.assertFalse(MSSQL.execute_sql("FAIL", ctx))
            self.assertTrue(ctx.get("connection_usable", True))
            self.assertEqual(ctx["transaction_depth"], 1)
            self.assertTrue(MSSQL.execute_sql("COMMIT", ctx))
        self.assertEqual(ctx["conn"].durable, [1])

    def test_execute_sql_treats_savepoint_rollback_as_transaction_still_open(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        MSSQL.execute_sql("SAVE TRANSACTION sp1; ROLLBACK TRANSACTION sp1;", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_tracks_savepoint_rollback_across_commands(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        MSSQL.execute_sql("SAVE TRANSACTION sp1", ctx)
        MSSQL.execute_sql("ROLLBACK TRANSACTION sp1", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])
        self.assertIn("SP1", ctx["savepoints"])

    def test_execute_sql_treats_named_transaction_rollback_as_full_rollback(self):
        conn = PymssqlTransactionConn()
        ctx = {"conn": conn, "output": io.StringIO()}

        MSSQL.execute_sql("BEGIN TRANSACTION outer_tran; ROLLBACK TRANSACTION outer_tran;", ctx)

        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])

    def test_execute_sql_does_not_treat_string_literal_as_transaction_control(self):
        cursor = FakeCursor([{"description": None, "rowcount": 1}])
        conn = FakeConn(cursor)

        MSSQL.execute_sql(
            "INSERT INTO logs(msg) VALUES('COMMIT complete')",
            {"conn": conn, "output": io.StringIO()},
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_does_not_treat_bracketed_identifier_as_transaction_control(self):
        cursor = FakeCursor([{"description": None, "rowcount": 1}])
        conn = FakeConn(cursor)

        MSSQL.execute_sql(
            "UPDATE [commit] SET value = 1",
            {"conn": conn, "output": io.StringIO()},
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_does_not_treat_double_quoted_identifier_as_transaction_control(self):
        cursor = FakeCursor([{"description": None, "rowcount": 1}])
        conn = FakeConn(cursor)

        MSSQL.execute_sql(
            'UPDATE "commit" SET value = 1',
            {"conn": conn, "output": io.StringIO()},
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_does_not_treat_begin_transaction_alias_as_control(self):
        cursor = FakeCursor(
            [
                {
                    "description": [("BEGIN",)],
                    "rows": [(1,)],
                    "rowcount": -1,
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("SELECT BEGIN TRANSACTION = 1", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_does_not_treat_save_transaction_alias_as_control(self):
        cursor = FakeCursor(
            [
                {
                    "description": [("SAVE",)],
                    "rows": [(1,)],
                    "rowcount": -1,
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("SELECT\nSAVE TRANSACTION = 1", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])
        self.assertEqual(ctx["savepoints"], set())

    def test_execute_sql_does_not_treat_line_start_alias_as_transaction_control(self):
        cursor = FakeCursor(
            [
                {
                    "description": [("COMMIT",)],
                    "rows": [(1,)],
                    "rowcount": -1,
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("SELECT\n  COMMIT = 1", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_transaction_control_flags_match_server_for_mid_line_keywords(self):
        # Ground truth from SQL Server 2019: these words are reserved, so they
        # run as statements wherever they appear; "KEYWORD = 1" aliases are a
        # syntax error, and WITH ROLLBACK is an ALTER DATABASE option.
        cases = {
            "SELECT 1 AS x BEGIN TRAN": [("begin", None)],
            "SELECT 1 AS x COMMIT": [("commit", None)],
            "SELECT 1 commit": [("commit", None)],
            "SELECT * FROM t WHERE 1 = 0 ROLLBACK": [("rollback", None)],
            "SELECT 1 x ROLLBACK TRAN": [("rollback", None)],
            "SELECT 1 AS x SAVE TRAN s": [("save", "S")],
            "SELECT 1 AS [commit] COMMIT": [("commit", None)],
            "UPDATE t SET v = 1 COMMIT": [("commit", None)],
            "BEGIN TRAN COMMIT": [("begin", None), ("commit", None)],
            "SELECT 1 AS a,\nCOMMIT = 1": [],
            "SELECT BEGIN TRANSACTION = 1": [],
            "ALTER DATABASE d SET SINGLE_USER WITH ROLLBACK IMMEDIATE": [],
            "SELECT 'COMMIT' AS s": [],
        }
        for sql, ops in cases.items():
            with self.subTest(sql=sql):
                self.assertEqual(MSSQL._transaction_control_flags(sql)["ops"], ops)

    def test_execute_sql_mid_line_begin_tran_keeps_later_work_uncommitted(self):
        ctx = self._transaction_ctx(conn=MidLineControlConn())
        conn = ctx["conn"]
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertTrue(MSSQL.execute_sql("SELECT 1 AS x BEGIN TRAN", ctx))
            self.assertEqual(ctx["transaction_depth"], 1)
            self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
            self.assertEqual(conn.durable, [])
            self.assertTrue(MSSQL.execute_sql("ROLLBACK", ctx))
        self.assertEqual(conn.durable, [])
        self.assertEqual(ctx["transaction_depth"], 0)

    def test_requires_autocommit_for_statements_sql_server_rejects_in_transactions(self):
        for sql in (
            "CREATE DATABASE x", "IF DB_ID('x') IS NULL CREATE DATABASE x", "DROP DATABASE x",
            "ALTER DATABASE x SET RECOVERY SIMPLE", "ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0",
            "BACKUP DATABASE x TO DISK = 'f'", "RESTORE DATABASE x FROM DISK = 'f'", "RECONFIGURE",
            "CREATE FULLTEXT CATALOG c", "DBCC SHRINKFILE (1)", "SELECT @@TRANCOUNT", "SELECT XACT_STATE()",
            "SET IMPLICIT_TRANSACTIONS ON", "SET ANSI_DEFAULTS ON",
        ):
            with self.subTest(sql=sql):
                self.assertTrue(MSSQL._requires_autocommit(sql))
        for sql in ("SELECT 1", "INSERT INTO t VALUES (1)", "SELECT 'CREATE DATABASE x'",
                    "SELECT [backup] FROM t", "-- RECONFIGURE\nSELECT 1", "DBCC CHECKDB"):
            with self.subTest(sql=sql):
                self.assertFalse(MSSQL._requires_autocommit(sql))

    def test_execute_sql_runs_database_ddl_without_owned_transaction(self):
        for sql in ("CREATE DATABASE x", "SELECT @@TRANCOUNT"):
            with self.subTest(sql=sql):
                ctx = self._transaction_ctx()
                with mock.patch("sys.stderr", new=io.StringIO()):
                    self.assertTrue(MSSQL.execute_sql(sql, ctx))
                self.assertEqual(ctx["conn"].autocommit_calls, [])

    def test_execute_sql_refuses_raw_nul_on_pymssql(self):
        # pymssql truncates SQL text at NUL: this would run "DELETE FROM t".
        ctx = self._transaction_ctx()
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            self.assertFalse(MSSQL.execute_sql("DELETE FROM t\x00 WHERE id = 1", ctx))
        self.assertEqual(ctx["conn"].executed, [])
        self.assertIn("NUL", stderr.getvalue())

    def test_pymssql_message_handler_prints_info_messages_only(self):
        handlers = []
        conn = types.SimpleNamespace(_conn=types.SimpleNamespace(set_msghandler=handlers.append))
        MSSQL._install_pymssql_message_handler(conn)
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            handlers[0](1, 0, b"srv", b"", 1, "progress é😀".encode("utf-8"))
            handlers[0](1, 16, b"srv", b"", 1, b"an error, reported via the exception")
        self.assertEqual(stderr.getvalue(), "progress é😀\n")
        MSSQL._install_pymssql_message_handler(types.SimpleNamespace())  # no _conn: no-op

    def test_execute_sql_prints_pyodbc_messages_for_each_result_set(self):
        class MessageCursor(FakeCursor):
            per_set = [[("[01000] (0)", "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]before")],
                       [("[01000] (0)", "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]after")]]

            @property
            def messages(self):
                return self.per_set[min(self.index, len(self.per_set) - 1)]

        cursor = MessageCursor([{"description": [("a",)], "rows": [(1,)]}, {"description": None, "rowcount": -1}])
        ctx = {"conn": OdbcTransactionConn(), "driver_name": "pyodbc", "output": io.StringIO(),
               "transaction_depth": 1}
        ctx["conn"].cursor = lambda: cursor
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.execute_sql("SELECT 1 AS a; PRINT 'after'", ctx)
        self.assertEqual([l for l in stderr.getvalue().splitlines() if l in ("before", "after")],
                         ["before", "after"])
        self.assertNotIn("[Microsoft]", stderr.getvalue())

    def test_old_pymssql_warns_about_temporal_corruption(self):
        for version, warns in (("2.2.11", True), ("2.3.13", True), ("2.4.4", False), ("test", False)):
            with self.subTest(version=version), mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL._warn_old_pymssql(version)
            self.assertEqual("datetimeoffset" in stderr.getvalue(), warns)

    def test_db_error_includes_pymssql_low_level_exception(self):
        low_level = type("MSSQLException", (Exception,), {})
        module = load_mssql_module(
            "mssql_low_level_under_test", _mssql=types.SimpleNamespace(MSSQLException=low_level))
        self.assertIn(low_level, module.DBError)
        self.assertNotIn(None, MSSQL.DBError)

        class LowLevelRollbackConn(FakeConn):
            def rollback(self):
                raise low_level("pending results")

        ctx = {"conn": LowLevelRollbackConn(), "driver_name": "pymssql"}
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            self.assertFalse(module._restore_autocommit(ctx["conn"], ctx, "test"))
        self.assertFalse(ctx["connection_usable"])
        self.assertIn("pending results", stderr.getvalue())

    def test_execute_sql_tracks_same_line_if_commit(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("IF @@TRANCOUNT > 0 COMMIT", ctx)

        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])

    def test_execute_sql_tracks_same_line_if_rollback(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("IF @@TRANCOUNT > 0 ROLLBACK", ctx)

        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])

    def test_execute_sql_tracks_if_exists_commit(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("IF EXISTS (SELECT 1) COMMIT", ctx)

        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])

    def test_execute_sql_tracks_if_exists_rollback(self):
        conn = PymssqlTransactionConn()
        conn.begin()
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("IF EXISTS (SELECT 1) ROLLBACK", ctx)

        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])

    def test_execute_sql_does_not_treat_if_select_commit_alias_as_control(self):
        cursor = FakeCursor(
            [
                {
                    "description": [("COMMIT",)],
                    "rows": [(1,)],
                    "rowcount": -1,
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("IF 1 = 1 SELECT COMMIT = 1", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_does_not_treat_if_select_rollback_alias_as_control(self):
        cursor = FakeCursor(
            [
                {
                    "description": [("ROLLBACK",)],
                    "rows": [(1,)],
                    "rowcount": -1,
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        MSSQL.execute_sql("IF 1 = 1 SELECT ROLLBACK = 1", ctx)

        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    def test_run_dot_command_rejects_ambiguous_schema_lookup(self):
        rows = [
            ("audit", "users", "id", "int", None, "NO", None, 1),
            ("dbo", "users", "id", "int", None, "NO", None, 1),
        ]
        headers = [
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "IS_NULLABLE",
            "COLUMN_DEFAULT",
            "ORDINAL_POSITION",
        ]
        cursor = FakeCursor([{"description": [(h,) for h in headers], "rows": rows}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with mock.patch.object(MSSQL, "print_results") as print_results:
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                handled = MSSQL.run_dot_command(".schema users", ctx)

        self.assertTrue(handled)
        print_results.assert_not_called()
        self.assertIn("ambiguous across schemas", stderr.getvalue())

    def test_run_dot_command_exact_schema_lookup_strips_prefix_columns(self):
        rows = [
            ("dbo", "users", "id", "int", None, "NO", None, 1),
            ("dbo", "users", "name", "nvarchar", 100, "YES", None, 2),
        ]
        headers = [
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "IS_NULLABLE",
            "COLUMN_DEFAULT",
            "ORDINAL_POSITION",
        ]
        cursor = FakeCursor([{"description": [(h,) for h in headers], "rows": rows}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with mock.patch.object(MSSQL, "print_results") as print_results:
            handled = MSSQL.run_dot_command(".schema dbo.users", ctx)

        self.assertTrue(handled)
        print_results.assert_called_once()
        args = print_results.call_args.args
        self.assertEqual(
            args[0],
            [
                ("id", "int", None, "NO", None, 1),
                ("name", "nvarchar", 100, "YES", None, 2),
            ],
        )
        self.assertEqual(
            args[1],
            [
                "COLUMN_NAME",
                "DATA_TYPE",
                "CHARACTER_MAXIMUM_LENGTH",
                "IS_NULLABLE",
                "COLUMN_DEFAULT",
                "ORDINAL_POSITION",
            ],
        )

    def test_dot_command_usage_and_not_found_errors_go_to_stderr(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".export", ctx)
        self.assertIn("Usage: .export TABLE FILE", stderr.getvalue())

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".import", ctx)
        self.assertIn("Usage: .import FILE TABLE", stderr.getvalue())

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".schema", ctx)
        self.assertIn("Usage: .schema TABLE", stderr.getvalue())

        cursor = FakeCursor([{"description": [("TABLE_SCHEMA",), ("TABLE_NAME",)], "rows": []}])
        not_found_ctx = {"conn": FakeConn(cursor), "output": io.StringIO()}
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".schema missing_table", not_found_ctx)
        self.assertIn("Table 'missing_table' not found.", stderr.getvalue())

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".read", ctx)
        self.assertIn("Usage: .read FILE", stderr.getvalue())

    def test_read_dot_command_accepts_quoted_file_path(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with mock.patch.object(MSSQL, "execute_file") as execute_file:
            handled = MSSQL.run_dot_command('.read "dir/query file.sql"', ctx)

        self.assertTrue(handled)
        execute_file.assert_called_once_with("dir/query file.sql", ctx)

    def test_execute_file_reports_decode_errors_cleanly(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            sql_path = Path(tmpdir) / "bad.sql"
            sql_path.write_bytes(b"\xff")
            with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    ok = MSSQL.execute_file(sql_path, ctx)

        self.assertFalse(ok)
        execute_sql.assert_not_called()
        self.assertIn("Error reading", stderr.getvalue())
        self.assertIn("invalid UTF-8", stderr.getvalue())

    def test_execute_file_expands_user_script_path(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            sql_path = Path(tmpdir) / "query.sql"
            sql_path.write_text("SELECT 1", encoding="utf-8")
            with mock.patch.dict(os.environ, {"HOME": tmpdir}, clear=False):
                with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                    ok = MSSQL.execute_file("~/query.sql", ctx)

        self.assertTrue(ok)
        execute_sql.assert_called_once_with("SELECT 1", ctx)

    def test_execute_file_returns_false_when_any_batch_fails(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            sql_path = Path(tmpdir) / "query.sql"
            sql_path.write_text("SELECT 1\nGO\nSELECT 2", encoding="utf-8")
            with mock.patch.object(MSSQL, "execute_sql", side_effect=[True, False]) as execute_sql:
                ok = MSSQL.execute_file(sql_path, ctx)

        self.assertFalse(ok)
        self.assertEqual(execute_sql.call_count, 2)

    def test_dot_command_status_messages_go_to_stderr(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO(), "mode": "table", "headers": True}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".mode csv", ctx)
        self.assertIn("Output mode: csv", stderr.getvalue())

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".headers off", ctx)
        self.assertIn("Headers: off", stderr.getvalue())

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "redir.txt"
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.run_dot_command(f".output {quote_dot_arg(out_path)}", ctx)
            self.assertIn(f"Output: {out_path}", stderr.getvalue())
            if ctx["output"] is not sys.stdout:
                ctx["output"].close()

    def test_main_closes_final_output_file_after_interactive_output_redirect(self):
        conn = FakeConn()

        class FakeInteractiveFile(io.StringIO):
            def __init__(self):
                super().__init__()
                self.was_closed = False

            def close(self):
                self.was_closed = True
                super().close()

        redirected = FakeInteractiveFile()

        def fake_interactive_loop(ctx):
            ctx["output"] = redirected

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch.object(MSSQL, "interactive_loop", side_effect=fake_interactive_loop):
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa"]):
                    with mock.patch("sys.stdin.isatty", return_value=True):
                        MSSQL.main()

        self.assertTrue(conn.closed)
        self.assertTrue(redirected.was_closed)

    def test_import_uses_database_qualified_metadata_lookup_and_insert(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id,name\n1,Alice\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} otherdb.dbo.users", ctx)

        self.assertIn("[otherdb].INFORMATION_SCHEMA.COLUMNS", cursor.executed[0][0])
        self.assertIn("INSERT INTO [otherdb].[dbo].[users]", cursor.executed[1][0])

    def test_import_escapes_database_table_and_column_identifiers(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("weird]col", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("weird]col\n1\n", encoding="utf-8")
            MSSQL.import_data(
                f"{quote_dot_arg(csv_path)} [odd]]db].[strange]]schema].[bad]]table]",
                ctx,
            )

        self.assertIn("[odd]]db].INFORMATION_SCHEMA.COLUMNS", cursor.executed[0][0])
        self.assertIn(
            "INSERT INTO [odd]]db].[strange]]schema].[bad]]table] ([weird]]col]) VALUES (%s)",
            cursor.executed[1][0],
        )

    def test_import_rejects_invalid_batch_and_delimiter_before_metadata_query(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --batch 0", ctx)
            self.assertIn("--batch must be greater than 0", stderr.getvalue())

            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --delimiter ::", ctx)
            self.assertIn("--delimiter must be exactly one character", stderr.getvalue())

        self.assertEqual(ctx["conn"].cursor().executed, [])

    def test_import_does_not_commit_when_transaction_is_open(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertEqual(conn.commit_count, 0)
        self.assertIn(
            ("INSERT INTO [dbo].[users] ([id]) VALUES (%s)", [("1",)]),
            cursor.executed,
        )

    def test_import_truncate_happens_after_input_validation(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            bad_path = Path(tmpdir) / "missing.json"
            MSSQL.import_data(f"{quote_dot_arg(bad_path)} dbo.users --truncate", ctx)

        self.assertEqual(len(cursor.executed), 1)
        self.assertEqual(conn.commit_count, 0)

    def test_import_truncate_executes_for_header_only_csv(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        out = io.StringIO()
        ctx = {"conn": conn, "output": out}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --truncate", ctx)

        executed_sql = [sql for sql, _ in cursor.executed]
        self.assertIn("TRUNCATE TABLE [dbo].[users]", executed_sql)
        self.assertEqual(conn.commit_count, 1)
        self.assertIn("Truncated [dbo].[users]", stderr.getvalue())
        self.assertEqual(out.getvalue(), "")

    def test_import_truncate_abort_does_not_issue_truncate_before_failed_batch(self):
        class ImportCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))
                raise MSSQL.pymssql.Error("bad row")

        cursor = ImportCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --truncate", ctx)

        executed_sql = [sql for sql, _ in cursor.executed]
        self.assertIn("TRUNCATE TABLE [dbo].[users]", executed_sql)
        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(conn.rollback_count, 1)

    def test_import_truncate_skip_truncates_before_row_retry(self):
        class ImportCursor(FakeCursor):
            def __init__(self, states=None):
                super().__init__(states)
                self._executemany_calls = 0

            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))
                self._executemany_calls += 1
                raise MSSQL.pymssql.Error("batch fail")

        cursor = ImportCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n2\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --truncate --on-error skip", ctx)

        executed_sql = [sql for sql, _ in cursor.executed]
        truncate_idx = executed_sql.index("TRUNCATE TABLE [dbo].[users]")
        first_insert_idx = executed_sql.index("INSERT INTO [dbo].[users] ([id]) VALUES (%s)")
        self.assertLess(truncate_idx, first_insert_idx)

    def test_import_truncate_skip_all_invalid_rows_still_truncates(self):
        # Every data row has more fields than the CSV header, so --on-error
        # skip discards all of them via per-row validation (never reaching
        # _flush_batch). --truncate must still run: row_num > 0 here, unlike
        # the row_num == 0 (empty-source) case already covered above.
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1,extra\n2,extra\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --truncate --on-error skip", ctx)

        executed_sql = [sql for sql, _ in cursor.executed]
        self.assertIn("TRUNCATE TABLE [dbo].[users]", executed_sql)
        self.assertEqual(conn.commit_count, 1)
        self.assertIn("Truncated [dbo].[users]", stderr.getvalue())
        self.assertIn("2 row(s) skipped due to errors", stderr.getvalue())

    def test_import_truncate_skip_reports_retry_truncate_failure_cleanly(self):
        class ImportCursor(FakeCursor):
            def execute(self, sql, params=()):
                self.executed.append((sql, params))
                if sql.startswith("TRUNCATE TABLE"):
                    raise MSSQL.pymssql.Error("truncate boom")

            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))
                raise MSSQL.pymssql.Error("batch fail")

        cursor = ImportCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --truncate --on-error skip", ctx)

        self.assertIn("Aborted: Truncate failed: truncate boom", stderr.getvalue())
        self.assertEqual(conn.rollback_count, 2)

    def test_import_skip_rolls_back_failed_single_row_retry(self):
        class ImportCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))
                raise MSSQL.pymssql.Error("batch fail")

            def execute(self, sql, params=()):
                self.executed.append((sql, params))
                if sql.startswith("INSERT") and params == ("bad",):
                    raise MSSQL.pymssql.Error("row fail")

        cursor = ImportCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\nbad\n2\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --on-error skip", ctx)

        self.assertEqual(conn.rollback_count, 2)
        self.assertIn("Skipped row 1: row fail", stderr.getvalue())
        self.assertIn(("INSERT INTO [dbo].[users] ([id]) VALUES (%s)", ("2",)), cursor.executed)

    # ── error-io-paths: cleanup/rollback on interrupt and reopen failure ────

    def test_import_reports_clean_error_when_cursor_reopen_fails_after_batch_error(self):
        # After a batch error with --on-error skip, import_data recreates the
        # cursor to retry row-by-row. If that reopen itself fails (e.g. the
        # connection dropped), it must surface as a clean "Aborted: ..."
        # message via RuntimeError, not an unhandled pymssql.Error escaping
        # past import_data's own exception handling.
        class BatchErrorCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                raise MSSQL.pymssql.Error("batch boom")

        class FlakyConn(FakeConn):
            def __init__(self, cursor):
                super().__init__(cursor)
                self.cursor_calls = 0

            def cursor(self):
                self.cursor_calls += 1
                if self.cursor_calls == 1:
                    return self._cursor
                raise MSSQL.pymssql.Error("connection dropped")

        cursor = BatchErrorCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FlakyConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id\n1\n2\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --batch 10 --on-error skip", ctx)

        self.assertIn(
            "Aborted: Could not reopen cursor after batch error", stderr.getvalue()
        )

    def test_import_keyboard_interrupt_mid_batch_rolls_back(self):
        # A batch's executemany/commit is not wrapped in autocommit (pymssql
        # runs with autocommit off), so an interrupt between the two can
        # leave the batch inserted-but-uncommitted on the connection unless
        # it's explicitly rolled back before the interrupt propagates.
        class InterruptingCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                raise KeyboardInterrupt()

        cursor = InterruptingCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                with self.assertRaises(KeyboardInterrupt):
                    MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertEqual(conn.rollback_count, 1)
        self.assertFalse(ctx["transaction_open"])

    def test_import_keyboard_interrupt_mid_row_retry_rolls_back(self):
        # After a whole-batch executemany fails, --on-error skip retries the
        # batch row-by-row. Each row's execute()/commit() pair has the same
        # autocommit-off hazard as the whole-batch attempt above, so an
        # interrupt partway through the retry loop must roll back too instead
        # of leaving that row's INSERT dangling on the connection.
        class RetryInterruptCursor(FakeCursor):
            def __init__(self, states):
                super().__init__(states)
                self.insert_calls = 0

            def executemany(self, sql, params_seq):
                raise MSSQL.pymssql.Error("batch boom")

            def execute(self, sql, params=()):
                super().execute(sql, params)
                if sql.strip().upper().startswith("INSERT"):
                    self.insert_calls += 1
                    if self.insert_calls == 2:
                        raise KeyboardInterrupt()

        cursor = RetryInterruptCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id\n1\n2\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                with self.assertRaises(KeyboardInterrupt):
                    MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --batch 2 --on-error skip", ctx)

        # rollback #1: the failed whole-batch executemany.
        # rollback #2: the interrupted row (id=2) mid-retry.
        self.assertEqual(conn.rollback_count, 2)
        self.assertEqual(conn.commit_count, 1)  # only row id=1 committed
        self.assertFalse(ctx["transaction_open"])
        self.assertEqual(ctx["transaction_depth"], 0)

    def test_import_json_columns_remap_by_key(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "users.json"
            json_path.write_text('[{"name":"Alice","id":1}]', encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users --columns id,name", ctx)

        self.assertEqual(cursor.executed[1][1], [(1, "Alice")])

    def test_import_json_rejects_missing_required_keys(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "users.json"
            json_path.write_text('[{"id":1,"name":"Alice"},{"id":2}]', encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users --format json", ctx)

        self.assertEqual(len(cursor.executed), 1)
        self.assertIn("missing key(s) in JSON record", stderr.getvalue())

    def test_import_csv_columns_remap_by_header_name(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("name,id\nAlice,1\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --columns id,name", ctx)

        self.assertEqual(
            cursor.executed[1],
            ("INSERT INTO [dbo].[users] ([id], [name]) VALUES (%s, %s)", [("1", "Alice")]),
        )

    def test_import_csv_columns_accepts_subset_of_wider_header(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("name,id,ignored\nAlice,1,x\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --columns id,name", ctx)

        self.assertEqual(cursor.executed[1][1], [("1", "Alice")])

    def test_import_csv_columns_reports_missing_header_column(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --columns id,name", ctx)

        self.assertEqual(len(cursor.executed), 1)
        self.assertEqual(conn.commit_count, 0)
        self.assertIn("column(s) not found in CSV header", stderr.getvalue())

    def test_import_csv_rejects_extra_fields_beyond_header(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id,name\n1,Alice,extra\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertEqual(len(cursor.executed), 1)
        self.assertEqual(conn.commit_count, 0)
        self.assertIn("expected 2 columns from CSV header, got 3", stderr.getvalue())

    def test_import_csv_rejects_short_rows_against_header(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id,name\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertEqual(len(cursor.executed), 1)
        self.assertEqual(conn.commit_count, 0)
        self.assertIn("expected 2 columns from CSV header, got 1", stderr.getvalue())

    def test_import_csv_rejects_duplicate_header_columns(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id,name,id\n1,Alice,2\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertEqual(len(cursor.executed), 1)
        self.assertEqual(conn.commit_count, 0)
        self.assertIn("CSV header contains duplicate column(s)", stderr.getvalue())

    def test_import_reports_input_validation_errors_as_read_errors(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "empty.csv"
            csv_path.write_text("", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertIn("Error reading", stderr.getvalue())
        self.assertIn("CSV file is empty", stderr.getvalue())
        self.assertNotIn("Error opening", stderr.getvalue())

    def test_import_closes_streaming_input_on_validation_error(self):
        class ClosableRows:
            def __init__(self):
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                raise AssertionError("validation should fail before reading rows")

            def close(self):
                self.closed = True

        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}
        rows = ClosableRows()

        with mock.patch.object(MSSQL, "read_csv_data", return_value=(rows, ["name"])):
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data("rows.csv dbo.users --columns id --format csv", ctx)

        self.assertTrue(rows.closed)
        self.assertIn("column(s) not found in CSV header", stderr.getvalue())
        self.assertEqual(len(cursor.executed), 1)

    def test_import_metadata_query_error_is_reported_cleanly(self):
        class MetadataErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                self.executed.append((sql, params))
                raise MSSQL.pymssql.Error("metadata boom")

        cursor = MetadataErrorCursor()
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertIn("Error reading table metadata: metadata boom", stderr.getvalue())

    def test_import_jsonl_format_override_ignores_file_suffix(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "users.data"
            json_path.write_text('{"id":1,"name":"Alice"}\n', encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users --format jsonl", ctx)

        self.assertEqual(cursor.executed[1][1], [(1, "Alice")])

    def test_import_jsonl_skip_uses_first_imported_row_for_columns(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "users.jsonl"
            jsonl_path.write_text('{"bad":0}\n{"id":1}\n', encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(jsonl_path)} dbo.users --format jsonl --skip 1", ctx)

        self.assertEqual(cursor.executed[1][1], [(1,)])
        self.assertEqual(conn.commit_count, 1)

    def test_import_json_skip_uses_first_imported_row_for_columns(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "users.json"
            json_path.write_text('[{"bad":0},{"id":1}]', encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users --format json --skip 1", ctx)

        self.assertEqual(cursor.executed[1][1], [(1,)])
        self.assertEqual(conn.commit_count, 1)

    def test_import_jsonl_skip_all_rows_still_truncates(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "users.jsonl"
            jsonl_path.write_text('{"id":1}\n', encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(jsonl_path)} dbo.users --format jsonl --skip 1 --truncate", ctx)

        self.assertIn("TRUNCATE TABLE [dbo].[users]", cursor.executed[1][0])
        self.assertEqual(conn.commit_count, 1)

    def test_import_json_skip_all_rows_still_truncates(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "users.json"
            json_path.write_text('[{"id":1}]', encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users --format json --skip 1 --truncate", ctx)

        self.assertIn("TRUNCATE TABLE [dbo].[users]", cursor.executed[1][0])
        self.assertEqual(conn.commit_count, 1)

    def test_import_jsonl_reports_invalid_record_cleanly(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "users.jsonl"
            jsonl_path.write_text('{"id":1,"name":"Alice"}\n{"id":\n', encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(jsonl_path)} dbo.users --format jsonl", ctx)

        self.assertEqual(conn.rollback_count, 1)
        self.assertIn("invalid JSON", stderr.getvalue())

    def test_import_jsonl_reports_invalid_first_record_as_read_error(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "users.jsonl"
            jsonl_path.write_text('{"id":\n', encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(jsonl_path)} dbo.users --format jsonl", ctx)

        self.assertEqual(len(cursor.executed), 1)
        self.assertIn("Error reading", stderr.getvalue())
        self.assertIn("invalid JSON", stderr.getvalue())
        self.assertNotIn("Error opening", stderr.getvalue())

    def test_import_json_format_override_ignores_jsonl_suffix(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                        ("name", "nvarchar", "YES"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "users.jsonl"
            json_path.write_text('[{"id":1,"name":"Alice"}]', encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users --format json", ctx)

        self.assertEqual(cursor.executed[1][1], [(1, "Alice")])

    def test_run_dot_command_reports_query_errors_cleanly(self):
        class QueryErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("dot boom")

        cursor = QueryErrorCursor()
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with mock.patch.object(MSSQL, "print_results") as print_results:
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                handled = MSSQL.run_dot_command(".databases", ctx)

        self.assertTrue(handled)
        print_results.assert_not_called()
        self.assertIn("Query error: dot boom", stderr.getvalue())
        self.assertEqual(cursor.close_count, 1)

    def test_export_csv_append_skips_duplicate_header_on_nonempty_file(self):
        class ExportCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("id",), ("name",)], "rows": []}])
                self.fetchmany_calls = 0

            def fetchmany(self, size):
                self.fetchmany_calls += 1
                if self.fetchmany_calls == 1:
                    return [(1, "Alice")]
                return []

        conn = FakeConn(ExportCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            csv_path.write_text("id,name\n0,Bob\n", encoding="utf-8")
            MSSQL.export_data(f"dbo.users {quote_dot_arg(csv_path)} --append --format csv", ctx)
            self.assertEqual(csv_path.read_text(encoding="utf-8"), "id,name\n0,Bob\n1,Alice\n")

    def test_format_csv_preserves_trailing_spaces_in_last_field(self):
        self.assertEqual(MSSQL.format_csv([("Alice ",)], ["name"]), "name\r\nAlice ")

    def test_format_csv_hex_encodes_bytes(self):
        # Bytes must not fall back to Python's repr() (e.g. "b'\x01\x02'"),
        # which is not valid CSV data and cannot be re-imported.
        self.assertEqual(
            MSSQL.format_csv([(b"\x01\x02",)], ["data"]),
            "data\r\n0x0102",
        )

    def test_format_json_hex_encodes_bytes_and_maps_nonfinite_floats_to_null(self):
        result = MSSQL.json.loads(
            MSSQL.format_json(
                [(b"\x01\x02", float("nan"), float("inf"))],
                ["data", "n", "i"],
            )
        )
        self.assertEqual(result, [{"data": "0x0102", "n": None, "i": None}])

    def test_export_rejects_invalid_chunk_and_delimiter_before_query(self):
        conn = FakeConn()
        ctx = {"conn": conn}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.export_data("dbo.users out.csv --chunk 0 --format csv", ctx)
        self.assertIn("--chunk must be greater than 0", stderr.getvalue())

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.export_data("dbo.users out.csv --delimiter :: --format csv", ctx)
        self.assertIn("--delimiter must be exactly one character", stderr.getvalue())

        self.assertEqual(conn.cursor().executed, [])

    def test_export_opens_output_before_executing_query(self):
        cursor = FakeCursor([{"description": [("id",)], "rows": [(1,)]}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn}

        with mock.patch("builtins.open", side_effect=OSError("nope")):
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data("dbo.users /bad/path.csv --format csv", ctx)

        self.assertEqual(cursor.executed, [])
        self.assertIn("Error opening '/bad/path.csv':", stderr.getvalue())

    def test_export_rejects_dash_table_without_query(self):
        conn = FakeConn()
        ctx = {"conn": conn}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.export_data("- out.csv --format csv", ctx)

        self.assertIn("use - only with --query SQL", stderr.getvalue())

    def test_export_table_mode_quotes_columns(self):
        cursor = FakeCursor([{"description": [("id",), ("weird]name",)], "rows": []}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            MSSQL.export_data(
                f"dbo.users {quote_dot_arg(csv_path)} --format csv "
                "--columns id,weird]name --where \"id > 0\" --order-by id",
                ctx,
            )

        self.assertEqual(
            cursor.executed[0][0],
            "SELECT [id], [weird]]name] FROM [dbo].[users] WHERE id > 0 ORDER BY id",
        )

    def test_export_rejects_stacked_table_fragments_before_query(self):
        cursor = FakeCursor([{"description": [("id",)], "rows": []}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data(
                    f"dbo.users {quote_dot_arg(csv_path)} --format csv --where \"1=1; DROP TABLE users\"",
                    ctx,
                )

            self.assertFalse(csv_path.exists())

        self.assertEqual(cursor.executed, [])
        self.assertIn("--where must not contain statement separators", stderr.getvalue())

    def test_export_rejects_non_select_and_stacked_query_before_execution(self):
        cursor = FakeCursor([{"description": [("id",)], "rows": []}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data(
                    f"- {quote_dot_arg(csv_path)} --format csv --query \"DELETE FROM dbo.users\"",
                    ctx,
                )
            self.assertFalse(csv_path.exists())
            self.assertIn("--query must be a read-only SELECT statement", stderr.getvalue())

            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data(
                    f"- {quote_dot_arg(csv_path)} --format csv --query \"SELECT * FROM dbo.users; DROP TABLE dbo.users\"",
                    ctx,
                )
            self.assertFalse(csv_path.exists())
            self.assertIn("--query must contain a single SELECT statement", stderr.getvalue())

        self.assertEqual(cursor.executed, [])

    def test_export_rejects_select_into_query_before_execution(self):
        cursor = FakeCursor([{"description": [("id",)], "rows": []}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data(
                    f"- {quote_dot_arg(csv_path)} --format csv --query "
                    "\"WITH c AS (SELECT id FROM dbo.users) SELECT id INTO dbo.copy FROM c\"",
                    ctx,
                )

            self.assertFalse(csv_path.exists())

        self.assertEqual(cursor.executed, [])
        self.assertIn("--query must be a read-only SELECT statement", stderr.getvalue())

    def test_export_json_failure_truncates_partial_file(self):
        class ExportCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("id",)], "rows": []}])
                self.fetchmany_calls = 0

            def fetchmany(self, size):
                self.fetchmany_calls += 1
                if self.fetchmany_calls == 1:
                    return [(1,)]
                raise MSSQL.pymssql.Error("stream boom")

        conn = FakeConn(ExportCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "out.json"
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data(f"dbo.users {quote_dot_arg(json_path)} --format json", ctx)

            self.assertFalse(json_path.exists())
            self.assertIn("Export error after 1 rows: stream boom", stderr.getvalue())

    def test_export_csv_hex_encodes_bytes_column(self):
        class ExportCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("data",)], "rows": []}])
                self.fetchmany_calls = 0

            def fetchmany(self, size):
                self.fetchmany_calls += 1
                if self.fetchmany_calls == 1:
                    return [(b"\x01\x02",)]
                return []

        conn = FakeConn(ExportCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            MSSQL.export_data(f"dbo.users {quote_dot_arg(csv_path)} --format csv", ctx)
            self.assertEqual(csv_path.read_text(encoding="utf-8"), "data\n0x0102\n")

    def test_export_json_hex_encodes_bytes_and_maps_nonfinite_floats_to_null(self):
        class ExportCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("data",), ("n",)], "rows": []}])
                self.fetchmany_calls = 0

            def fetchmany(self, size):
                self.fetchmany_calls += 1
                if self.fetchmany_calls == 1:
                    return [(b"\x01\x02", float("nan"))]
                return []

        conn = FakeConn(ExportCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "out.json"
            MSSQL.export_data(f"dbo.users {quote_dot_arg(json_path)} --format json", ctx)
            rows = MSSQL.json.loads(json_path.read_text(encoding="utf-8"))

        self.assertEqual(rows, [{"data": "0x0102", "n": None}])

    def test_export_jsonl_hex_encodes_bytes_and_maps_nonfinite_floats_to_null(self):
        class ExportCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("data",), ("n",)], "rows": []}])
                self.fetchmany_calls = 0

            def fetchmany(self, size):
                self.fetchmany_calls += 1
                if self.fetchmany_calls == 1:
                    return [(b"\x01\x02", float("inf"))]
                return []

        conn = FakeConn(ExportCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "out.jsonl"
            MSSQL.export_data(f"dbo.users {quote_dot_arg(jsonl_path)} --format jsonl", ctx)
            lines = jsonl_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 1)
        self.assertEqual(MSSQL.json.loads(lines[0]), {"data": "0x0102", "n": None})

    def test_export_overwrite_query_error_preserves_existing_file(self):
        class QueryErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                self.executed.append((sql, params))
                raise MSSQL.pymssql.Error("query boom")

        conn = FakeConn(QueryErrorCursor())
        cursor = conn.cursor()
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            csv_path.write_text("old\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data(f"dbo.users {quote_dot_arg(csv_path)} --format csv", ctx)

            self.assertEqual(csv_path.read_text(encoding="utf-8"), "old\n")
            self.assertIn("Query error: query boom", stderr.getvalue())
            self.assertEqual(cursor.close_count, 1)

    # ── error-io-paths: cleanup on interrupt (not just pymssql.Error) ───────

    def test_export_keyboard_interrupt_during_query_cleans_up_temp_file(self):
        # Interrupted while the query is still running server-side, before
        # any row has been fetched. The overwrite-mode temp file (already
        # opened before the query runs) must not be left orphaned on disk.
        class InterruptCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise KeyboardInterrupt()

        conn = FakeConn(InterruptCursor())
        cursor = conn.cursor()
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            with self.assertRaises(KeyboardInterrupt):
                MSSQL.export_data(f"dbo.users {quote_dot_arg(csv_path)} --format csv", ctx)

            self.assertEqual(list(Path(tmpdir).iterdir()), [])
        self.assertEqual(cursor.close_count, 1)

    def test_export_keyboard_interrupt_during_query_preserves_append_target(self):
        class InterruptCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise KeyboardInterrupt()

        conn = FakeConn(InterruptCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with self.assertRaises(KeyboardInterrupt):
                MSSQL.export_data(f"dbo.users {quote_dot_arg(csv_path)} --format csv --append", ctx)

            self.assertEqual(csv_path.read_text(encoding="utf-8"), "id\n1\n")

    def test_export_append_keyboard_interrupt_mid_stream_truncates_partial_write(self):
        # A --append export interrupted mid-stream must not leave a partial
        # record appended to the existing file (only pymssql.Error/OSError
        # were rolled back here before; KeyboardInterrupt was not).
        class InterruptingCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("id",)], "rows": []}])
                self.calls = 0

            def fetchmany(self, size):
                self.calls += 1
                if self.calls == 1:
                    return [(1,)]
                raise KeyboardInterrupt()

        conn = FakeConn(InterruptingCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            csv_path.write_text("id\n0\n", encoding="utf-8")
            with self.assertRaises(KeyboardInterrupt):
                MSSQL.export_data(f"dbo.users {quote_dot_arg(csv_path)} --format csv --append", ctx)

            self.assertEqual(csv_path.read_text(encoding="utf-8"), "id\n0\n")

    def test_output_open_failure_preserves_existing_output_handle(self):
        existing = io.StringIO()
        ctx = {"conn": FakeConn(), "output": existing}

        with mock.patch("builtins.open", side_effect=OSError("nope")):
            handled = MSSQL.run_dot_command(".output /bad/path.txt", ctx)

        self.assertTrue(handled)
        self.assertIs(ctx["output"], existing)
        existing.write("still open")
        self.assertEqual(existing.getvalue(), "still open")

    def test_output_switch_to_file_reports_previous_close_error(self):
        # close() is where a buffered writer's final flush surfaces failures
        # (disk full, dropped network share). Silently swallowing it would
        # leave the user believing the previous output file is complete.
        class FailCloseFile(io.StringIO):
            def close(self):
                raise OSError("disk full")

        prev = FailCloseFile()
        ctx = {"conn": FakeConn(), "output": prev}

        with tempfile.TemporaryDirectory() as tmpdir:
            new_path = Path(tmpdir) / "out.csv"
            try:
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    handled = MSSQL.run_dot_command(f".output {quote_dot_arg(new_path)}", ctx)
                self.assertIsNot(ctx["output"], prev)
            finally:
                if ctx["output"] is not prev:
                    ctx["output"].close()

        self.assertTrue(handled)
        self.assertIn(
            "Warning: error closing previous output: disk full", stderr.getvalue()
        )
        # The switch itself still succeeds despite the close() failure.
        self.assertIsNot(ctx["output"], prev)
        self.assertTrue(ctx["output"].closed)

    def test_output_switch_to_stdout_reports_previous_close_error(self):
        class FailCloseFile(io.StringIO):
            def close(self):
                raise OSError("disk full")

        prev = FailCloseFile()
        ctx = {"conn": FakeConn(), "output": prev}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            handled = MSSQL.run_dot_command(".output -", ctx)

        self.assertTrue(handled)
        self.assertIn(
            "Warning: error closing previous output: disk full", stderr.getvalue()
        )
        self.assertIs(ctx["output"], sys.stdout)

    def test_output_parse_and_open_errors_go_to_stderr(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command('.output "unterminated', ctx)
        self.assertIn(".output parse error", stderr.getvalue())

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            with mock.patch("builtins.open", side_effect=OSError("nope")):
                MSSQL.run_dot_command(".output /bad/path.txt", ctx)
        self.assertIn("Error opening '/bad/path.txt': nope", stderr.getvalue())

    def test_split_batches_ignores_go_inside_string_literal(self):
        sql = "PRINT 'first line\nGO\nlast line';\nGO\nSELECT 1;"
        self.assertEqual(
            MSSQL.split_batches(sql),
            ["PRINT 'first line\nGO\nlast line';", "SELECT 1;"],
        )

    def test_split_batches_ignores_go_inside_double_quotes(self):
        sql = 'SELECT "first line\nGO\nlast line";\nGO\nSELECT 1;'
        self.assertEqual(
            MSSQL.split_batches(sql),
            ['SELECT "first line\nGO\nlast line";', "SELECT 1;"],
        )

    def test_split_batches_ignores_go_inside_block_comment(self):
        sql = "/* comment\nGO\n*/\nSELECT 1;\nGO\nSELECT 2;"
        self.assertEqual(
            MSSQL.split_batches(sql),
            ["/* comment\nGO\n*/\nSELECT 1;", "SELECT 2;"],
        )

    def test_split_batches_ignores_go_inside_nested_block_comment(self):
        sql = "/* outer\n/* inner */\nGO\n*/\nSELECT 1;\nGO\nSELECT 2;"
        self.assertEqual(
            MSSQL.split_batches(sql),
            ["/* outer\n/* inner */\nGO\n*/\nSELECT 1;", "SELECT 2;"],
        )

    def test_split_batches_preserves_line_breaks_inside_literals(self):
        # str.splitlines() would turn CR, CRLF, U+2028, VT, FF, NEL and \x1c
        # inside the literal into LF and silently change stored data.
        text = "a\r\nb\rc\u2028d\x0be\x0cf\x85g\x1ch"
        sql = f"INSERT t VALUES (N'{text}')\r\nGO\r\nSELECT 1\rGO\rSELECT 2"
        self.assertEqual(
            MSSQL.split_batches(sql),
            [f"INSERT t VALUES (N'{text}')", "SELECT 1", "SELECT 2"],
        )

    def test_split_batches_keeps_original_line_endings_between_lines(self):
        self.assertEqual(MSSQL.split_batches("SELECT 1,\r\n2\nGO\n"), ["SELECT 1,\r\n2"])
        self.assertEqual(MSSQL._sql_lines("a\u2028b\r\nc\rd"), ["a\u2028b", "c", "d"])

    def test_sql_literal_preserves_backslash_before_line_break(self):
        # T-SQL drops "\"+LF and "\"+CRLF (and the "\" of "\"+CR) as a line
        # continuation; doubling leaves exactly the original characters.
        self.assertEqual(MSSQL.sql_literal("C:\\dir\\\nx"), "N'C:\\dir\\\\\n\nx'")
        self.assertEqual(MSSQL.sql_literal("a\\\r\nb"), "N'a\\\\\r\n\r\nb'")
        self.assertEqual(MSSQL.sql_literal("a\\\rb"), "N'a\\\\\rb'")
        self.assertEqual(MSSQL.sql_literal("a\\b\n"), "N'a\\b\n'")
        self.assertEqual(MSSQL.sql_literal("a\\\nb", "mysql"), "'a\\\\\nb'")

    def test_sql_literal_replaces_nul_without_raw_nul_in_sql(self):
        literal = MSSQL.sql_literal("a\x00b\ue000\x00")
        self.assertNotIn("\x00", literal)
        self.assertEqual(
            literal,
            "REPLACE(CAST(N'a\ue001b\ue000\ue001' AS nvarchar(max)) COLLATE Latin1_General_BIN2, "
            "NCHAR(57345), NCHAR(0))",
        )

    def test_sql_literal_zero_pads_years_before_1000(self):
        # strftime("%Y") gives "1" on glibc and SQL Server reads '1-01-01' as 2001.
        self.assertEqual(
            MSSQL.sql_literal(datetime.datetime(1, 1, 1, 0, 0, 0, 123000)),
            "'0001-01-01 00:00:00.123'",
        )
        self.assertEqual(MSSQL.sql_literal(datetime.datetime(99, 12, 19, 1, 2, 3)), "'0099-12-19 01:02:03.000'")
        self.assertEqual(
            MSSQL.sql_literal(datetime.datetime(2020, 5, 6, 7, 8, 9, 10000), "mysql"),
            "'2020-05-06 07:08:09.010'",
        )
        offset = datetime.timezone(datetime.timedelta(hours=-8))
        self.assertEqual(
            MSSQL.sql_literal(datetime.datetime(1, 1, 1, 0, 0, 0, 123000, tzinfo=offset)),
            "'0001-01-01T00:00:00.123-08:00'",
        )

    def test_sql_literal_microseconds_keeps_sub_millisecond_digits(self):
        value = datetime.datetime(2020, 5, 6, 7, 8, 9, 123456)
        offset = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        self.assertEqual(MSSQL.sql_literal(value), "'2020-05-06 07:08:09.123'")
        self.assertEqual(MSSQL.sql_literal(value, "mssql", "microseconds"), "'2020-05-06 07:08:09.123456'")
        self.assertEqual(MSSQL.sql_literal(value, "mysql", "microseconds"), "'2020-05-06 07:08:09.123456'")
        self.assertEqual(
            MSSQL.sql_literal(value.replace(tzinfo=offset), "mssql", "microseconds"),
            "'2020-05-06T07:08:09.123456+05:30'",
        )
        # Millisecond-aligned values (all legacy datetime values) stay at 3
        # digits: SQL Server's datetime rejects '...00.003000'.
        self.assertEqual(
            MSSQL.sql_literal(datetime.datetime(1, 1, 1, 0, 0, 0, 3000), "mssql", "microseconds"),
            "'0001-01-01 00:00:00.003'",
        )

    def test_insert_microseconds_ctx_applies_to_buffered_and_streamed_output(self):
        value = datetime.datetime(2020, 5, 6, 7, 8, 9, 123456)
        for flag, expected in ((False, "'2020-05-06 07:08:09.123'"), (True, "'2020-05-06 07:08:09.123456'")):
            with self.subTest(insert_microseconds=flag):
                ctx = {"mode": "insert", "insert_table": "[t]", "rowcount": False,
                       "output": io.StringIO(), "insert_microseconds": flag}
                MSSQL.print_results([(value,)], ["d"], ctx)
                cursor = FakeCursor([{"description": [("d",)]}])
                cursor.fetchmany = lambda size, _rows=[[(value,)], []]: _rows.pop(0)
                MSSQL.print_results_stream(cursor, ["d"], ctx)
                self.assertEqual(ctx["output"].getvalue(), f"INSERT INTO [t] ([d]) VALUES ({expected});\n" * 2)

    def test_main_insert_microseconds_flag(self):
        value = datetime.datetime(2020, 5, 6, 7, 8, 9, 123456)
        for extra, expected in (([], "'2020-05-06 07:08:09.123'"),
                                (["--insert-microseconds"], "'2020-05-06 07:08:09.123456'")):
            with self.subTest(extra=extra):
                cursor = FakeCursor([{"description": [("d",)]}])
                cursor.fetchmany = lambda size, _rows=[[(value,)], []]: _rows.pop(0)
                argv = ["mssql.py", "-U", "sa", "-c", "SELECT d", "--mode", "insert",
                        "--insert-table", "dbo.t", *extra]
                with mock.patch.object(MSSQL.pymssql, "connect", return_value=FakeConn(cursor)), \
                     mock.patch("sys.argv", argv), \
                     mock.patch("sys.stdout", new=io.StringIO()) as stdout, \
                     mock.patch("sys.stderr", new=io.StringIO()):
                    MSSQL.main()
                self.assertIn(f"VALUES ({expected});", stdout.getvalue())

    def test_interactive_loop_ignores_go_inside_string_literal(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}
        lines = iter(["PRINT 'first line", "GO", "last line';"])

        with mock.patch("builtins.input", side_effect=lambda prompt: next(lines)):
            with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                with self.assertRaises(StopIteration):
                    MSSQL.interactive_loop(ctx)

        execute_sql.assert_called_once_with("PRINT 'first line\nGO\nlast line';", ctx)

    def test_interactive_loop_does_not_terminate_on_semicolon_inside_string(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}
        lines = iter(["PRINT 'value;","still string';"])

        with mock.patch("builtins.input", side_effect=lambda prompt: next(lines)):
            with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                with self.assertRaises(StopIteration):
                    MSSQL.interactive_loop(ctx)

        execute_sql.assert_called_once_with("PRINT 'value;\nstill string';", ctx)

    def test_interactive_loop_keyboard_interrupt_resets_lexer_state(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}
        inputs = iter([KeyboardInterrupt(), "GO", EOFError()])

        def fake_input(prompt):
            value = next(inputs)
            if isinstance(value, BaseException):
                raise value
            return value

        with mock.patch("builtins.input", side_effect=fake_input):
            with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                MSSQL.interactive_loop(ctx)

        execute_sql.assert_not_called()

    def test_interactive_loop_unknown_dot_command_goes_to_stderr(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}
        inputs = iter([".wat", EOFError()])

        def fake_input(prompt):
            value = next(inputs)
            if isinstance(value, BaseException):
                raise value
            return value

        with mock.patch("builtins.input", side_effect=fake_input):
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.interactive_loop(ctx)

        self.assertIn("Unknown command '.wat'. Try .help", stderr.getvalue())
        self.assertEqual(ctx["output"].getvalue(), "")

    def test_format_insert_escapes_closing_bracket_in_column_names(self):
        sql = MSSQL.format_insert([(1,)], ["weird]name"], "[dbo].[t]")
        self.assertIn("[weird]]name]", sql)

    def test_statement_terminator_ignores_nested_block_comment_semicolon(self):
        lines = ["/* outer", "/* inner */", ";", "*/", "SELECT 1;"]
        self.assertTrue(MSSQL._ends_with_statement_terminator(lines))
        self.assertFalse(MSSQL._ends_with_statement_terminator(lines[:-1]))

    def test_setup_readline_ignores_history_io_errors(self):
        fake_readline = types.SimpleNamespace(
            __doc__="",
            read_history_file=mock.Mock(side_effect=OSError("ro")),
            write_history_file=mock.Mock(side_effect=OSError("ro")),
            set_history_length=mock.Mock(),
            set_completer=mock.Mock(),
            parse_and_bind=mock.Mock(),
        )

        with mock.patch.object(MSSQL, "readline", fake_readline):
            with mock.patch.object(MSSQL.atexit, "register") as register:
                MSSQL.setup_readline()

        register.assert_called_once()
        write_cb = register.call_args.args[0]
        write_cb()

    def test_load_readline_falls_back_to_pyreadline3(self):
        fallback = object()

        def fake_import_module(module_name):
            if module_name == "readline":
                raise ImportError("missing readline")
            if module_name == "pyreadline3":
                return fallback
            raise AssertionError(f"unexpected import: {module_name}")

        with mock.patch.object(MSSQL.importlib, "import_module", side_effect=fake_import_module):
            self.assertIs(MSSQL._load_readline(), fallback)

    def test_setup_readline_noops_without_compatible_module(self):
        with mock.patch.object(MSSQL, "readline", None):
            with mock.patch.object(MSSQL.atexit, "register") as register:
                MSSQL.setup_readline()

        register.assert_not_called()

    def test_build_connection_rejects_invalid_env_port(self):
        args = types.SimpleNamespace(
            host="",
            port=0,
            user="sa",
            password="",
            database="",
            timeout=30,
        )

        with mock.patch.dict(os.environ, {"MSSQL_PORT": "abc"}, clear=False):
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                with self.assertRaises(SystemExit) as exc:
                    MSSQL.build_connection(args)

        self.assertEqual(exc.exception.code, 1)
        self.assertIn("Error: invalid MSSQL_PORT 'abc'", stderr.getvalue())

    def test_connect_passes_separate_query_timeout(self):
        args = types.SimpleNamespace(
            host="db.example", port=1433, user="sa", password="secret",
            database="app", timeout=5, query_timeout=17,
        )
        conn = object()

        with mock.patch.object(MSSQL.pymssql, "connect", return_value=conn) as connect:
            result, _, _ = MSSQL._connect(args)

        self.assertIs(result, conn)
        self.assertEqual(connect.call_args.kwargs["login_timeout"], 5)
        self.assertEqual(connect.call_args.kwargs["timeout"], 17)
        self.assertTrue(connect.call_args.kwargs["autocommit"])

    # ── Trusted connection (-E, Windows Integrated Auth via pyodbc) ────────

    def test_connect_trusted_builds_expected_connection_string_and_metadata(self):
        args = types.SimpleNamespace(
            host="db.example", port=1433, database="app",
            timeout=5, query_timeout=17, user="", password="",
            trusted_connection=True, odbc_driver="", trust_server_certificate=False,
        )
        conn = mock.Mock()

        with mock.patch.object(MSSQL.pyodbc, "connect", return_value=conn) as connect:
            result, conn_info, driver = MSSQL._connect(args)

        self.assertIs(result, conn)
        conn_str = connect.call_args.args[0]
        self.assertIn(f"DRIVER={{{MSSQL.DEFAULT_ODBC_DRIVER}}}", conn_str)
        self.assertIn("SERVER={db.example,1433}", conn_str)
        self.assertIn("Trusted_Connection=yes", conn_str)
        self.assertIn("DATABASE={app}", conn_str)
        self.assertNotIn("TrustServerCertificate", conn_str)
        self.assertEqual(connect.call_args.kwargs["timeout"], 5)
        self.assertEqual(connect.call_args.kwargs["autocommit"], True)
        self.assertEqual(conn.timeout, 17)
        conn.add_output_converter.assert_called_once_with(-155, MSSQL._decode_datetimeoffset)
        self.assertEqual(conn_info, "(trusted)@db.example:1433/app")
        self.assertEqual(
            driver,
            {"name": "pyodbc", "version": "test-pyodbc", "paramstyle": "qmark", "dialect": "mssql"},
        )

    def test_connect_trusted_odbc_driver_precedence(self):
        base = dict(
            host="localhost", port=0, database="", timeout=30, query_timeout=30,
            user="", password="", trusted_connection=True, trust_server_certificate=False,
        )

        args = types.SimpleNamespace(odbc_driver="", **base)
        with mock.patch.dict(os.environ, {"MSSQL_ODBC_DRIVER": ""}, clear=False):
            with mock.patch.object(MSSQL.pyodbc, "connect", return_value=mock.Mock()) as connect:
                MSSQL._connect(args)
        self.assertIn(f"DRIVER={{{MSSQL.DEFAULT_ODBC_DRIVER}}}", connect.call_args.args[0])

        with mock.patch.dict(os.environ, {"MSSQL_ODBC_DRIVER": "ODBC Driver 17 for SQL Server"}, clear=False):
            with mock.patch.object(MSSQL.pyodbc, "connect", return_value=mock.Mock()) as connect:
                MSSQL._connect(args)
        self.assertIn("DRIVER={ODBC Driver 17 for SQL Server}", connect.call_args.args[0])

        args_with_flag = types.SimpleNamespace(odbc_driver="Custom Driver", **base)
        with mock.patch.dict(os.environ, {"MSSQL_ODBC_DRIVER": "ODBC Driver 17 for SQL Server"}, clear=False):
            with mock.patch.object(MSSQL.pyodbc, "connect", return_value=mock.Mock()) as connect:
                MSSQL._connect(args_with_flag)
        self.assertIn("DRIVER={Custom Driver}", connect.call_args.args[0])

    def test_connect_trusted_trust_server_certificate_flag(self):
        args = types.SimpleNamespace(
            host="localhost", port=0, database="", timeout=30, query_timeout=30,
            user="", password="", trusted_connection=True, odbc_driver="",
            trust_server_certificate=True,
        )
        with mock.patch.object(MSSQL.pyodbc, "connect", return_value=mock.Mock()) as connect:
            MSSQL._connect(args)
        self.assertIn("TrustServerCertificate=yes", connect.call_args.args[0])

    def test_connect_trusted_braces_semicolon_in_database_so_it_cannot_inject_keys(self):
        # A database name containing ';' must not be able to append extra
        # connection-string keys (e.g. downgrading TLS via Encrypt=no).
        args = types.SimpleNamespace(
            host="localhost", port=0, database="mydb;Encrypt=no;TrustServerCertificate=yes",
            timeout=30, query_timeout=30, user="", password="",
            trusted_connection=True, odbc_driver="", trust_server_certificate=False,
        )
        with mock.patch.object(MSSQL.pyodbc, "connect", return_value=mock.Mock()) as connect:
            MSSQL._connect(args)

        conn_str = connect.call_args.args[0]
        # The injected-looking keys must stay inside the braced DATABASE
        # value — the whole thing is the last, and only, DATABASE field.
        self.assertTrue(
            conn_str.endswith(";DATABASE={mydb;Encrypt=no;TrustServerCertificate=yes}")
        )
        self.assertEqual(conn_str.count("DATABASE="), 1)

    def test_connect_trusted_escapes_closing_brace_in_driver_name(self):
        # A driver name containing '}' must not be able to close the DRIVER
        # field early and inject a second key afterward.
        args = types.SimpleNamespace(
            host="localhost", port=0, database="", timeout=30, query_timeout=30,
            user="", password="", trusted_connection=True,
            odbc_driver="Evil}; UID=sa; PWD=x; DRIVER={ODBC Driver 18 for SQL Server",
            trust_server_certificate=False,
        )
        with mock.patch.object(MSSQL.pyodbc, "connect", return_value=mock.Mock()) as connect:
            MSSQL._connect(args)

        conn_str = connect.call_args.args[0]
        # '}' doubled inside the braces keeps the whole malicious string one
        # opaque DRIVER value; SERVER must be the very next field, not an
        # injected UID/PWD pair from inside the driver name.
        expected_driver_field = (
            "DRIVER={Evil}}; UID=sa; PWD=x; DRIVER={ODBC Driver 18 for SQL Server}"
        )
        self.assertEqual(
            conn_str,
            f"{expected_driver_field};SERVER={{localhost,1433}};Trusted_Connection=yes",
        )

    def test_connect_trusted_requires_pyodbc_installed(self):
        args = types.SimpleNamespace(
            host="localhost", port=0, database="", timeout=30, query_timeout=30,
            user="", password="", trusted_connection=True, odbc_driver="",
            trust_server_certificate=False,
        )
        with mock.patch.object(MSSQL, "pyodbc", None):
            with self.assertRaisesRegex(ValueError, "pyodbc is not installed"):
                MSSQL._connect(args)

    def test_main_rejects_trusted_connection_combined_with_user(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "-E", "-U", "sa"]):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 2)
        build_connection.assert_not_called()
        self.assertIn("--trusted-connection cannot be combined", stderr.getvalue())

    def test_main_rejects_trusted_connection_combined_with_password(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "-E", "-W", "secret"]):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 2)
        build_connection.assert_not_called()
        self.assertIn("--trusted-connection cannot be combined", stderr.getvalue())

    def test_main_trusted_connection_does_not_require_user(self):
        cursor = FakeCursor([{"description": None, "rowcount": 1}])
        conn = FakeConn(cursor)

        with mock.patch.object(MSSQL.pyodbc, "connect", return_value=conn):
            with mock.patch("sys.argv", ["mssql.py", "-H", "myserver", "-E", "-d", "mydb", "-c", "SELECT 1"]):
                with mock.patch("sys.stdout", new=io.StringIO()):
                    with mock.patch("sys.stderr", new=io.StringIO()):
                        MSSQL.main()

        self.assertEqual(conn.commit_count, 1)

    def test_interactive_loop_banner_shows_active_driver(self):
        ctx = {
            "conn": FakeConn(),
            "conn_info": "(trusted)@host:1433/db",
            "driver_name": "pyodbc",
            "driver_version": "5.3.0",
            "output": io.StringIO(),
        }
        with mock.patch("builtins.input", side_effect=EOFError):
            with mock.patch("sys.stdout", new=io.StringIO()) as stdout:
                MSSQL.interactive_loop(ctx)

        self.assertIn("MSSQL Query Tool  (pyodbc 5.3.0)", stdout.getvalue())

    def test_run_dot_command_catches_pyodbc_error_under_trusted_connection(self):
        # fake_pymssql.Error is the builtin Exception, so this specifically
        # proves DBError catches a *pyodbc*-raised error, not just that
        # Exception happens to catch everything.
        class PyodbcErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pyodbc.Error("odbc boom")

        conn = FakeConn(PyodbcErrorCursor())
        ctx = {"conn": conn, "output": io.StringIO(), "paramstyle": "qmark"}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            handled = MSSQL.run_dot_command(".databases", ctx)

        self.assertTrue(handled)
        self.assertIn("Query error: odbc boom", stderr.getvalue())

    def test_run_dot_command_adapts_placeholders_for_qmark_paramstyle(self):
        cursor = FakeCursor(
            [{"description": [("TABLE_SCHEMA",), ("TABLE_NAME",), ("TABLE_TYPE",)], "rows": []}]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO(), "paramstyle": "qmark"}

        MSSQL.run_dot_command(".tables foo", ctx)

        sql, params = cursor.executed[0]
        self.assertNotIn("%s", sql)
        self.assertIn("?", sql)
        self.assertEqual(params, ("%foo%", "foo"))

    def test_import_adapts_placeholders_for_qmark_paramstyle(self):
        metadata = FakeCursor(
            [{"description": [("COLUMN_NAME",)], "rows": [("id", "int", "NO")]}]
        )
        conn = FakeConn(metadata)
        ctx = {"conn": conn, "output": io.StringIO(), "paramstyle": "qmark"}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        metadata_sql = metadata.executed[0][0]
        insert_sql = [sql for sql, _ in metadata.executed if sql.startswith("INSERT")][0]
        self.assertNotIn("%s", metadata_sql)
        self.assertIn("?", metadata_sql)
        self.assertNotIn("%s", insert_sql)
        self.assertIn("?", insert_sql)

    def test_import_metadata_query_preserves_literal_percent_s_in_database_name(self):
        # "%s" is legal text inside a T-SQL bracket-quoted identifier (e.g. a
        # database literally named [my%sdb]). Placeholder adaptation must not
        # mistake that embedded text for one of its own %s/? markers — it
        # should leave the identifier untouched and still bind exactly the
        # two real parameters (table_name, schema_name).
        metadata = FakeCursor(
            [{"description": [("COLUMN_NAME",)], "rows": [("id", "int", "NO")]}]
        )
        conn = FakeConn(metadata)
        ctx = {"conn": conn, "output": io.StringIO(), "paramstyle": "qmark"}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            MSSQL.import_data(f"{quote_dot_arg(csv_path)} [my%sdb].dbo.users", ctx)

        metadata_sql, metadata_params = metadata.executed[0]
        self.assertIn("[my%sdb].INFORMATION_SCHEMA.COLUMNS", metadata_sql)
        self.assertEqual(metadata_sql.count("?"), 2)
        self.assertEqual(metadata_params, ("users", "dbo"))

    def test_run_dot_command_schema_preserves_literal_percent_s_in_database_name(self):
        cursor = FakeCursor(
            [{
                "description": [
                    ("TABLE_SCHEMA",), ("TABLE_NAME",), ("COLUMN_NAME",), ("DATA_TYPE",),
                    ("CHARACTER_MAXIMUM_LENGTH",), ("IS_NULLABLE",), ("COLUMN_DEFAULT",),
                    ("ORDINAL_POSITION",),
                ],
                "rows": [],
            }]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO(), "paramstyle": "qmark"}

        MSSQL.run_dot_command(".schema [my%sdb].dbo.users", ctx)

        sql, params = cursor.executed[0]
        self.assertIn("[my%sdb].INFORMATION_SCHEMA.COLUMNS", sql)
        self.assertEqual(sql.count("?"), 2)
        self.assertEqual(params, ("users", "dbo"))

    def test_main_validates_insert_mode_before_connecting(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "--mode", "insert"]):
                with self.assertRaises(SystemExit) as exc:
                    MSSQL.main()

        self.assertEqual(exc.exception.code, 2)
        build_connection.assert_not_called()

    def test_main_rejects_multiple_explicit_execution_sources_before_connecting(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-c", "SELECT 1", "-f", "query.sql"]):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 2)
        build_connection.assert_not_called()
        self.assertIn("choose only one execution source", stderr.getvalue())

    def test_main_rejects_extra_positionals_when_database_flag_is_used(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-d", "mydb", "otherdb", "query.sql"]):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 2)
        build_connection.assert_not_called()
        self.assertIn("when --database is used", stderr.getvalue())

    def test_main_command_exits_nonzero_on_sql_error(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("boom")

        conn = FakeConn(ErrorCursor())

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-c", "UPDATE t SET x = 1"]):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertTrue(conn.closed)
        self.assertIn("Error: boom", stderr.getvalue())

    def test_main_stdin_exits_nonzero_on_sql_error(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("boom")

        conn = FakeConn(ErrorCursor())

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch("sys.argv", ["mssql.py", "-U", "sa"]):
                with mock.patch("sys.stdin", new=fake_piped_stdin("UPDATE t SET x = 1")):
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        with self.assertRaises(SystemExit) as exc:
                            MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertTrue(conn.closed)
        self.assertIn("Error: boom", stderr.getvalue())

    def test_main_stdin_decodes_non_ascii_utf8(self):
        # A UTF-8-encoded accented identifier/value piped on stdin must
        # decode correctly regardless of the platform's console/locale
        # encoding, since main() now reads sys.stdin.buffer directly.
        cursor = FakeCursor([{"description": None, "rowcount": 1}])
        conn = FakeConn(cursor)

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch("sys.argv", ["mssql.py", "-U", "sa"]):
                with mock.patch("sys.stdin", new=fake_piped_stdin("INSERT INTO t VALUES (N'café')")):
                    with mock.patch("sys.stderr", new=io.StringIO()):
                        MSSQL.main()

        self.assertTrue(conn.closed)
        self.assertEqual(cursor.executed[0][0], "INSERT INTO t VALUES (N'café')")

    def test_main_stdin_rejects_invalid_utf8_with_friendly_error(self):
        conn = FakeConn()
        bad_bytes = b"SELECT '\xff\xfe'"  # not valid UTF-8

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch("sys.argv", ["mssql.py", "-U", "sa"]):
                with mock.patch("sys.stdin", new=fake_piped_stdin(bad_bytes)):
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        with self.assertRaises(SystemExit) as exc:
                            MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertTrue(conn.closed)
        self.assertIn("Error reading stdin: invalid UTF-8", stderr.getvalue())

    def test_main_stdin_rejects_oversized_input_before_execution(self):
        conn = FakeConn()

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch.object(MSSQL, "MAX_SQL_INPUT_BYTES", 5):
                with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                    with mock.patch("sys.argv", ["mssql.py", "-U", "sa"]):
                        with mock.patch("sys.stdin", new=fake_piped_stdin("SELECT 1")):
                            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                                with self.assertRaises(SystemExit) as exc:
                                    MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        execute_sql.assert_not_called()
        self.assertIn("exceeds the", stderr.getvalue())

    def test_main_positional_database_connects_interactively(self):
        conn = FakeConn()

        def fake_build_connection(args):
            self.assertEqual(args.database, "mydb")
            self.assertEqual(args.sql_file, None)
            return conn, "test", FAKE_DRIVER

        with mock.patch.object(MSSQL, "build_connection", side_effect=fake_build_connection):
            with mock.patch.object(MSSQL, "interactive_loop") as interactive_loop:
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "mydb"]):
                    with mock.patch("sys.stdin.isatty", return_value=True):
                        MSSQL.main()

        interactive_loop.assert_called_once()
        self.assertTrue(conn.closed)

    def test_main_database_flag_keeps_single_positional_as_sql_file(self):
        conn = FakeConn()

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch.object(MSSQL, "execute_file") as execute_file:
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-d", "mydb", "query.sql"]):
                    MSSQL.main()

        execute_file.assert_called_once()
        self.assertEqual(execute_file.call_args.args[0], "query.sql")
        self.assertTrue(conn.closed)

    def test_main_exits_nonzero_when_flag_script_path_cannot_be_read(self):
        conn = FakeConn()

        with tempfile.TemporaryDirectory() as tmpdir:
            missing = Path(tmpdir) / "missing.sql"
            with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-f", str(missing)]):
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        with self.assertRaises(SystemExit) as exc:
                            MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertTrue(conn.closed)
        self.assertIn("Error reading", stderr.getvalue())

    def test_main_file_exits_nonzero_on_sql_error(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("boom")

        conn = FakeConn(ErrorCursor())

        with tempfile.TemporaryDirectory() as tmpdir:
            sql_path = Path(tmpdir) / "query.sql"
            sql_path.write_text("UPDATE t SET x = 1", encoding="utf-8")
            with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-f", str(sql_path)]):
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        with self.assertRaises(SystemExit) as exc:
                            MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertTrue(conn.closed)
        self.assertIn("Error: boom", stderr.getvalue())

    def test_main_exits_nonzero_when_positional_script_path_cannot_be_read(self):
        conn = FakeConn()

        with tempfile.TemporaryDirectory() as tmpdir:
            missing = Path(tmpdir) / "missing.sql"
            with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-d", "mydb", str(missing)]):
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        with self.assertRaises(SystemExit) as exc:
                            MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertTrue(conn.closed)
        self.assertIn("Error reading", stderr.getvalue())

    def test_main_two_positionals_are_database_and_sql_file(self):
        conn = FakeConn()

        def fake_build_connection(args):
            self.assertEqual(args.database, "mydb")
            self.assertEqual(args.sql_file, "query.sql")
            return conn, "test", FAKE_DRIVER

        with mock.patch.object(MSSQL, "build_connection", side_effect=fake_build_connection):
            with mock.patch.object(MSSQL, "execute_file") as execute_file:
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "mydb", "query.sql"]):
                    MSSQL.main()

        execute_file.assert_called_once()
        self.assertEqual(execute_file.call_args.args[0], "query.sql")
        self.assertTrue(conn.closed)

    def test_main_single_sql_suffix_positional_is_database(self):
        conn = FakeConn()

        def fake_build_connection(args):
            self.assertEqual(args.database, "reporting.sql")
            self.assertEqual(args.sql_file, None)
            return conn, "test", FAKE_DRIVER

        with mock.patch.object(MSSQL, "build_connection", side_effect=fake_build_connection):
            with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "reporting.sql", "-c", "SELECT 1"]):
                    MSSQL.main()

        execute_sql.assert_called_once()
        self.assertTrue(conn.closed)

    def test_main_single_positional_matching_local_file_is_database(self):
        conn = FakeConn()

        def fake_build_connection(args):
            self.assertEqual(args.database, "prod")
            self.assertEqual(args.sql_file, None)
            return conn, "test", FAKE_DRIVER

        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "prod").write_text("SELECT 1", encoding="utf-8")
            try:
                os.chdir(tmpdir)
                with mock.patch.object(MSSQL, "build_connection", side_effect=fake_build_connection):
                    with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                        with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "prod", "-c", "SELECT 1"]):
                            MSSQL.main()
            finally:
                os.chdir(old_cwd)

        execute_sql.assert_called_once()
        self.assertTrue(conn.closed)

    def test_main_help_examples_prefer_password_environment_variable(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "--help"]):
                with mock.patch("sys.stdout", new=io.StringIO()) as stdout:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 0)
        build_connection.assert_not_called()
        help_text = stdout.getvalue()
        normalized_help = " ".join(help_text.split())
        self.assertIn("Set MSSQL_PASSWORD in your environment", help_text)
        self.assertIn("avoid putting secrets on command lines", normalized_help)
        self.assertNotIn("MSSQL_PASSWORD=secret", help_text)
        self.assertNotIn("-W secret", help_text)
        self.assertNotIn("-P secret", help_text)

    def test_main_closes_connection_when_output_open_fails(self):
        conn = FakeConn()

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch("builtins.open", side_effect=OSError("nope")):
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa", "-o", "/bad/path.txt"]):
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        self.assertTrue(conn.closed)

    def test_dot_help_mentions_jsonl_import_format(self):
        self.assertIn("--format csv|json|jsonl", MSSQL.DOT_HELP)
        self.assertIn("--format csv|json|jsonl", MSSQL.import_data.__doc__)

    # ── transaction_depth resync after mid-batch error ─────────────────────

    def test_execute_sql_resyncs_transaction_depth_after_mid_batch_error(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("boom")

        class TwoCursorConn(FakeConn):
            """First cursor() call (the batch itself) errors; the second
            (the @@TRANCOUNT resync) succeeds and reports a deeper nesting
            than ctx currently knows about."""

            def __init__(self, trancount):
                super().__init__(cursor=None)
                self._trancount = trancount
                self._calls = 0

            def cursor(self):
                self._calls += 1
                if self._calls == 1:
                    return ErrorCursor()
                return FakeCursor(
                    [{"description": [("x",)], "rows": [(self._trancount,)], "rowcount": 1}]
                )

        conn = TwoCursorConn(trancount=2)
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        ok = MSSQL.execute_sql("BEGIN TRANSACTION; UPDATE t SET x = 1", ctx)

        self.assertFalse(ok)
        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(ctx["transaction_depth"], 2)
        self.assertTrue(ctx["transaction_open"])

    def test_execute_sql_leaves_depth_unchanged_when_resync_query_fails(self):
        # Same cursor instance is reused for both the failing batch and the
        # resync attempt (simulating a fully dead connection) — resync
        # should fail silently and leave the previously-known depth alone.
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("boom")

        conn = FakeConn(ErrorCursor())
        ctx = {"conn": conn, "output": io.StringIO(), "transaction_depth": 1, "transaction_open": True}

        ok = MSSQL.execute_sql("UPDATE t SET x = 1", ctx)

        self.assertFalse(ok)
        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

    # ── .reconnect ───────────────────────────────────────────────────────

    def test_run_dot_command_reconnect_replaces_connection_and_resets_transaction_state(self):
        old_conn = FakeConn()
        new_conn = FakeConn()
        conn_args = types.SimpleNamespace(
            host="", port=0, user="sa", password="", database="", timeout=30,
        )
        ctx = {
            "conn": old_conn,
            "conn_args": conn_args,
            "output": io.StringIO(),
            "transaction_depth": 2,
            "transaction_open": True,
            "savepoints": {"SP1"},
            "connection_usable": False,
        }

        with mock.patch.object(MSSQL, "_connect", return_value=(new_conn, "sa@host:1433", FAKE_DRIVER)):
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                handled = MSSQL.run_dot_command(".reconnect", ctx)

        self.assertTrue(handled)
        self.assertTrue(old_conn.closed)
        self.assertIs(ctx["conn"], new_conn)
        self.assertEqual(ctx["conn_info"], "sa@host:1433")
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])
        self.assertEqual(ctx["savepoints"], set())
        self.assertTrue(ctx["connection_usable"])
        self.assertIn("Reconnected to: sa@host:1433", stderr.getvalue())

    def test_run_dot_command_reconnect_keeps_old_connection_on_failure(self):
        old_conn = FakeConn()
        conn_args = types.SimpleNamespace(
            host="", port=0, user="sa", password="", database="", timeout=30,
        )
        ctx = {
            "conn": old_conn,
            "conn_args": conn_args,
            "output": io.StringIO(),
            "transaction_depth": 0,
        }

        with mock.patch.object(MSSQL, "_connect", side_effect=MSSQL.pymssql.Error("down")):
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                handled = MSSQL.run_dot_command(".reconnect", ctx)

        self.assertTrue(handled)
        self.assertFalse(old_conn.closed)
        self.assertIs(ctx["conn"], old_conn)
        self.assertIn("Reconnect failed: down", stderr.getvalue())

    def test_run_dot_command_reconnect_without_conn_args_reports_error(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            handled = MSSQL.run_dot_command(".reconnect", ctx)

        self.assertTrue(handled)
        self.assertIn("not available", stderr.getvalue())

    def test_reconnect_is_in_dot_help_and_completer(self):
        self.assertIn(".reconnect", MSSQL.DOT_HELP)

    def test_main_records_conn_args_for_reconnect(self):
        conn = FakeConn()

        def fake_build_connection(args):
            return conn, "test", FAKE_DRIVER

        with mock.patch.object(MSSQL, "build_connection", side_effect=fake_build_connection):
            with mock.patch.object(MSSQL, "interactive_loop") as interactive_loop:
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa"]):
                    with mock.patch("sys.stdin.isatty", return_value=True):
                        MSSQL.main()

        ctx_arg = interactive_loop.call_args[0][0]
        self.assertIsInstance(ctx_arg["conn_args"], object)
        self.assertEqual(ctx_arg["conn_args"].user, "sa")

    # ── read_json_data / read_csv_data streaming docs ──────────────────────

    def test_read_json_data_docstring_clarifies_array_is_not_streamed(self):
        self.assertIn("does NOT stream", MSSQL.read_json_data.__doc__)
        self.assertIn("streams", MSSQL.read_csv_data.__doc__)

    def test_read_json_data_rejects_oversized_array_before_parsing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "big.json"
            json_path.write_text('[{"a": 1}, {"a": 2}, {"a": 3}]', encoding="utf-8")
            with mock.patch.object(MSSQL, "MAX_JSON_ARRAY_IMPORT_BYTES", 5):
                with self.assertRaisesRegex(ValueError, "exceeds the"):
                    MSSQL.read_json_data(json_path, 0)

    def test_import_reports_oversized_json_array_as_a_clean_read_error(self):
        metadata = FakeCursor(
            [{"description": [("COLUMN_NAME",)], "rows": [("id", "int", "NO")]}]
        )
        conn = FakeConn(metadata)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "big.json"
            json_path.write_text('[{"id": 1}, {"id": 2}, {"id": 3}]', encoding="utf-8")
            with mock.patch.object(MSSQL, "MAX_JSON_ARRAY_IMPORT_BYTES", 5):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users", ctx)

        self.assertIn("exceeds the", stderr.getvalue())
        self.assertIn("convert to .jsonl", stderr.getvalue())

    # ── --skip beyond end of file warns instead of silently importing 0 ────

    def test_import_jsonl_skip_all_rows_warns_on_stderr(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "users.jsonl"
            jsonl_path.write_text('{"id":1}\n', encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(jsonl_path)} dbo.users --format jsonl --skip 1", ctx)

        self.assertIn("Warning: --skip 1 leaves no rows", stderr.getvalue())

    def test_import_json_skip_all_rows_warns_on_stderr(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "users.json"
            json_path.write_text('[{"id":1}]', encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users --format json --skip 1", ctx)

        self.assertIn("Warning: --skip 1 leaves no rows", stderr.getvalue())

    # ── cross-platform: dot-command argument tokenizing ─────────────────────

    def test_split_dot_command_args_posix_strips_quotes(self):
        with mock.patch.object(MSSQL.os, "name", "posix"):
            self.assertEqual(
                MSSQL._split_dot_command_args('"my file.csv" dbo.users'),
                ["my file.csv", "dbo.users"],
            )

    def test_split_dot_command_args_windows_strips_quotes_and_keeps_backslashes(self):
        with mock.patch.object(MSSQL.os, "name", "nt"):
            self.assertEqual(
                MSSQL._split_dot_command_args(r'"C:\data\my file.csv" dbo.users'),
                [r"C:\data\my file.csv", "dbo.users"],
            )

    def test_split_dot_command_args_windows_preserves_unquoted_backslash_path(self):
        with mock.patch.object(MSSQL.os, "name", "nt"):
            self.assertEqual(
                MSSQL._split_dot_command_args(r"C:\data\file.csv dbo.users"),
                [r"C:\data\file.csv", "dbo.users"],
            )

    def test_split_dot_command_args_windows_preserves_single_quoted_paths(self):
        paths = [
            r"C:\data\my file.csv",
            r"\\server\share\my file.csv",
            r"\\?\C:\data\my file.csv",
            "C:\\data\\",
        ]
        with mock.patch.object(MSSQL, "os", types.SimpleNamespace(name="nt")):
            for path in paths:
                with self.subTest(path=path):
                    self.assertEqual(
                        MSSQL._split_dot_command_args(f"'{path}' dbo.users"),
                        [path, "dbo.users"],
                    )

    def test_split_dot_command_args_windows_preserves_query_and_null_marker(self):
        with mock.patch.object(MSSQL, "os", types.SimpleNamespace(name="nt")):
            self.assertEqual(
                MSSQL._split_dot_command_args(
                    r'''--query 'SELECT "C:\temp" AS path' --null-as '\N' '''
                ),
                ["--query", r'SELECT "C:\temp" AS path', "--null-as", r"\N"],
            )

    def test_split_dot_command_args_posix_preserves_shell_escapes(self):
        with mock.patch.object(MSSQL, "os", types.SimpleNamespace(name="posix")):
            self.assertEqual(
                MSSQL._split_dot_command_args(r'my\ file.csv "say \"hi\"" \#literal'),
                ["my file.csv", 'say "hi"', "#literal"],
            )

    def test_split_dot_command_args_rejects_unmatched_quotes_on_both_platforms(self):
        for platform in ("nt", "posix"):
            with self.subTest(platform=platform):
                with mock.patch.object(MSSQL, "os", types.SimpleNamespace(name=platform)):
                    for arg in ("'unterminated", '"unterminated'):
                        with self.assertRaises(ValueError):
                            MSSQL._split_dot_command_args(arg)

    def test_quote_dot_arg_round_trips_paths_on_both_platforms(self):
        paths = [
            "/tmp/Scott O'Brien/my file.csv",
            r"C:\Scott O'Brien\my file.csv",
            r"\\server\share\my file.csv",
        ]
        for platform in ("nt", "posix"):
            with mock.patch.object(MSSQL, "os", types.SimpleNamespace(name=platform)):
                for path in paths:
                    with self.subTest(platform=platform, path=path):
                        self.assertEqual(
                            MSSQL._split_dot_command_args(quote_dot_arg(path)), [path]
                        )

    def test_import_json_accepts_path_with_spaces_and_quotes(self):
        cursor = FakeCursor([{
            "description": [("COLUMN_NAME",), ("DATA_TYPE",), ("IS_NULLABLE",)],
            "rows": [("id", "int", "NO")],
        }])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}
        with tempfile.TemporaryDirectory(prefix="mssql import ") as tmpdir:
            json_path = Path(tmpdir) / "users 'copy'.json"
            json_path.write_text('[{"id":1}]', encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.import_data(f"{quote_dot_arg(json_path)} dbo.users", ctx)

        self.assertEqual(cursor.executed[-1], (
            "INSERT INTO [dbo].[users] ([id]) VALUES (%s)", [(1,)]
        ))
        self.assertEqual(conn.commit_count, 1)

    def test_read_dot_command_accepts_quoted_windows_path(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}

        with mock.patch.object(MSSQL.os, "name", "nt"):
            with mock.patch.object(MSSQL, "execute_file") as execute_file:
                handled = MSSQL.run_dot_command(r'.read "C:\scripts\my query.sql"', ctx)

        self.assertTrue(handled)
        execute_file.assert_called_once_with(r"C:\scripts\my query.sql", ctx)

    # ── cross-platform: non-console stdout/stderr encoding ──────────────────

    def test_main_hardens_stdout_stderr_encoding(self):
        conn = FakeConn()

        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)):
            with mock.patch.object(MSSQL, "interactive_loop"):
                with mock.patch("sys.argv", ["mssql.py", "-U", "sa"]):
                    with mock.patch("sys.stdin.isatty", return_value=True):
                        with mock.patch.object(sys.stdout, "reconfigure") as out_reconfig:
                            with mock.patch.object(sys.stderr, "reconfigure") as err_reconfig:
                                MSSQL.main()

        out_reconfig.assert_called_once_with(encoding="utf-8", errors="replace", newline="")
        err_reconfig.assert_called_once_with(encoding="utf-8", errors="replace", newline="")

    def test_csv_stdout_round_trips_windows_newlines_and_unicode(self):
        rows = [("café",), ("first\r\nsecond",), ("last",)]

        class StreamCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("value",)]}])
                self.chunks = [rows[:2], rows[2:]]

            def fetchmany(self, size):
                return self.chunks.pop(0) if self.chunks else []

        for operation in ("print", "stream", "export"):
            with self.subTest(operation=operation):
                raw = io.BytesIO()
                # Model Windows stdout's default CRLF translation on any OS.
                with io.TextIOWrapper(raw, encoding="cp1252", newline="\r\n") as out:
                    with mock.patch("sys.stdout", out), mock.patch("sys.stderr", io.StringIO()):
                        MSSQL._harden_console_encoding()
                        cursor = StreamCursor()
                        ctx = {"conn": FakeConn(cursor), "output": out,
                               "mode": "csv", "rowcount": False}
                        if operation == "print":
                            MSSQL.print_results(rows, ["value"], ctx)
                        elif operation == "stream":
                            MSSQL.print_results_stream(cursor, ["value"], ctx)
                        else:
                            MSSQL.export_data("dbo.users - --format csv", ctx)
                        out.flush()
                        data = raw.getvalue()

                self.assertNotIn(b"\r\r\n", data)
                parsed = list(MSSQL.csv.reader(io.StringIO(data.decode("utf-8"), newline="")))
                self.assertEqual(parsed, [["value"], ["café"], ["first\r\nsecond"], ["last"]])

    def test_harden_console_encoding_ignores_streams_without_reconfigure(self):
        # io.StringIO (used throughout this test suite as a stand-in stream)
        # has no reconfigure() method — must not raise.
        with mock.patch("sys.stdout", new=io.StringIO()):
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL._harden_console_encoding()  # no exception

    def test_import_csv_skip_all_rows_does_not_warn(self):
        # CSV always has file_cols from the header regardless of --skip, so
        # the json/jsonl-specific warning must not fire here.
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [
                        ("id", "int", "NO"),
                    ],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --skip 1", ctx)

        self.assertNotIn("Warning: --skip", stderr.getvalue())

    # ── I/O recovery and resource bounds ──────────────────────────────────

    def test_read_csv_data_closes_file_when_header_parser_fails(self):
        source = io.StringIO("id\n")

        with mock.patch("builtins.open", return_value=source):
            with mock.patch.object(MSSQL, "_iter_csv_records", side_effect=MSSQL.csv.Error("bad header")):
                with self.assertRaises(MSSQL.csv.Error):
                    MSSQL.read_csv_data("rows.csv", False, ",", 0, [])

        self.assertTrue(source.closed)

    def test_close_row_source_on_unstarted_generator_still_closes_file(self):
        # A bare generator's own try/finally only runs once next() has been
        # called at least once; close()ing (or GC'ing) one that never
        # started is a documented no-op that skips the finally entirely.
        # read_csv_data/read_json_data(jsonl) must not depend on that, since
        # import_data's column-validation gate calls _close_row_source
        # before ever iterating the row source when validation fails.
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id,bogus\n1,x\n", encoding="utf-8")
            row_gen, columns = MSSQL.read_csv_data(str(csv_path), False, ",", 0, [])
            MSSQL._close_row_source(row_gen)
            self.assertTrue(row_gen._file.closed)

            jsonl_path = Path(tmpdir) / "rows.jsonl"
            jsonl_path.write_text('{"id": 1, "bogus": "x"}\n', encoding="utf-8")
            row_gen, columns = MSSQL.read_json_data(str(jsonl_path), 0, fmt="jsonl")
            MSSQL._close_row_source(row_gen)
            self.assertTrue(row_gen._file.closed)

    def test_import_column_validation_failure_closes_csv_file_before_iteration(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "users.csv"
            # "bogus" isn't a real column on dbo.users, so import_data's
            # column-validation check must reject this before the row
            # source is ever iterated.
            csv_path.write_text("id,bogus\n1,x\n", encoding="utf-8")

            opened = []
            real_open = open

            def tracking_open(*args, **kwargs):
                fh = real_open(*args, **kwargs)
                if args and str(args[0]) == str(csv_path):
                    opened.append(fh)
                return fh

            with mock.patch("builtins.open", side_effect=tracking_open):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users", ctx)

        self.assertIn("column(s) not found", stderr.getvalue())
        self.assertEqual(len(opened), 1)
        self.assertTrue(opened[0].closed, "CSV file handle leaked past a column-validation failure")

    def test_import_column_validation_failure_closes_jsonl_file_before_iteration(self):
        cursor = FakeCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "users.jsonl"
            jsonl_path.write_text('{"id": 1, "bogus": "x"}\n', encoding="utf-8")

            opened = []
            real_open = open

            def tracking_open(*args, **kwargs):
                fh = real_open(*args, **kwargs)
                if args and str(args[0]) == str(jsonl_path):
                    opened.append(fh)
                return fh

            with mock.patch("builtins.open", side_effect=tracking_open):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.import_data(f"{quote_dot_arg(jsonl_path)} dbo.users", ctx)

        self.assertIn("column(s) not found", stderr.getvalue())
        self.assertEqual(len(opened), 1)
        self.assertTrue(opened[0].closed, "JSONL file handle leaked past a column-validation failure")

    def test_import_skip_does_not_retry_failed_batch_in_explicit_transaction(self):
        class FailingImportCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))
                raise MSSQL.pymssql.Error("batch fail")

        metadata = FailingImportCursor(
            [{"description": [("COLUMN_NAME",)], "rows": [("id", "int", "NO")]}]
        )
        state_cursor = FakeCursor(
            [{"description": [("XACT_STATE",), ("TRANCOUNT",)], "rows": [(-1, 1)]}]
        )

        class ExplicitTransactionConn(FakeConn):
            def __init__(self):
                super().__init__(metadata)
                self.calls = 0

            def cursor(self):
                self.calls += 1
                return metadata if self.calls == 1 else state_cursor

        conn = ExplicitTransactionConn()
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "transaction_open": True,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --on-error skip", ctx)

        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])
        self.assertIn("cannot safely retry", stderr.getvalue())
        attempts = [e for e in metadata.executed if e[0].startswith(("INSERT", "BEGIN TRY"))]
        self.assertEqual(len(attempts), 1)

    def test_resync_transaction_state_tolerates_single_column_cursor(self):
        # The XACT_STATE()/@@TRANCOUNT query always returns 2 columns against
        # a real server; the single-column branch exists only to tolerate
        # minimal test/cursor adapters exposing just @@TRANCOUNT, and had no
        # test driving it directly.
        cursor = FakeCursor([{"description": [("TRANCOUNT",)], "rows": [(3,)]}])
        conn = FakeConn(cursor)
        ctx = {"transaction_depth": 0, "transaction_open": False, "savepoints": {"sp1"}}

        xact_state = MSSQL._resync_transaction_state(conn, ctx)

        self.assertIsNone(xact_state)
        self.assertEqual(ctx["transaction_depth"], 3)
        self.assertTrue(ctx["transaction_open"])
        self.assertEqual(ctx["savepoints"], {"sp1"})

    def test_resync_transaction_state_single_column_zero_clears_savepoints(self):
        cursor = FakeCursor([{"description": [("TRANCOUNT",)], "rows": [(0,)]}])
        conn = FakeConn(cursor)
        ctx = {"transaction_depth": 1, "transaction_open": True, "savepoints": {"sp1"}}

        xact_state = MSSQL._resync_transaction_state(conn, ctx)

        self.assertIsNone(xact_state)
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])
        self.assertEqual(ctx["savepoints"], set())

    def test_execute_sql_reports_rollback_failure_without_masking_query_error(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise MSSQL.pymssql.Error("query lost")

        class RollbackErrorConn(FakeConn):
            def rollback(self):
                raise MSSQL.pymssql.Error("connection gone")

        ctx = {"conn": RollbackErrorConn(ErrorCursor()), "output": io.StringIO()}
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            self.assertFalse(MSSQL.execute_sql("UPDATE t SET x = 1", ctx))

        self.assertFalse(ctx["connection_usable"])
        self.assertIn("Rollback failed after SQL execution error", stderr.getvalue())
        self.assertIn("Error: query lost", stderr.getvalue())

    def test_execute_file_rejects_oversized_input_before_execution(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO()}
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_path = Path(tmpdir) / "large.sql"
            sql_path.write_bytes(b"SELECT")
            with mock.patch.object(MSSQL, "MAX_SQL_INPUT_BYTES", 5):
                with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        ok = MSSQL.execute_file(sql_path, ctx)

        self.assertFalse(ok)
        execute_sql.assert_not_called()
        self.assertIn("exceeds the", stderr.getvalue())

    def test_split_batches_rejects_excessive_go_repeat_count(self):
        with mock.patch.object(MSSQL, "MAX_GO_REPEAT_COUNT", 2):
            with self.assertRaisesRegex(ValueError, "repeat count 3 exceeds"):
                MSSQL.split_batches("SELECT 1\nGO 3")

    # ── Auto-commit for EXEC batches ──────────────────────────────────────

    def test_starts_with_data_change_statement_detects_exec_forms(self):
        self.assertTrue(MSSQL._starts_with_data_change_statement("EXEC dbo.p"))
        self.assertTrue(MSSQL._starts_with_data_change_statement("EXECUTE dbo.p @x = 1"))
        self.assertFalse(MSSQL._starts_with_data_change_statement("SELECT 'EXEC dbo.p'"))

    def test_execute_sql_commits_exec_that_returns_result_set(self):
        # A proc doing DML with SET NOCOUNT ON and a final SELECT yields only
        # a result set; its writes must still be committed.
        cursor = FakeCursor([{"description": [("id",)], "rows": [(1,)]}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        self.assertTrue(MSSQL.execute_sql("EXEC dbo.ArchiveOrders", ctx))

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)

    def test_execute_sql_commits_before_reraising_broken_pipe_from_result_output(self):
        # A batch with DML followed by a SELECT whose printed output hits a
        # broken pipe (e.g. `| head`) must still commit the DML — the
        # statements already ran server-side, and closing the connection
        # without committing would silently roll them back.
        class BrokenAfterFirstWrite:
            def __init__(self):
                self.calls = 0

            def write(self, s):
                self.calls += 1
                if self.calls > 1:
                    raise BrokenPipeError("simulated broken pipe")

            def flush(self):
                pass

        cursor = FakeCursor([
            {"description": None, "rowcount": 3},
            {"description": [("id",)], "rows": [(1,)]},
        ])
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": BrokenAfterFirstWrite(), "savepoints": set()}

        # fake_pymssql.Error is the builtin Exception (see load_mssql_module),
        # which would make `except DBError` swallow the re-raised
        # BrokenPipeError too and mask exactly the bug this test guards
        # against. Real pymssql.Error/pyodbc.Error are unrelated to OSError,
        # so narrow DBError to something realistic for this assertion.
        with mock.patch.object(MSSQL, "DBError", (FakePyodbcError,)):
            with self.assertRaises(BrokenPipeError):
                MSSQL.execute_sql("UPDATE t SET x = 1; SELECT id FROM t", ctx)

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(cursor.close_count, 1)

    def test_execute_sql_does_not_commit_exec_inside_explicit_transaction(self):
        cursor = FakeCursor([{"description": [("id",)], "rows": [(1,)]}])
        conn = FakeConn(cursor)
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 1,
            "savepoints": set(),
        }

        self.assertTrue(MSSQL.execute_sql("EXEC dbo.ArchiveOrders", ctx))

        self.assertEqual(conn.commit_count, 0)

    def test_export_still_rejects_exec_query(self):
        cursor = FakeCursor([{"description": [("id",)], "rows": []}])
        conn = FakeConn(cursor)
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.export_data(f'- {quote_dot_arg(csv_path)} --format csv --query "EXEC dbo.p"', ctx)
            self.assertFalse(csv_path.exists())

        self.assertEqual(cursor.executed, [])
        self.assertIn("--query must be a read-only SELECT statement", stderr.getvalue())

    # ── REPL survives Ctrl-C during execution ─────────────────────────────

    def test_interactive_loop_survives_keyboard_interrupt_during_query(self):
        conn = FreshCursorConn(
            lambda: [
                {"description": [("XACT_STATE",), ("TRANCOUNT",)], "rows": [(1, 2)]}
            ]
        )
        ctx = {
            "conn": conn,
            "output": io.StringIO(),
            "transaction_depth": 0,
            "savepoints": set(),
        }
        inputs = iter(["UPDATE t SET x = 1;", EOFError()])

        def fake_input(prompt):
            value = next(inputs)
            if isinstance(value, BaseException):
                raise value
            return value

        with mock.patch("builtins.input", side_effect=fake_input):
            with mock.patch.object(
                MSSQL, "execute_sql", side_effect=KeyboardInterrupt
            ) as execute_sql:
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.interactive_loop(ctx)

        execute_sql.assert_called_once()
        self.assertIn("Interrupted.", stderr.getvalue())
        # Transaction tracking is resynced from the server, not guessed.
        self.assertEqual(ctx["transaction_depth"], 2)
        self.assertTrue(ctx["transaction_open"])

    def test_interactive_loop_survives_keyboard_interrupt_during_go_batch(self):
        conn = FreshCursorConn(
            lambda: [
                {"description": [("XACT_STATE",), ("TRANCOUNT",)], "rows": [(0, 0)]}
            ]
        )
        ctx = {"conn": conn, "output": io.StringIO()}
        inputs = iter(["SELECT 1", "GO 3", EOFError()])

        def fake_input(prompt):
            value = next(inputs)
            if isinstance(value, BaseException):
                raise value
            return value

        with mock.patch("builtins.input", side_effect=fake_input):
            with mock.patch.object(
                MSSQL, "execute_sql", side_effect=KeyboardInterrupt
            ) as execute_sql:
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.interactive_loop(ctx)

        execute_sql.assert_called_once()
        self.assertIn("Interrupted.", stderr.getvalue())

    def test_interactive_loop_survives_keyboard_interrupt_during_dot_command(self):
        conn = FreshCursorConn(
            lambda: [
                {"description": [("XACT_STATE",), ("TRANCOUNT",)], "rows": [(0, 0)]}
            ]
        )
        ctx = {"conn": conn, "output": io.StringIO()}
        inputs = iter([".export dbo.big out.csv", EOFError()])

        def fake_input(prompt):
            value = next(inputs)
            if isinstance(value, BaseException):
                raise value
            return value

        with mock.patch("builtins.input", side_effect=fake_input):
            with mock.patch.object(
                MSSQL, "run_dot_command", side_effect=KeyboardInterrupt
            ):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.interactive_loop(ctx)

        self.assertIn("Interrupted.", stderr.getvalue())

    # ── Export temp-file cleanup on uncaught exceptions ───────────────────

    def test_export_removes_temp_file_on_keyboard_interrupt(self):
        class InterruptCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("id",)], "rows": []}])

            def fetchmany(self, size):
                raise KeyboardInterrupt()

        conn = FakeConn(InterruptCursor())
        ctx = {"conn": conn}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "out.csv"
            csv_path.write_text("old\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                with self.assertRaises(KeyboardInterrupt):
                    MSSQL.export_data(f"dbo.users {quote_dot_arg(csv_path)} --format csv", ctx)

            # Existing file untouched, and no stray .tmp file left behind.
            self.assertEqual(csv_path.read_text(encoding="utf-8"), "old\n")
            self.assertEqual(os.listdir(tmpdir), ["out.csv"])

    # ── Streaming output for row-independent modes ────────────────────────

    def test_execute_sql_streams_csv_mode_in_chunks_without_fetchall(self):
        class StreamCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("id",), ("name",)]}])
                self.chunks = [[(1, "Alice")], [(2, "Bob")]]

            def fetchmany(self, size):
                return self.chunks.pop(0) if self.chunks else []

            def fetchall(self):
                raise AssertionError("csv mode must stream, not fetchall")

        out = io.StringIO()
        ctx = {
            "conn": FakeConn(StreamCursor()),
            "output": out,
            "mode": "csv",
            "headers": True,
            "rowcount": True,
        }

        self.assertTrue(MSSQL.execute_sql("SELECT id, name FROM t", ctx))

        # Header once, rows across chunks, row count from the running total.
        self.assertEqual(out.getvalue(), "id,name\r\n1,Alice\n2,Bob\n")

    def test_execute_sql_streaming_insert_mode_matches_non_streamed_output(self):
        class StreamCursor(FakeCursor):
            def __init__(self, chunks):
                super().__init__([{"description": [("id",)]}])
                self.chunks = list(chunks)

            def fetchmany(self, size):
                return self.chunks.pop(0) if self.chunks else []

            def fetchall(self):
                raise AssertionError("insert mode must stream, not fetchall")

        out = io.StringIO()
        ctx = {
            "conn": FakeConn(StreamCursor([[(1,)], [(2,)]])),
            "output": out,
            "mode": "insert",
            "insert_table": "[dbo].[t]",
            "rowcount": True,
        }
        MSSQL.execute_sql("SELECT id FROM t", ctx)
        self.assertEqual(
            out.getvalue(),
            "INSERT INTO [dbo].[t] ([id]) VALUES (1);\n"
            "INSERT INTO [dbo].[t] ([id]) VALUES (2);\n",
        )

        out = io.StringIO()
        ctx = {
            "conn": FakeConn(StreamCursor([])),
            "output": out,
            "mode": "insert",
            "insert_table": "[dbo].[t]",
            "rowcount": True,
        }
        MSSQL.execute_sql("SELECT id FROM t", ctx)
        self.assertEqual(out.getvalue(), "-- (no rows)\n")

    # ── Import error messages carry true source row numbers ───────────────

    def test_import_skip_reports_source_row_numbers_after_validation_skips(self):
        class ImportCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))
                raise MSSQL.pymssql.Error("batch fail")

            def execute(self, sql, params=()):
                self.executed.append((sql, params))
                if sql.startswith("INSERT") and params == ("1",):
                    raise MSSQL.pymssql.Error("row fail")

        cursor = ImportCursor(
            [
                {
                    "description": [
                        ("COLUMN_NAME",),
                        ("DATA_TYPE",),
                        ("IS_NULLABLE",),
                    ],
                    "rows": [("id", "int", "NO")],
                }
            ]
        )
        conn = FakeConn(cursor)
        ctx = {"conn": conn, "output": io.StringIO()}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            # Row 2 is validation-skipped (wrong column count), so the DB
            # error on row 1 must not be misattributed to another row.
            csv_path.write_text("id\n1\n2,extra\n3\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} dbo.users --on-error skip", ctx)

        self.assertIn("Skipped row 1: row fail", stderr.getvalue())
        self.assertNotIn("Skipped row 2: row fail", stderr.getvalue())
        self.assertIn("Row 2: expected 1 columns from CSV header, got 2", stderr.getvalue())


    # ── MySQL (--mysql, pymysql) ───────────────────────────────────────────

    def _mysql_args(self, **overrides):
        values = dict(
            host="", port=0, user="", password=None, database="",
            timeout=5, query_timeout=17, mysql=True, trusted_connection=False,
        )
        values.update(overrides)
        return types.SimpleNamespace(**values)

    def test_connect_mysql_builds_expected_connection_and_metadata(self):
        env = {
            "MYSQL_HOST": "my.example", "MYSQL_USER": "app",
            "MYSQL_PASSWORD": "pw", "MYSQL_DATABASE": "appdb",
        }
        conn = object()
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(MSSQL.pymysql, "connect", return_value=conn) as connect:
                result, conn_info, driver = MSSQL._connect(self._mysql_args())

        self.assertIs(result, conn)
        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs["host"], "my.example")
        self.assertEqual(kwargs["port"], 3306)
        self.assertEqual(kwargs["user"], "app")
        self.assertEqual(kwargs["password"], b"pw")
        self.assertEqual(kwargs["database"], "appdb")
        self.assertEqual(kwargs["connect_timeout"], 5)
        self.assertEqual(kwargs["read_timeout"], 17)
        self.assertEqual(kwargs["charset"], "utf8mb4")
        self.assertIs(kwargs["autocommit"], True)
        self.assertTrue(kwargs["client_flag"] & MSSQL.MYSQL_CLIENT_MULTI_STATEMENTS)
        self.assertEqual(conn_info, "app@my.example:3306/appdb")
        self.assertEqual(
            driver,
            {"name": "pymysql", "version": "test-pymysql", "paramstyle": "format", "dialect": "mysql"},
        )

    def test_connect_mysql_does_not_fall_back_to_mssql_env_vars(self):
        env = {"MSSQL_USER": "sa", "MSSQL_PASSWORD": "mssql-secret"}
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(MSSQL.pymysql, "connect") as connect:
                with self.assertRaisesRegex(ValueError, "MYSQL_USER"):
                    MSSQL._connect(self._mysql_args())
        connect.assert_not_called()

    def test_connect_mysql_rejects_invalid_env_port(self):
        with mock.patch.dict(os.environ, {"MYSQL_PORT": "abc"}, clear=True):
            with self.assertRaisesRegex(ValueError, "invalid MYSQL_PORT 'abc'"):
                MSSQL._connect(self._mysql_args(user="app"))

    def test_connect_mysql_requires_pymysql_installed(self):
        with mock.patch.object(MSSQL, "pymysql", None):
            with self.assertRaisesRegex(ValueError, "pymysql is not installed"):
                MSSQL._connect(self._mysql_args(user="app"))

    def test_connect_sql_auth_requires_pymssql_installed(self):
        args = self._mysql_args(user="sa", mysql=False)
        with mock.patch.object(MSSQL, "pymssql", None):
            with self.assertRaisesRegex(ValueError, "pymssql is not installed"):
                MSSQL._connect(args)

    def test_db_error_catches_pymysql_errors(self):
        self.assertIn(FakePymysqlError, MSSQL.DBError)

    def test_main_rejects_mysql_combined_with_trusted_connection(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "--mysql", "-E"]):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 2)
        build_connection.assert_not_called()
        self.assertIn("--mysql cannot be combined", stderr.getvalue())

    def test_main_mysql_sets_dialect_and_backtick_quotes_insert_table(self):
        cursor = FakeCursor([{"description": [("x",)], "rows": [("C:\\tmp",)]}])
        cursor.fetchmany = lambda size, _rows=[[("C:\\tmp",)], []]: _rows.pop(0)
        conn = FakeConn(cursor)
        conn.server_status = 0
        argv = [
            "mssql.py", "--mysql", "-U", "app", "-c", "SELECT x",
            "--mode", "insert", "--insert-table", "db.t",
        ]
        with mock.patch.object(MSSQL.pymysql, "connect", return_value=conn):
            with mock.patch("sys.argv", argv):
                with mock.patch("sys.stdout", new=io.StringIO()) as stdout:
                    with mock.patch("sys.stderr", new=io.StringIO()):
                        MSSQL.main()

        self.assertIn("INSERT INTO `db`.`t` (`x`) VALUES ('C:\\\\tmp');", stdout.getvalue())

    def test_mysql_identifier_helpers_use_backticks(self):
        self.assertEqual(MSSQL._bracket_table("db.`we``ird`", "mysql"), "`db`.`we``ird`")
        self.assertEqual(MSSQL._quote_identifier_part("a`b", "mysql"), "`a``b`")
        self.assertEqual(MSSQL._qualify_table_parts("t", "dbo", "mysql"), (None, None, "t"))
        self.assertEqual(MSSQL._qualify_table_parts("`my.db`.t", "dbo", "mysql"), (None, "my.db", "t"))
        self.assertEqual(MSSQL._table_ref(None, None, "t", "mysql"), "`t`")
        with self.assertRaisesRegex(ValueError, "Use table or database.table"):
            MSSQL._qualify_table_parts("a.b.c", dialect="mysql")
        self.assertEqual(MSSQL._quote_column_list("a, `b``c`", "mysql"), "`a`, `b``c`")

    def test_sql_literal_mysql_escapes_backslashes_and_empty_binary(self):
        self.assertEqual(MSSQL.sql_literal("C:\\x'y", "mysql"), "'C:\\\\x''y'")
        self.assertEqual(MSSQL.sql_literal("a\x00b", "mysql"), "'a\\0b'")
        self.assertEqual(MSSQL.sql_literal(b"", "mysql"), "X''")
        self.assertEqual(MSSQL.sql_literal(b"\x00\xff", "mysql"), "X'00ff'")
        # SQL Server output is unchanged.
        self.assertEqual(MSSQL.sql_literal("C:\\x'y"), "N'C:\\x''y'")
        self.assertEqual(MSSQL.sql_literal(b"\x00\xff"), "0x00ff")

    def test_mysql_time_values_render_as_hh_mm_ss(self):
        import datetime
        neg = -datetime.timedelta(hours=30, minutes=15)
        frac = datetime.timedelta(hours=1, seconds=3, microseconds=500)
        self.assertEqual(MSSQL.sql_literal(neg, "mysql"), "'-30:15:00'")
        self.assertEqual(MSSQL.sql_literal(frac, "mysql"), "'01:00:03.000500'")
        self.assertIn("| -30:15:00 |", MSSQL.format_table([(neg,)], ["d"]))
        self.assertEqual(MSSQL.format_csv([(neg,)], ["d"]), "d\r\n-30:15:00")
        self.assertIn('"d": "-30:15:00"', MSSQL.format_json([(neg,)], ["d"]))
        self.assertEqual(MSSQL.format_line([(neg,)], ["d"]), "d=-30:15:00")

    def test_split_batches_mysql_honors_backslash_escaped_quote(self):
        sql = "SELECT 'it\\'s'\nGO\nSELECT 2"
        self.assertEqual(MSSQL.split_batches(sql, "mysql"), ["SELECT 'it\\'s'", "SELECT 2"])
        # T-SQL has no backslash escapes: the same text leaves a string open,
        # so GO is (correctly, for SQL Server) treated as string content.
        self.assertEqual(MSSQL.split_batches(sql), [sql])

    def test_scan_sql_line_state_mysql_comment_rules(self):
        scan = MSSQL._scan_sql_line_state
        # "--" needs trailing whitespace in MySQL; 1--1 is arithmetic.
        self.assertEqual(scan("SELECT 1--1;", dialect="mysql")[4], ";")
        self.assertEqual(scan("SELECT 1--1;")[4], "1")
        self.assertEqual(scan("SELECT 1 -- c;", dialect="mysql")[4], "1")
        self.assertEqual(scan("SELECT 1 # c;", dialect="mysql")[4], "1")
        # Backticks quote identifiers; brackets don't.
        self.assertNotIn(";", scan("SELECT `a;b`", dialect="mysql")[3])
        self.assertIn(";", scan("SELECT [a;b]", dialect="mysql")[3])
        # MySQL block comments don't nest: the first */ closes the comment.
        self.assertEqual(scan("/* /* */ SELECT 1;", dialect="mysql")[0], 0)
        self.assertEqual(scan("/* /* */ SELECT 1;")[0], 1)

    def test_interactive_loop_mysql_ignores_semicolons_in_escaped_strings_and_comments(self):
        ctx = {"conn": FakeConn(), "output": io.StringIO(), "dialect": "mysql"}
        lines = iter(["SELECT 'it\\'s;' # c;", "  ;"])

        with mock.patch("builtins.input", side_effect=lambda prompt: next(lines)):
            with mock.patch.object(MSSQL, "execute_sql") as execute_sql:
                with mock.patch("sys.stdout", new=io.StringIO()) as stdout:
                    with self.assertRaises(StopIteration):
                        MSSQL.interactive_loop(ctx)

        execute_sql.assert_called_once_with("SELECT 'it\\'s;' # c;\n  ;", ctx)
        self.assertIn("MySQL Query Tool", stdout.getvalue())

    def test_execute_sql_mysql_tracks_transaction_from_server_status(self):
        conn = FakeConn(FakeCursor([{"description": None, "rowcount": 0}]))
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql", "savepoints": {"sp"}}

        conn.server_status = MSSQL.MYSQL_SERVER_STATUS_IN_TRANS
        self.assertTrue(MSSQL.execute_sql("START TRANSACTION", ctx))
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(ctx["transaction_open"])

        conn._cursor = FakeCursor([{"description": None, "rowcount": 0}])
        conn.server_status = 0  # e.g. DDL implicitly committed
        self.assertTrue(MSSQL.execute_sql("CREATE TABLE t (x INT)", ctx))
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])
        self.assertEqual(ctx["savepoints"], set())
        # T-SQL lexical tracking isn't applied: the server flag wins.
        conn._cursor = FakeCursor([{"description": None, "rowcount": 0}])
        self.assertTrue(MSSQL.execute_sql("BEGIN TRANSACTION", ctx))
        self.assertEqual(ctx["transaction_depth"], 0)

    def test_execute_sql_mysql_error_in_transaction_resyncs_with_fresh_status(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise FakePymysqlError("deadlock")

        class Conn(FakeConn):
            server_status = MSSQL.MYSQL_SERVER_STATUS_IN_TRANS

            def __init__(self):
                super().__init__(cursor=None)
                self.calls = 0

            def cursor(self):
                self.calls += 1
                if self.calls == 1:
                    return ErrorCursor()
                conn = self

                class SyncCursor(FakeCursor):
                    def execute(self, sql, params=()):
                        super().execute(sql, params)
                        conn.server_status = 0  # deadlock rolled the txn back

                self.sync_cursor = SyncCursor([{"description": [("1",)], "rows": [(1,)]}])
                return self.sync_cursor

        conn = Conn()
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql",
               "transaction_depth": 1, "transaction_open": True}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            ok = MSSQL.execute_sql("UPDATE t SET x = 1", ctx)

        self.assertFalse(ok)
        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(conn.sync_cursor.executed, [("DO 0", ())])
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertFalse(ctx["transaction_open"])
        self.assertIn("deadlock", stderr.getvalue())

    def _failing_second_statement_cursor(self, dialect_error):
        class SecondStatementFails(FakeCursor):
            def nextset(self):
                raise dialect_error("Table 'appdb.nope' doesn't exist")

        return SecondStatementFails([{"description": None, "rowcount": 1}])

    def test_execute_sql_mysql_warns_earlier_batch_statements_stay_committed(self):
        # Server-side autocommit already committed the first INSERT; the
        # rollback can't undo it, so the user must be told.
        conn = FakeConn(self._failing_second_statement_cursor(FakePymysqlError))
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql"}

        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            ok = MSSQL.execute_sql("INSERT INTO a VALUES (1); INSERT INTO nope VALUES (2)", ctx)

        self.assertFalse(ok)
        self.assertIn("already committed (MySQL autocommit)", stderr.getvalue())
        self.assertIn("(1 row affected)", stderr.getvalue())
        self.assertEqual(ctx["output"].getvalue(), "")
        self.assertEqual(conn.rollback_count, 1)

    def test_execute_sql_partial_commit_warning_only_when_it_applies(self):
        class FirstStatementFails(FakeCursor):
            def execute(self, sql, params=()):
                raise FakePymysqlError("syntax error")

        cases = (
            # Nothing completed before the error.
            ("mysql", FakeConn(FirstStatementFails()), {}),
            # Inside START TRANSACTION nothing was committed.
            ("mysql", FakeConn(self._failing_second_statement_cursor(FakePymysqlError)),
             {"transaction_depth": 1, "transaction_open": True}),
            # SQL Server rolls the whole batch back.
            ("mssql", FakeConn(self._failing_second_statement_cursor(FakePymssqlError)), {}),
        )
        for dialect, conn, extra in cases:
            with self.subTest(dialect=dialect, extra=extra):
                conn.server_status = MSSQL.MYSQL_SERVER_STATUS_IN_TRANS
                ctx = {"conn": conn, "output": io.StringIO(), "dialect": dialect, **extra}
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    self.assertFalse(MSSQL.execute_sql("INSERT INTO a VALUES (1)", ctx))
                self.assertNotIn("already committed", stderr.getvalue())

    def _lost_connection_conn(self):
        class TimedOutCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise FakePymysqlError(
                    MSSQL.MYSQL_CR_SERVER_LOST,
                    "Lost connection to MySQL server during query (timed out)",
                )

        class DroppedConn(FakeConn):
            def thread_id(self):
                return 42

            def rollback(self):
                raise FakePymysqlError(0, "")

        return DroppedConn(TimedOutCursor())

    def test_execute_sql_mysql_lost_connection_kills_query_on_server(self):
        # A read timeout only closes pymysql's socket; without KILL QUERY the
        # statement keeps running server-side and commits under autocommit.
        conn = self._lost_connection_conn()
        side_cursor = FakeCursor()
        side_conn = FakeConn(side_cursor)
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql",
               "conn_args": self._mysql_args(user="app")}

        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(MSSQL.pymysql, "connect", return_value=side_conn):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    ok = MSSQL.execute_sql("UPDATE big SET x = 1", ctx)

        self.assertFalse(ok)
        self.assertEqual(side_cursor.executed, [("KILL QUERY %s", (42,))])
        self.assertTrue(side_conn.closed)
        self.assertIn("sent KILL QUERY", stderr.getvalue())
        self.assertIn("Reconnect before issuing more SQL", stderr.getvalue())

    def test_execute_sql_mysql_lost_connection_warns_when_kill_fails(self):
        conn = self._lost_connection_conn()
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql",
               "conn_args": self._mysql_args(user="app")}

        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(
                MSSQL.pymysql, "connect", side_effect=FakePymysqlError(2003, "unreachable")
            ):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.execute_sql("UPDATE big SET x = 1", ctx)

        self.assertIn("may still be running on the server and may commit", stderr.getvalue())

    def test_execute_sql_mysql_other_errors_do_not_kill_query(self):
        class ErrorCursor(FakeCursor):
            def execute(self, sql, params=()):
                raise FakePymysqlError(1146, "Table doesn't exist")

        conn = FakeConn(ErrorCursor())
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql",
               "conn_args": self._mysql_args(user="app")}
        with mock.patch.object(MSSQL.pymysql, "connect") as connect:
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.execute_sql("SELECT * FROM nope", ctx)

        connect.assert_not_called()

    def test_run_dot_command_mysql_listing_commands_use_mysql_metadata(self):
        for cmd, needle in (
            (".databases", "INFORMATION_SCHEMA.SCHEMATA"),
            (".tables", "TABLE_SCHEMA = DATABASE()"),
            (".views", "INFORMATION_SCHEMA.VIEWS"),
            (".procs", "ROUTINE_SCHEMA = DATABASE()"),
        ):
            with self.subTest(cmd=cmd):
                cursor = FakeCursor([{"description": [("x",)], "rows": []}])
                ctx = {"conn": FakeConn(cursor), "output": io.StringIO(), "dialect": "mysql"}
                MSSQL.run_dot_command(cmd, ctx)
                sql, params = cursor.executed[0]
                self.assertIn(needle, sql)
                self.assertNotIn("sys.databases", sql)
                self.assertNotIn(MSSQL.PARAM_PLACEHOLDER, sql)
                if cmd != ".databases":
                    self.assertEqual(params, ("%", ""))

    def test_run_dot_command_mysql_schema_resolves_bare_and_qualified_names(self):
        rows = [("appdb", "users", "id", "int", None, "NO", None, 1)]
        description = [(c,) for c in (
            "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH", "IS_NULLABLE", "COLUMN_DEFAULT", "ORDINAL_POSITION",
        )]

        cursor = FakeCursor([{"description": description, "rows": rows}])
        ctx = {"conn": FakeConn(cursor), "output": io.StringIO(), "dialect": "mysql"}
        MSSQL.run_dot_command(".schema users", ctx)
        sql, params = cursor.executed[0]
        self.assertIn("TABLE_SCHEMA = DATABASE()", sql)
        self.assertEqual(params, ("users",))

        cursor = FakeCursor([{"description": description, "rows": rows}])
        ctx = {"conn": FakeConn(cursor), "output": io.StringIO(), "dialect": "mysql"}
        MSSQL.run_dot_command(".schema `appdb`.users", ctx)
        sql, params = cursor.executed[0]
        self.assertIn("FROM INFORMATION_SCHEMA.COLUMNS", sql)
        self.assertEqual(params, ("users", "appdb"))

        cursor = FakeCursor([{"description": description, "rows": rows}])
        ctx = {"conn": FakeConn(cursor), "output": io.StringIO(), "dialect": "mysql"}
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.run_dot_command(".schema a.b.c", ctx)
        self.assertEqual(cursor.executed, [])
        self.assertIn("Usage: .schema", stderr.getvalue())

    def _mysql_import_conn(self, columns=(("id", "int", "NO"),), fk_refs=0, autocommit=True):
        class AutocommitConn(FakeConn):
            def __init__(self, cursor):
                super().__init__(cursor)
                self.autocommit_calls = []
                self.server_status = 2 if autocommit else 0

            def autocommit(self, value):
                self.autocommit_calls.append(value)

            def get_autocommit(self):
                return autocommit

        class ImportCursor(FakeCursor):
            """Answers the foreign-key check separately from column metadata."""

            def fetchall(self):
                if "REFERENTIAL_CONSTRAINTS" in self.executed[-1][0]:
                    return [(fk_refs,)]
                return super().fetchall()

        cursor = ImportCursor([{
            "description": [("COLUMN_NAME",), ("DATA_TYPE",), ("IS_NULLABLE",)],
            "rows": list(columns),
        }])
        return cursor, AutocommitConn(cursor)

    def test_import_mysql_uses_delete_backticks_and_suspends_autocommit(self):
        cursor, conn = self._mysql_import_conn()
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql", "paramstyle": "format"}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} users --truncate", ctx)

        self.assertEqual(cursor.executed[0], ("DO 0", ()))
        meta_sql, meta_params = cursor.executed[1]
        self.assertIn("TABLE_SCHEMA = DATABASE()", meta_sql)
        self.assertEqual(meta_params, ("users",))
        executed_sql = [sql for sql, _ in cursor.executed]
        self.assertIn("DELETE FROM `users`", executed_sql)
        self.assertNotIn("TRUNCATE TABLE `users`", executed_sql)
        self.assertIn(("INSERT INTO `users` (`id`) VALUES (%s)", [("1",)]), cursor.executed)
        self.assertEqual(conn.autocommit_calls, [False, True])
        self.assertEqual(conn.commit_count, 1)
        # Pending work is rolled back (not implicitly committed) before
        # autocommit is turned back on.
        self.assertEqual(conn.rollback_count, 1)
        self.assertIn("Truncated `users`", stderr.getvalue())

    def _run_mysql_import(self, conn, arg, csv_text="id\n1\n", **ctx_extra):
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql", "paramstyle": "format"}
        ctx.update(ctx_extra)
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text(csv_text, encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} {arg}", ctx)
        return stderr.getvalue()

    def test_import_mysql_truncate_checks_foreign_keys_in_target_database(self):
        cursor, conn = self._mysql_import_conn()
        self._run_mysql_import(conn, "appdb.users --truncate")

        fk_sql, fk_params = cursor.executed[2]
        self.assertIn("INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS", fk_sql)
        self.assertIn("@@foreign_key_checks = 1", fk_sql)
        self.assertEqual(fk_params, ("users", "appdb"))
        self.assertIn("DELETE FROM `appdb`.`users`", [sql for sql, _ in cursor.executed])

    def test_import_mysql_truncate_refuses_table_referenced_by_foreign_key(self):
        # DELETE FROM would apply the child table's ON DELETE CASCADE / SET
        # NULL rule; TRUNCATE TABLE (on either server) refuses instead.
        cursor, conn = self._mysql_import_conn(fk_refs=1)
        stderr = self._run_mysql_import(conn, "customers --truncate")

        self.assertIn("cannot --truncate `customers`", stderr)
        self.assertIn("FOREIGN KEY", stderr)
        executed_sql = [sql for sql, _ in cursor.executed]
        self.assertFalse(any(sql.startswith(("DELETE", "INSERT")) for sql in executed_sql))
        self.assertEqual(conn.commit_count, 0)
        # The suspended autocommit is still restored on the early return.
        self.assertEqual(conn.autocommit_calls, [False, True])

    def test_import_mysql_without_truncate_skips_foreign_key_check(self):
        cursor, conn = self._mysql_import_conn(fk_refs=1)
        self._run_mysql_import(conn, "users")

        executed_sql = [sql for sql, _ in cursor.executed]
        self.assertFalse(any("REFERENTIAL_CONSTRAINTS" in sql for sql in executed_sql))
        self.assertIn(("INSERT INTO `users` (`id`) VALUES (%s)", [("1",)]), cursor.executed)

    def test_import_mysql_doubles_percent_in_identifiers_for_pymysql(self):
        # pymysql's executemany formats the INSERT prefix with `% ()`, so an
        # undoubled % in a quoted identifier raised TypeError and killed the REPL.
        cursor, conn = self._mysql_import_conn(columns=[("pct%", "int", "NO")])
        self._run_mysql_import(conn, "`rate%s`", csv_text="pct%\n5\n")

        insert_sql, rows = cursor.executed[-1]
        self.assertEqual(insert_sql, "INSERT INTO `rate%%s` (`pct%%`) VALUES (%s)")
        self.assertEqual(rows, [("5",)])
        self.assertEqual(insert_sql.split("VALUES")[0] % (), "INSERT INTO `rate%s` (`pct%`) ")
        self.assertEqual(cursor.executed[1][1], ("rate%s",))

    def test_adapt_placeholders_only_doubles_percent_for_mysql(self):
        sql = f"SELECT `a%` FROM t WHERE x = {MSSQL.PARAM_PLACEHOLDER}"
        self.assertEqual(
            MSSQL._adapt_placeholders(sql, {"dialect": "mysql", "paramstyle": "format"}),
            "SELECT `a%%` FROM t WHERE x = %s",
        )
        self.assertEqual(
            MSSQL._adapt_placeholders(sql, {"paramstyle": "format"}),
            "SELECT `a%` FROM t WHERE x = %s",
        )
        self.assertEqual(
            MSSQL._adapt_placeholders(sql, {"paramstyle": "qmark"}),
            "SELECT `a%` FROM t WHERE x = ?",
        )

    def test_import_mysql_leaves_user_disabled_autocommit_off(self):
        # After SET autocommit = 0 (and nothing pending yet), .import must not
        # turn autocommit back on behind the user's back.
        cursor, conn = self._mysql_import_conn(autocommit=False)
        self._run_mysql_import(conn, "users")

        self.assertEqual(conn.autocommit_calls, [])
        self.assertIn(("INSERT INTO `users` (`id`) VALUES (%s)", [("1",)]), cursor.executed)
        self.assertEqual(conn.commit_count, 1)

    def test_import_unknown_column_error_names_table_in_active_dialect(self):
        for dialect, table, expected in (
            ("mysql", "users", "column(s) not found in `users`: ['nope']"),
            ("mssql", "dbo.users", "column(s) not found in [dbo].[users]: ['nope']"),
        ):
            with self.subTest(dialect=dialect):
                cursor, conn = self._mysql_import_conn()
                ctx = {"conn": conn, "output": io.StringIO(), "dialect": dialect, "paramstyle": "format"}

                with tempfile.TemporaryDirectory() as tmpdir:
                    csv_path = Path(tmpdir) / "rows.csv"
                    csv_path.write_text("nope\n1\n", encoding="utf-8")
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        MSSQL.import_data(f"{quote_dot_arg(csv_path)} {table}", ctx)

                self.assertIn(expected, stderr.getvalue())
                self.assertNotIn("None", stderr.getvalue())
                self.assertEqual(len(cursor.executed), 2 if dialect == "mysql" else 1)

    def test_import_mysql_inside_open_transaction_leaves_autocommit_alone(self):
        cursor, conn = self._mysql_import_conn()
        conn.server_status = 3  # explicit transaction and autocommit enabled
        ctx = {"conn": conn, "output": io.StringIO(), "dialect": "mysql",
               "paramstyle": "format", "transaction_depth": 1}

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "rows.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.import_data(f"{quote_dot_arg(csv_path)} appdb.users", ctx)

        self.assertEqual(cursor.executed[1][1], ("users", "appdb"))
        self.assertEqual(conn.autocommit_calls, [])
        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(conn.rollback_count, 0)

    def test_export_mysql_quotes_table_and_columns_with_backticks(self):
        cursor = FakeCursor([{"description": [("a",)], "rows": []}])
        ctx = {"conn": FakeConn(cursor), "output": io.StringIO(), "dialect": "mysql"}

        with mock.patch("sys.stdout", new=io.StringIO()):
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.export_data("appdb.users - --format csv --columns a,b", ctx)

        self.assertEqual(cursor.executed[0][0], "SELECT `a`, `b` FROM `appdb`.`users`")

    # ── --env-file ─────────────────────────────────────────────────────────

    def _write_env_file(self, tmpdir, text):
        path = Path(tmpdir) / "test.env"
        path.write_text(text, encoding="utf-8")
        return path

    def test_load_env_file_parses_dotenv_syntax(self):
        text = (
            "# comment\n"
            "\n"
            "DB_HOST=db.example\n"
            "export DB_USER = app\n"
            "DB_PASSWORD='p#ss word'  # trailing comment\n"
            'DB_NAME="appdb"\n'
            "DB_PORT=3307 # inline comment\n"
            "EMPTY=\n"
            "HASHED=a#b\n"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            values = MSSQL.load_env_file(self._write_env_file(tmpdir, text))

        self.assertEqual(values, {
            "DB_HOST": "db.example", "DB_USER": "app", "DB_PASSWORD": "p#ss word",
            "DB_NAME": "appdb", "DB_PORT": "3307", "EMPTY": "", "HASHED": "a#b",
        })

    def test_load_env_file_errors_cite_line_number_without_content(self):
        for text, message in (
            ("DB_USER=x\nsecret-value-no-equals\n", ":2: expected KEY=VALUE"),
            ("DB_PASSWORD='secret-unterminated\n", ":1: unterminated quoted value"),
            ("BAD KEY=secret-value\n", ":1: expected KEY=VALUE"),
        ):
            with self.subTest(text=text):
                with tempfile.TemporaryDirectory() as tmpdir:
                    path = self._write_env_file(tmpdir, text)
                    with self.assertRaises(ValueError) as exc:
                        MSSQL.load_env_file(path)
                self.assertIn(message, str(exc.exception))
                self.assertNotIn("secret", str(exc.exception))

    def test_resolve_env_file_values_maps_generic_names_to_active_backend(self):
        file_values = {
            "DB_HOST": "generic-host", "DB_USER": "generic-user", "DB_NAME": "appdb",
            "MYSQL_HOST": "specific-host", "MSSQL_USER": "sa", "DB_ROOT_PASSWORD": "root",
        }
        self.assertEqual(
            MSSQL._resolve_env_file_values(file_values, "MYSQL"),
            {"MYSQL_HOST": "specific-host", "MYSQL_USER": "generic-user", "MYSQL_DATABASE": "appdb"},
        )
        self.assertEqual(
            MSSQL._resolve_env_file_values(file_values, "MSSQL"),
            {"MSSQL_HOST": "generic-host", "MSSQL_USER": "sa", "MSSQL_DATABASE": "appdb"},
        )

    def test_connect_precedence_is_flags_then_env_file_then_environment(self):
        args = self._mysql_args(
            host="flag-host",
            env_file_values={"MYSQL_HOST": "file-host", "MYSQL_USER": "file-user", "MYSQL_PORT": "3310"},
        )
        env = {"MYSQL_HOST": "env-host", "MYSQL_USER": "env-user", "MYSQL_PASSWORD": "env-pw"}
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(MSSQL.pymysql, "connect") as connect:
                MSSQL._connect(args)

        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs["host"], "flag-host")
        self.assertEqual(kwargs["user"], "file-user")
        self.assertEqual(kwargs["port"], 3310)
        self.assertEqual(kwargs["password"], b"env-pw")

    def test_main_env_file_supplies_mssql_connection_settings(self):
        cursor = FakeCursor([{"description": None, "rowcount": 0}])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_env_file(tmpdir, "DB_HOST=sqlhost\nDB_USER=sa\nDB_PASSWORD=pw\n")
            argv = ["mssql.py", "--env-file", str(path), "-c", "SELECT 1"]
            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch.object(MSSQL.pymssql, "connect", return_value=FakeConn(cursor)) as connect:
                    with mock.patch("sys.argv", argv):
                        with mock.patch("sys.stdout", new=io.StringIO()):
                            MSSQL.main()

        kwargs = connect.call_args.kwargs
        self.assertEqual((kwargs["server"], kwargs["user"], kwargs["password"]), ("sqlhost", "sa", "pw"))

    def test_main_reports_unreadable_env_file_before_connecting(self):
        with mock.patch.object(MSSQL, "build_connection") as build_connection:
            with mock.patch("sys.argv", ["mssql.py", "--mysql", "--env-file", "/nonexistent/x.env"]):
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as exc:
                        MSSQL.main()

        self.assertEqual(exc.exception.code, 1)
        build_connection.assert_not_called()
        self.assertIn("Error reading env file", stderr.getvalue())

    def _transaction_ctx(self, mysql=False, conn=None):
        conn = conn if conn is not None else (
            MysqlTransactionConn() if mysql else PymssqlTransactionConn())
        if mysql:
            MSSQL._install_mysql_status_tracking(conn)
        return {
            "conn": conn,
            "driver_name": "pymysql" if mysql else "pymssql",
            "dialect": "mysql" if mysql else "mssql",
            "paramstyle": "format",
            "transaction_depth": 0,
            "savepoints": set(),
            "output": io.StringIO(),
            "conn_args": self._mysql_args(user="app", mysql=mysql),
        }

    def _transaction_import(self, ctx, text, options=""):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.csv"
            path.write_text(text, encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(path)} t {options}", ctx)
        return stderr.getvalue()

    def test_pymssql_explicit_commits_make_rows_durable(self):
        for statements in (
            ["BEGIN TRAN", "INSERT INTO t VALUES (1)", "COMMIT"],
            ["BEGIN TRAN; INSERT INTO t VALUES (1); COMMIT"],
            ["BEGIN TRAN", "BEGIN TRAN", "INSERT INTO t VALUES (1)", "COMMIT", "COMMIT"],
        ):
            with self.subTest(statements=statements):
                ctx = self._transaction_ctx()
                for sql in statements:
                    self.assertTrue(MSSQL.execute_sql(sql, ctx))
                conn = ctx["conn"]
                self.assertEqual(conn.depth, 0)
                self.assertEqual(ctx["transaction_depth"], 0)
                conn.close()
                self.assertEqual(conn.durable, [1])

    def test_pymssql_inner_commit_leaves_outer_transaction_pending(self):
        ctx = self._transaction_ctx()
        conn = ctx["conn"]
        for sql in ("BEGIN TRAN", "BEGIN TRAN", "INSERT INTO t VALUES (1)", "COMMIT"):
            self.assertTrue(MSSQL.execute_sql(sql, ctx))
        self.assertEqual(conn.depth, 1)
        self.assertEqual(conn.durable, [])
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertTrue(MSSQL.execute_sql("ROLLBACK", ctx))
        self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (2)", ctx))
        self.assertEqual(conn.durable, [2])
        self.assertEqual(conn.depth, 0)

    def test_pymssql_ordinary_batches_are_atomic_and_leave_no_driver_transaction(self):
        ctx = self._transaction_ctx()
        conn = ctx["conn"]
        self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertFalse(MSSQL.execute_sql("INSERT INTO t VALUES (2); FAIL", ctx))
        self.assertEqual(conn.durable, [1])
        self.assertEqual(conn.depth, 0)
        self.assertTrue(conn.autocommit_state)
        self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (3)", ctx))
        self.assertEqual(conn.durable, [1, 3])

    def test_pymssql_idle_interrupt_does_not_create_explicit_transaction(self):
        ctx = self._transaction_ctx()
        with mock.patch("sys.stderr", new=io.StringIO()):
            MSSQL._handle_repl_interrupt(ctx)
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
        self.assertEqual(ctx["conn"].durable, [1])

    def test_pymssql_failed_nested_batch_resyncs_actual_depth(self):
        ctx = self._transaction_ctx()
        MSSQL.execute_sql("BEGIN TRAN", ctx)
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertFalse(MSSQL.execute_sql("BEGIN TRAN; FAIL", ctx))
        self.assertEqual(ctx["transaction_depth"], 2)
        self.assertEqual(ctx["conn"].depth, 2)

    def test_pymssql_failed_batch_that_opens_transaction_rolls_back(self):
        ctx = self._transaction_ctx()
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertFalse(MSSQL.execute_sql("BEGIN TRAN; INSERT INTO t VALUES (1); FAIL", ctx))
        self.assertEqual(ctx["conn"].depth, 0)
        self.assertEqual(ctx["conn"].durable, [])
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (2)", ctx))
        self.assertEqual(ctx["conn"].durable, [2])

    def test_pymssql_import_commits_batches_without_leaving_baseline_transaction(self):
        ctx = self._transaction_ctx()
        self._transaction_import(ctx, "id\n1\n2\n", "--batch 1")
        self.assertEqual(ctx["conn"].durable, [1, 2])
        self.assertEqual(ctx["conn"].commit_count, 2)
        self.assertEqual(ctx["conn"].depth, 0)
        self.assertTrue(ctx["conn"].autocommit_state)

    def test_pymssql_import_joins_explicit_transaction_until_outer_commit(self):
        ctx = self._transaction_ctx()
        MSSQL.execute_sql("BEGIN TRAN", ctx)
        self._transaction_import(ctx, "id\n1\n2\n", "--batch 1")
        self.assertEqual(ctx["conn"].durable, [])
        self.assertEqual(ctx["conn"].pending, [1, 2])
        self.assertEqual(ctx["conn"].commit_count, 0)
        self.assertEqual(ctx["conn"].autocommit_calls, [])
        self.assertTrue(MSSQL.execute_sql("COMMIT", ctx))
        self.assertEqual(ctx["conn"].durable, [1, 2])

    def test_pymssql_failed_truncate_import_restores_original_rows(self):
        ctx = self._transaction_ctx()
        ctx["conn"].durable = [99]
        stderr = self._transaction_import(ctx, "id\n1\nbad\n", "--truncate")
        self.assertIn("Aborted", stderr)
        self.assertEqual(ctx["conn"].durable, [99])
        self.assertEqual(ctx["conn"].depth, 0)

    def test_pymssql_interrupt_and_output_error_restore_transaction_mode(self):
        for error, expected in ((KeyboardInterrupt(), []), (BrokenPipeError(), [1])):
            with self.subTest(error=type(error).__name__):
                ctx = self._transaction_ctx()
                conn = ctx["conn"]
                if isinstance(error, KeyboardInterrupt):
                    cursor = conn.cursor()
                    original = cursor.execute
                    def interrupted(sql):
                        original(sql)
                        raise error
                    cursor.execute = interrupted
                    conn.cursor = lambda: cursor
                else:
                    ctx["output"].write = mock.Mock(side_effect=error)
                with self.assertRaises(type(error)):
                    MSSQL.execute_sql("INSERT INTO t VALUES (1); SELECT id FROM t", ctx)
                self.assertEqual(conn.durable, expected)
                self.assertEqual(conn.depth, 0)
                self.assertTrue(conn.autocommit_state)

    def test_failed_transaction_mode_restore_blocks_sql_until_reconnect(self):
        ctx = self._transaction_ctx()
        conn = ctx["conn"]
        original = conn.autocommit
        def fail_restore(enabled):
            if enabled:
                raise FakePymssqlError("restore failed")
            original(enabled)
        conn.autocommit = fail_restore
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            self.assertFalse(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
            before = list(conn.executed)
            self.assertFalse(MSSQL.execute_sql("INSERT INTO t VALUES (2)", ctx))
            MSSQL.import_data("missing.csv t", ctx)
            MSSQL.export_data("t -", ctx)
            MSSQL.run_dot_command(".tables", ctx)
        self.assertEqual(conn.executed, before)
        self.assertFalse(ctx["connection_usable"])
        self.assertIn("Reconnect", stderr.getvalue())

    def test_pymssql_transaction_start_failure_does_not_execute_sql(self):
        ctx = self._transaction_ctx()
        ctx["conn"].autocommit = mock.Mock(side_effect=FakePymssqlError("cannot start"))
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertFalse(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
        self.assertEqual(ctx["conn"].executed, [])
        self.assertFalse(ctx["connection_usable"])

    def test_mysql_select_transaction_flag_is_refreshed_from_eof_packet(self):
        ctx = self._transaction_ctx(mysql=True)
        MSSQL.execute_sql("SET autocommit = 0", ctx)
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertTrue(MSSQL.execute_sql("SELECT id FROM t", ctx))
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertEqual(ctx["conn"].executed[-1], ("SELECT id FROM t", ()))
        # An import in that transaction must not commit the SELECT's transaction.
        self._transaction_import(ctx, "id\n1\n")
        self.assertEqual(ctx["conn"].commit_count, 0)
        self.assertEqual(ctx["conn"].durable, [])

    def test_connect_mysql_tracks_eof_status_without_extra_sql(self):
        conn = MysqlTransactionConn()
        with mock.patch.object(MSSQL.pymysql, "connect", return_value=conn):
            MSSQL._connect(self._mysql_args(user="app"))
        conn.autocommit(False)
        conn.cursor().execute("SELECT id FROM t")
        self.assertEqual(conn.server_status, 1)
        self.assertEqual(conn.executed, [("SELECT id FROM t", ())])

    def test_mysql_status_packet_hook_preserves_packet_types_and_errors(self):
        packet = types.SimpleNamespace(is_eof_packet=lambda: False)
        reader = mock.Mock(return_value=packet)
        conn = types.SimpleNamespace(_read_packet=reader, server_status=2)
        MSSQL._install_mysql_status_tracking(conn)
        token = object()
        self.assertIs(conn._read_packet(token, sample=True), packet)
        reader.assert_called_once_with(token, sample=True)
        self.assertEqual(conn.server_status, 2)
        reader.side_effect = FakePymysqlError("packet failure")
        with self.assertRaises(FakePymysqlError):
            conn._read_packet()

    def test_mysql_deadlock_recovery_keeps_following_failed_import_atomic(self):
        ctx = self._transaction_ctx(mysql=True)
        MSSQL.execute_sql("BEGIN", ctx)
        self.assertEqual(ctx["transaction_depth"], 1)
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertFalse(MSSQL.execute_sql("DEADLOCK", ctx))
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertTrue(ctx["conn"].get_autocommit())
        stderr = self._transaction_import(ctx, "id\n1\nbad\n")
        self.assertIn("Aborted", stderr)
        self.assertEqual(ctx["conn"].durable, [])
        self.assertEqual(ctx["conn"].autocommit_calls, [False, True])

    def test_mysql_import_refreshes_state_after_direct_cursor_select(self):
        ctx = self._transaction_ctx(mysql=True)
        conn = ctx["conn"]
        conn.autocommit(False)
        conn.cursor().execute("SELECT id FROM t")
        self.assertEqual(ctx["transaction_depth"], 0)
        self._transaction_import(ctx, "id\n1\n")
        self.assertEqual(ctx["transaction_depth"], 1)
        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(conn.durable, [])

    def test_mysql_socket_interrupt_cancels_query_and_repl_recovers(self):
        ctx = self._transaction_ctx(mysql=True)
        conn = ctx["conn"]
        cursor = conn.cursor()
        def interrupted(sql):
            conn.closed = True
            raise KeyboardInterrupt
        cursor.execute = interrupted
        conn.cursor = lambda: cursor
        side_cursor = FakeCursor()
        side_conn = FakeConn(side_cursor)
        with mock.patch("builtins.input", side_effect=["INSERT INTO t VALUES (1);", ".quit"]), \
             mock.patch.object(MSSQL, "setup_readline"), \
             mock.patch.object(MSSQL.pymysql, "connect", return_value=side_conn) as connect, \
             mock.patch("sys.stdout", new=io.StringIO()), \
             mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            with self.assertRaises(SystemExit):
                MSSQL.interactive_loop(ctx)
        self.assertEqual(side_cursor.executed, [("KILL QUERY %s", (42,))])
        connect.assert_called_once()
        self.assertTrue(side_conn.closed)
        self.assertFalse(ctx["connection_usable"])
        self.assertIn(".reconnect", stderr.getvalue())

    def test_mysql_command_interrupt_also_cancels_server_query(self):
        ctx = self._transaction_ctx(mysql=True)
        conn = ctx["conn"]
        cursor = conn.cursor()
        def interrupted(sql):
            conn.closed = True
            raise KeyboardInterrupt
        cursor.execute = interrupted
        conn.cursor = lambda: cursor
        with mock.patch.object(MSSQL, "_mysql_kill_running_query", return_value=True) as kill, \
             mock.patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx)
        kill.assert_called_once_with(ctx)
        self.assertFalse(ctx["connection_usable"])

    def test_mysql_socket_interrupt_warns_if_cancellation_fails(self):
        ctx = self._transaction_ctx(mysql=True)
        ctx["conn"].closed = True
        with mock.patch.object(MSSQL.pymysql, "connect", side_effect=FakePymysqlError("unreachable")), \
             mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL._handle_repl_interrupt(ctx)
        self.assertIn("may commit", stderr.getvalue())
        self.assertFalse(ctx["connection_usable"])

    def test_mysql_formatting_interrupt_does_not_cancel_idle_connection(self):
        ctx = self._transaction_ctx(mysql=True)
        with mock.patch.object(MSSQL, "_mysql_kill_running_query") as kill, \
             mock.patch("sys.stderr", new=io.StringIO()):
            MSSQL._handle_repl_interrupt(ctx)
        kill.assert_not_called()
        self.assertTrue(ctx["conn"].open)

    def test_datetimeoffset_converter_preserves_fraction_and_timezone(self):
        import datetime
        for hours, minutes in ((0, 0), (5, 30), (-3, -30), (0, -30)):
            with self.subTest(offset=(hours, minutes)):
                raw = struct.pack("<6hI2h", 2026, 10, 10, 15, 42, 30, 123456700, hours, minutes)
                value = MSSQL._decode_datetimeoffset(raw)
                self.assertEqual(value.microsecond, 123456)
                self.assertEqual(value.utcoffset(), datetime.timedelta(hours=hours, minutes=minutes))
                self.assertEqual((value.year, value.month, value.day), (2026, 10, 10))
                self.assertIn(value.isoformat(sep=" "), MSSQL.format_json([(value,)], ["date"]))
        self.assertIsNone(MSSQL._decode_datetimeoffset(None))

    def test_trusted_connection_closes_if_converter_setup_fails(self):
        conn = mock.Mock()
        conn.add_output_converter.side_effect = FakePyodbcError("setup failed")
        with mock.patch.object(MSSQL.pyodbc, "connect", return_value=conn):
            with self.assertRaises(FakePyodbcError):
                MSSQL._connect(self._mysql_args(mysql=False, trusted_connection=True))
        conn.close.assert_called_once()

    def test_mysql_unicode_passwords_use_utf8_bytes(self):
        for password in ("ascii", "café", "漢字🔑"):
            with self.subTest(password=password), \
                 mock.patch.object(MSSQL.pymysql, "connect") as connect:
                MSSQL._connect(self._mysql_args(user="app", password=password))
                self.assertEqual(connect.call_args.kwargs["password"], password.encode("utf-8"))

    def test_mysql_missing_rsa_dependency_is_handled_at_startup_and_reconnect(self):
        args = self._mysql_args(user="app")
        error = RuntimeError("'cryptography' package is required for caching_sha2_password auth methods")
        ctx = {"conn": FakeConn(), "conn_args": args}
        with mock.patch.object(MSSQL.pymysql, "connect", side_effect=error), \
             mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            with self.assertRaises(SystemExit) as exc:
                MSSQL.build_connection(args)
            self.assertEqual(exc.exception.code, 1)
            self.assertTrue(MSSQL.run_dot_command(".reconnect", ctx))
        self.assertFalse(ctx["conn"].closed)
        self.assertIn('pip install "pymysql[rsa]"', stderr.getvalue())
        self.assertIn("Reconnect failed", stderr.getvalue())

    def test_db_errors_do_not_swallow_unrelated_runtime_errors(self):
        error = RuntimeError("unrelated implementation failure")
        with mock.patch.object(MSSQL.pymysql, "connect", side_effect=error):
            with self.assertRaises(RuntimeError):
                MSSQL.build_connection(self._mysql_args(user="app"))
        self.assertNotIn(Exception, MSSQL.DBError)

    def test_main_closes_active_connection_after_reconnects(self):
        for reconnects, error in ((0, None), (1, None), (2, None), (1, RuntimeError("loop failed"))):
            with self.subTest(reconnects=reconnects, error=error):
                connections = [mock.Mock() for _ in range(reconnects + 1)]
                def interactive(ctx):
                    for _ in range(reconnects):
                        MSSQL.run_dot_command(".reconnect", ctx)
                    if error:
                        raise error
                results = [(conn, "test", FAKE_DRIVER) for conn in connections[1:]]
                with mock.patch.object(MSSQL, "build_connection", return_value=(connections[0], "test", FAKE_DRIVER)), \
                     mock.patch.object(MSSQL, "_connect", side_effect=results), \
                     mock.patch.object(MSSQL, "interactive_loop", side_effect=interactive), \
                     mock.patch("sys.argv", ["mssql.py", "-U", "app"]), \
                     mock.patch("sys.stdin.isatty", return_value=True), \
                     mock.patch("sys.stderr", new=io.StringIO()):
                    if error:
                        with self.assertRaises(RuntimeError):
                            MSSQL.main()
                    else:
                        MSSQL.main()
                for conn in connections:
                    conn.close.assert_called_once()

    def test_main_closes_original_connection_when_reconnect_fails(self):
        conn = mock.Mock()
        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)), \
             mock.patch.object(MSSQL, "_connect", side_effect=FakePymssqlError("down")), \
             mock.patch.object(MSSQL, "interactive_loop", side_effect=lambda ctx: MSSQL.run_dot_command(".reconnect", ctx)), \
             mock.patch("sys.argv", ["mssql.py", "-U", "app"]), \
             mock.patch("sys.stdin.isatty", return_value=True), \
             mock.patch("sys.stderr", new=io.StringIO()):
            MSSQL.main()
        conn.close.assert_called_once()

    def test_password_resolution_distinguishes_omitted_and_explicit_empty(self):
        for mysql, module, prefix in ((False, MSSQL.pymssql, "MSSQL"), (True, MSSQL.pymysql, "MYSQL")):
            for requested, expected in ((None, "file-pw"), ("", ""), ("flag-pw", "flag-pw")):
                with self.subTest(mysql=mysql, password=requested):
                    args = self._mysql_args(user="app", mysql=mysql, password=requested,
                                            env_file_values={f"{prefix}_PASSWORD": "file-pw"})
                    with mock.patch.dict(os.environ, {f"{prefix}_PASSWORD": "env-pw"}, clear=True), \
                         mock.patch.object(module, "connect") as connect:
                        MSSQL._connect(args)
                    self.assertEqual(connect.call_args.kwargs["password"], expected.encode("utf-8") if mysql else expected)

    def test_main_empty_password_and_reconnect_override_environment(self):
        for mysql, module, prefix in ((False, MSSQL.pymssql, "MSSQL"), (True, MSSQL.pymysql, "MYSQL")):
            with self.subTest(mysql=mysql):
                connections = [mock.Mock(), mock.Mock()]
                argv = ["mssql.py", "-U", "app", "-W", ""] + (["--mysql"] if mysql else [])
                with mock.patch.dict(os.environ, {f"{prefix}_PASSWORD": "env-pw"}, clear=True), \
                     mock.patch.object(module, "connect", side_effect=connections) as connect, \
                     mock.patch.object(MSSQL, "interactive_loop", side_effect=lambda ctx: MSSQL.run_dot_command(".reconnect", ctx)), \
                     mock.patch("sys.argv", argv), \
                     mock.patch("sys.stdin.isatty", return_value=True), \
                     mock.patch("sys.stderr", new=io.StringIO()):
                    MSSQL.main()
                self.assertEqual(len(connect.call_args_list), 2)
                for call in connect.call_args_list:
                    self.assertEqual(call.kwargs["password"], b"" if mysql else "")

    def test_trusted_connection_rejects_explicit_empty_password(self):
        with mock.patch.object(MSSQL, "build_connection") as connect, \
             mock.patch("sys.argv", ["mssql.py", "-E", "-W", ""]), \
             mock.patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(SystemExit) as exc:
                MSSQL.main()
        self.assertEqual(exc.exception.code, 2)
        connect.assert_not_called()

    def test_skipped_transaction_controls_preserve_pending_rows(self):
        for driver, conn_type in (("pymssql", PymssqlTransactionConn),
                                  ("pyodbc", OdbcTransactionConn)):
            for control in ("COMMIT", "ROLLBACK"):
                with self.subTest(driver=driver, control=control):
                    ctx = self._transaction_ctx(conn=conn_type())
                    ctx["driver_name"] = driver
                    conn = ctx["conn"]
                    with mock.patch("sys.stderr", new=io.StringIO()):
                        for sql in ("BEGIN TRAN", "INSERT INTO t VALUES (1)",
                                    f"IF 1 = 0 {control}", "INSERT INTO t VALUES (2)"):
                            self.assertTrue(MSSQL.execute_sql(sql, ctx))
                        self.assertEqual(ctx["transaction_depth"], 1)
                        self.assertEqual(conn.depth, 1)
                        self.assertEqual(conn.pending, [1, 2])
                        self.assertEqual(conn.durable, [])
                        self.assertTrue(MSSQL.execute_sql("COMMIT", ctx))
                    conn.close()
                    self.assertEqual(conn.durable, [1, 2])
                    self.assertEqual(conn.autocommit_calls, [])

    def test_skipped_begin_leaves_following_work_owned_by_client(self):
        ctx = self._transaction_ctx()
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertTrue(MSSQL.execute_sql("IF 1 = 0 BEGIN TRAN", ctx))
            self.assertEqual(ctx["transaction_depth"], 0)
            self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
        self.assertEqual(ctx["conn"].durable, [1])
        self.assertEqual(ctx["conn"].depth, 0)

    def test_named_rollback_across_commands_refreshes_actual_state(self):
        ctx = self._transaction_ctx()
        with mock.patch("sys.stderr", new=io.StringIO()):
            for sql in ("BEGIN TRAN outer_name", "INSERT INTO t VALUES (1)",
                        "ROLLBACK TRAN outer_name"):
                self.assertTrue(MSSQL.execute_sql(sql, ctx))
            self.assertEqual(ctx["transaction_depth"], 0)
            self.assertFalse(ctx["transaction_open"])
            self._transaction_import(ctx, "id\n2\n")
        self.assertEqual(ctx["conn"].durable, [2])
        self.assertEqual(ctx["conn"].depth, 0)

    def test_failed_transaction_verification_blocks_following_work(self):
        ctx = self._transaction_ctx()
        conn = ctx["conn"]
        cursor_factory = conn.cursor
        def failing_sync_cursor():
            cursor = cursor_factory()
            execute = cursor.execute
            def fail_sync(sql, params=()):
                if sql.startswith("SELECT XACT_STATE"):
                    raise FakePymssqlError("state unavailable")
                return execute(sql, params)
            cursor.execute = fail_sync
            return cursor
        conn.cursor = failing_sync_cursor
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            self.assertFalse(MSSQL.execute_sql("BEGIN TRAN", ctx))
            before = list(conn.executed)
            self.assertFalse(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
            MSSQL.import_data("missing.csv t", ctx)
        self.assertFalse(ctx["connection_usable"])
        self.assertEqual(conn.executed, before)
        self.assertIn("Cannot verify transaction state", stderr.getvalue())

    def test_odbc_select_then_explicit_commit_is_durable(self):
        ctx = self._transaction_ctx(conn=OdbcTransactionConn())
        ctx["driver_name"] = "pyodbc"
        conn = ctx["conn"]
        with mock.patch("sys.stderr", new=io.StringIO()):
            for sql in ("SELECT id FROM t", "BEGIN TRAN", "BEGIN TRAN",
                        "INSERT INTO t VALUES (1)", "COMMIT"):
                self.assertTrue(MSSQL.execute_sql(sql, ctx))
            self.assertEqual(conn.depth, 1)
            self.assertEqual(conn.durable, [])
            self.assertTrue(MSSQL.execute_sql("COMMIT", ctx))
        conn.close()
        self.assertEqual(conn.durable, [1])
        self.assertEqual(ctx["transaction_depth"], 0)
        self.assertEqual(conn.autocommit_calls, [False, True])

    def test_implicit_transactions_leave_commit_to_user(self):
        # With SET IMPLICIT_TRANSACTIONS ON the tool's driver BEGIN nested in
        # the session's transaction, so its COMMIT made nothing durable and
        # restoring autocommit rolled the rows back.
        for ending, durable in (("COMMIT", [1, 2]), ("ROLLBACK", [])):
            with self.subTest(ending=ending):
                ctx = self._transaction_ctx(conn=ImplicitTransactionConn())
                conn = ctx["conn"]
                with mock.patch("sys.stderr", new=io.StringIO()):
                    for sql in ("SET IMPLICIT_TRANSACTIONS ON", "INSERT INTO t VALUES (1)",
                                "INSERT INTO t VALUES (2)"):
                        self.assertTrue(MSSQL.execute_sql(sql, ctx))
                    self.assertTrue(ctx["implicit_transactions"])
                    self.assertEqual(ctx["transaction_depth"], 1)
                    self.assertEqual(conn.durable, [])
                    self.assertTrue(MSSQL.execute_sql(ending, ctx))
                self.assertEqual(conn.durable, durable)
                self.assertEqual(ctx["transaction_depth"], 0)
                self.assertEqual(conn.autocommit_calls, [])

    def test_implicit_transactions_off_again_restores_owned_batches(self):
        ctx = self._transaction_ctx(conn=ImplicitTransactionConn())
        conn = ctx["conn"]
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertTrue(MSSQL.execute_sql("SET IMPLICIT_TRANSACTIONS ON", ctx))
            self.assertTrue(MSSQL.execute_sql("SET IMPLICIT_TRANSACTIONS OFF", ctx))
            self.assertFalse(ctx["implicit_transactions"])
            self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
        self.assertEqual(conn.durable, [1])
        self.assertEqual(ctx["transaction_depth"], 0)

    def test_implicit_transaction_import_waits_for_user_commit(self):
        ctx = self._transaction_ctx(conn=ImplicitTransactionConn())
        conn = ctx["conn"]
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertTrue(MSSQL.execute_sql("SET IMPLICIT_TRANSACTIONS ON", ctx))
        self._transaction_import(ctx, "id\n1\n2\n", "--batch 1")
        self.assertEqual(conn.durable, [])
        self.assertEqual(ctx["transaction_depth"], 1)
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertTrue(MSSQL.execute_sql("COMMIT", ctx))
        self.assertEqual(conn.durable, [1, 2])

    def test_pymssql_import_skip_retries_rows_with_named_params(self):
        # A failed batch is retried row by row; each retry must use the same
        # %(pN)s parameter mapping as the batch, or every row fails.
        ctx = self._transaction_ctx()
        stderr = self._transaction_import(ctx, "id\n1\nbad\n3\n", "--on-error skip")
        self.assertEqual(ctx["conn"].durable, [1, 3])
        self.assertIn("1 row(s) skipped", stderr)

    def test_odbc_owned_batches_and_imports_restore_autocommit(self):
        ctx = self._transaction_ctx(conn=OdbcTransactionConn())
        ctx["driver_name"] = "pyodbc"
        conn = ctx["conn"]
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertTrue(MSSQL.execute_sql("INSERT INTO t VALUES (1)", ctx))
            self.assertFalse(MSSQL.execute_sql("INSERT INTO t VALUES (2); FAIL", ctx))
            self._transaction_import(ctx, "id\n3\n4\n", "--batch 1")
        self.assertEqual(conn.durable, [1, 3, 4])
        self.assertEqual(conn.depth, 0)
        self.assertTrue(conn.autocommit)

    def test_odbc_import_joins_explicit_transaction_and_failed_begin_rolls_back(self):
        ctx = self._transaction_ctx(conn=OdbcTransactionConn())
        ctx["driver_name"] = "pyodbc"
        conn = ctx["conn"]
        with mock.patch("sys.stderr", new=io.StringIO()):
            self.assertFalse(MSSQL.execute_sql("BEGIN TRAN; INSERT INTO t VALUES (9); FAIL", ctx))
            self.assertEqual(conn.depth, 0)
            self.assertTrue(MSSQL.execute_sql("BEGIN TRAN", ctx))
            self._transaction_import(ctx, "id\n1\n2\n", "--batch 1")
            self.assertEqual(conn.pending, [1, 2])
            self.assertEqual(conn.durable, [])
            self.assertEqual(conn.autocommit_calls, [])
            self.assertTrue(MSSQL.execute_sql("COMMIT", ctx))
        self.assertEqual(conn.durable, [1, 2])

    @unittest.skipUnless(importlib.util.find_spec("sqlglot"), "optional SQLGlot dependency")
    def test_export_rejects_all_semicolon_free_write_batches(self):
        queries = (
            "SELECT 1 DELETE FROM dbo.victims",
            "SELECT 1 AS id\nDELETE FROM dbo.victims",
            "SELECT 1 AS id UPDATE t SET id = 2",
            "SELECT 1 AS id INSERT INTO t VALUES (2)",
            "SELECT 1 AS id EXEC p",
            "SELECT 1 AS id BEGIN TRAN",
            "WITH x AS (SELECT 1 AS id) SELECT id FROM x DELETE FROM t",
            "WITH x AS (DELETE FROM t) SELECT * FROM x",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "out.csv"
            for sql in queries:
                with self.subTest(sql=sql):
                    path.write_text("original\n", encoding="utf-8")
                    conn = FakeConn()
                    with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        MSSQL.export_data(
                            f"- {quote_dot_arg(path)} --query {quote_dot_arg(sql)}",
                            {"conn": conn},
                        )
                    self.assertEqual(conn._cursor.executed, [])
                    self.assertIn("read-only SELECT", stderr.getvalue())
                    self.assertEqual(path.read_text(), "original\n")
                    self.assertEqual(list(Path(tmpdir).iterdir()), [path])

    @unittest.skipUnless(importlib.util.find_spec("sqlglot"), "optional SQLGlot dependency")
    def test_export_rejects_exec_that_sqlglot_reads_as_table_alias(self):
        # SQLGlot parses "FROM t EXEC('...')" as table t aliased EXEC with a
        # column list; SQL Server runs the EXEC as a second statement.
        queries = (
            "SELECT v FROM dbo.t EXEC('DELETE FROM dbo.t')",
            "SELECT v FROM dbo.t EXECUTE('DELETE FROM dbo.t')",
            "SELECT TOP (1) v FROM dbo.t /* a /* b */ c */ eXeCuTe('DELETE FROM dbo.t')",
            "SELECT v FROM dbo.t\r\nEXEC (N'DROP TABLE dbo.t')",
            "SELECT v FROM dbo.t SET IMPLICIT_TRANSACTIONS ON",
            "SELECT v FROM dbo.t USE master",
            "SELECT v FROM dbo.t TRUNCATE TABLE dbo.t",
        )
        for sql in queries:
            with self.subTest(sql=sql):
                with self.assertRaisesRegex(ValueError, "read-only SELECT"):
                    MSSQL._validate_export_query(sql, "mssql")
                conn = FakeConn()
                with tempfile.TemporaryDirectory() as tmpdir, \
                     mock.patch("sys.stderr", new=io.StringIO()):
                    MSSQL.export_data(
                        f"- {quote_dot_arg(Path(tmpdir) / 'out.csv')} --query {quote_dot_arg(sql)}",
                        {"conn": conn})
                self.assertEqual(conn._cursor.executed, [])

    @unittest.skipUnless(importlib.util.find_spec("sqlglot"), "optional SQLGlot dependency")
    def test_export_accepts_semicolon_in_string_and_parenthesized_select(self):
        for sql in ("SELECT 'x;' AS a", "(SELECT 1 AS a)", "((SELECT 1 AS a)) UNION (SELECT 2)",
                    "SELECT [exec] = 1, [set] = 2", "SELECT 'EXEC(x)' AS s",
                    "SELECT 1 AS a OPTION (RECOMPILE)"):
            with self.subTest(sql=sql):
                MSSQL._validate_export_query(sql, "mssql")
                cursor = FakeCursor([{"description": [("a",)]}])
                with tempfile.TemporaryDirectory() as tmpdir, \
                     mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.export_data(
                        f"- {quote_dot_arg(Path(tmpdir) / 'out.csv')} --query {quote_dot_arg(sql)}",
                        {"conn": FakeConn(cursor)})
                self.assertEqual(cursor.executed, [(sql, ())], stderr.getvalue())

    @unittest.skipUnless(importlib.util.find_spec("sqlglot"), "optional SQLGlot dependency")
    def test_export_accepts_complete_read_only_queries_and_quoted_keywords(self):
        for dialect, queries in (
            ("mssql", ("SELECT [DELETE] FROM t", "SELECT 'DELETE FROM t' AS text",
                       "WITH x AS (SELECT 1 AS id) SELECT id FROM x",
                       "SELECT 1 AS id UNION ALL SELECT 2", "SELECT 1 /* DELETE FROM t */")),
            ("mysql", ("SELECT `DELETE` FROM t", "SELECT '/*! DELETE */' AS text")),
        ):
            for sql in queries:
                with self.subTest(sql=sql):
                    cursor = FakeCursor([{"description": [("id",)]}])
                    with mock.patch("sys.stdout", new=io.StringIO()), \
                         mock.patch("sys.stderr", new=io.StringIO()):
                        MSSQL.export_data(
                            f"- - --format csv --query {quote_dot_arg(sql)}",
                            {"conn": FakeConn(cursor), "dialect": dialect},
                        )
                    self.assertEqual(cursor.executed, [(sql, ())])

    @unittest.skipUnless(importlib.util.find_spec("sqlglot"), "optional SQLGlot dependency")
    def test_export_rejects_mysql_executable_comments(self):
        for comment in ("/*! DELETE FROM t */", "/*M! DELETE FROM t */"):
            with self.subTest(comment=comment), self.assertRaisesRegex(ValueError, "executable comments"):
                MSSQL._validate_export_query("SELECT 1 " + comment, "mysql")

    def test_export_missing_query_parser_fails_before_database_execution(self):
        conn = FakeConn()
        with mock.patch.dict(sys.modules, {"sqlglot": None}), \
             mock.patch("sys.stdout", new=io.StringIO()), \
             mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.export_data('- - --format csv --query "SELECT 1"', {"conn": conn})
        self.assertEqual(conn._cursor.executed, [])
        self.assertIn("requirements-query.txt", stderr.getvalue())

    def test_binary_export_import_round_trip_binds_bytes_only_to_binary_columns(self):
        values = [(b"\x00\xff\x01", "0x00ff01"), (b"", "0x"), (None, "plain")]
        class ExportCursor(FakeCursor):
            def __init__(self):
                super().__init__([{"description": [("Payload",), ("label",)]}])
                self.chunks = [values]
            def fetchmany(self, size):
                return self.chunks.pop(0) if self.chunks else []
        for dialect, binary_type in (("mssql", "varbinary"), ("mysql", "blob")):
            for fmt in ("csv", "json", "jsonl"):
                with self.subTest(dialect=dialect, fmt=fmt), tempfile.TemporaryDirectory() as tmpdir:
                    path = Path(tmpdir) / ("rows." + fmt)
                    metadata = FakeCursor([{"rows": [
                        ("Payload", binary_type, "YES"), ("label", "varchar", "YES")]}])
                    import_conn = FakeConn(metadata)
                    if dialect == "mysql":
                        import_conn.get_autocommit = lambda: False
                        import_conn.server_status = 0
                    with mock.patch("sys.stderr", new=io.StringIO()):
                        MSSQL.export_data(f"t {quote_dot_arg(path)}", {
                            "conn": FakeConn(ExportCursor()), "dialect": dialect})
                        MSSQL.import_data(f"{quote_dot_arg(path)} t", {
                            "conn": import_conn, "dialect": dialect})
                    inserted = [params for sql, params in metadata.executed if sql.startswith("INSERT")]
                    self.assertEqual(inserted, [values])

    def _import_rows(self, text, db_cols, suffix="csv", options="", ctx_extra=None):
        """Import text into t with the given (name, type) columns; return INSERT params."""
        metadata = FakeCursor([{"rows": [(name, typ, "YES") for name, typ in db_cols]}])
        ctx = {"conn": FakeConn(metadata), **(ctx_extra or {})}
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / ("rows." + suffix)
            path.write_text(text, encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(path)} t {options}", ctx)
        inserted = [params for sql, params in metadata.executed if sql.startswith("INSERT")]
        return metadata, inserted, stderr.getvalue()

    def test_adapt_placeholders_uses_named_params_for_pymssql(self):
        # pymssql substitutes every "%s" in the statement, even inside [p%s],
        # and never collapses "%%"; it leaves "%s" alone with named params.
        ctx = {"driver_name": "pymssql", "paramstyle": "format"}
        sql = f"SELECT * FROM [p%s] WHERE a = {MSSQL.PARAM_PLACEHOLDER} AND b = {MSSQL.PARAM_PLACEHOLDER}"
        self.assertEqual(MSSQL._adapt_placeholders(sql, ctx),
                         "SELECT * FROM [p%s] WHERE a = %(p0)s AND b = %(p1)s")
        self.assertEqual(MSSQL._adapt_params(("x", None), ctx), {"p0": "x", "p1": None})
        self.assertEqual(MSSQL._adapt_params((), ctx), ())
        mysql = {"driver_name": "pymysql", "dialect": "mysql", "paramstyle": "format"}
        self.assertEqual(MSSQL._adapt_params(("x",), mysql), ("x",))
        # pymssql inlines params as N'...' literals, where "\\"+LF is a line
        # continuation; pyodbc binds real parameters and needs no escaping.
        self.assertEqual(MSSQL._adapt_params(("C:\\d\\\nx",), ctx), {"p0": "C:\\d\\\\\n\nx"})
        odbc = {"driver_name": "pyodbc", "paramstyle": "qmark"}
        self.assertEqual(MSSQL._adapt_params(("C:\\d\\\nx",), odbc), ("C:\\d\\\nx",))

    def test_pymssql_import_into_percent_s_names_uses_named_params(self):
        metadata, inserted, stderr = self._import_rows(
            "c%d\n1\n", [("c%d", "int")], options="",
            ctx_extra={"driver_name": "pymssql", "paramstyle": "format"})
        meta_sql, meta_params = metadata.executed[0]
        self.assertIn("c.TABLE_NAME = %(p0)s AND c.TABLE_SCHEMA = %(p1)s", meta_sql)
        self.assertEqual(meta_params, {"p0": "t", "p1": "dbo"})
        insert_sql = next(sql for sql, _ in metadata.executed if sql.startswith("INSERT"))
        self.assertEqual(insert_sql, "INSERT INTO [dbo].[t] ([c%d]) VALUES (%(p0)s)")
        self.assertEqual(inserted, [[{"p0": "1"}]])
        self.assertIn("Done. 1 rows", stderr)

    def test_pymssql_import_sends_binary_as_bytearray(self):
        # pymssql quotes bytes as a varchar literal, which SQL Server refuses
        # to convert implicitly to varbinary; bytearray is sent as 0x...
        _, inserted, _ = self._import_rows(
            "payload\n0x0102\n0x\n", [("payload", "varbinary")],
            ctx_extra={"driver_name": "pymssql", "paramstyle": "format"})
        values = [row["p0"] for row in inserted[0]]
        self.assertEqual(values, [b"\x01\x02", b""])
        self.assertTrue(all(type(v) is bytearray for v in values))

    def test_pymssql_import_rejects_nul_text_per_on_error_policy(self):
        pymssql_ctx = {"driver_name": "pymssql", "paramstyle": "format"}
        _, inserted, stderr = self._import_rows(
            '{"s": "a\\u0000b"}\n{"s": "ok"}\n', [("s", "nvarchar")], suffix="jsonl",
            ctx_extra=pymssql_ctx)
        self.assertEqual(inserted, [])
        self.assertIn("NUL", stderr)
        _, inserted, stderr = self._import_rows(
            '{"s": "a\\u0000b"}\n{"s": "ok"}\n', [("s", "nvarchar")], suffix="jsonl",
            options="--on-error skip", ctx_extra=pymssql_ctx)
        self.assertEqual(inserted, [[{"p0": "ok"}]])

    def test_import_json_warns_once_about_keys_the_first_record_lacks(self):
        text = '{"id": 1}\n{"id": 2, "s": "dropped"}\n{"id": 3, "s": "also"}\n'
        _, inserted, stderr = self._import_rows(text, [("id", "int"), ("s", "nvarchar")], suffix="jsonl")
        self.assertEqual(inserted, [[(1,), (2,), (3,)]])
        self.assertEqual(stderr.count("that the first record lacks"), 1)
        self.assertIn("Row 2 has key(s) ['s']", stderr)
        _, _, stderr = self._import_rows(text, [("id", "int"), ("s", "nvarchar")], suffix="jsonl",
                                         options="--columns id")
        self.assertNotIn("first record lacks", stderr)

    def _import_with_metadata(self, text, metadata_rows, options="", suffix="csv"):
        metadata = FakeCursor([{"rows": metadata_rows}])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / ("rows." + suffix)
            path.write_text(text, encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(path)} t {options}", {"conn": FakeConn(metadata)})
        return metadata.executed, stderr.getvalue()

    def test_import_skips_generated_columns_from_a_full_export(self):
        # (name, type, nullable, is_generated, is_identity): rowversion and
        # computed columns can't be inserted, but a full export includes them.
        meta = [("id", "int", "NO", 0, 0), ("rv", "timestamp", "NO", 1, 0),
                ("comp", "int", "YES", 1, 0), ("s", "nvarchar", "YES", 0, 0)]
        for text, options in (("id,rv,comp,s\n1,0x00,2,a\n", ""), ("1,0x00,2,a\n", "--no-header")):
            with self.subTest(options=options):
                executed, stderr = self._import_with_metadata(text, meta, options)
                inserts = [(sql, params) for sql, params in executed if sql.startswith("INSERT")]
                self.assertEqual(inserts, [("INSERT INTO [dbo].[t] ([id], [s]) VALUES (%s, %s)", [("1", "a")])])
                self.assertIn("Skipping generated column(s) ['rv', 'comp']", stderr)
        executed, stderr = self._import_with_metadata("id,rv\n1,0x00\n", meta, "--columns id,rv")
        self.assertIn("[rv]", next(sql for sql, _ in executed if sql.startswith("INSERT")))
        self.assertNotIn("Skipping", stderr)

    def test_import_identity_column_requires_keep_identity(self):
        meta = [("id", "int", "NO", 0, 1), ("s", "nvarchar", "YES", 0, 0)]
        executed, stderr = self._import_with_metadata("id,s\n5,a\n", meta)
        self.assertFalse(any(sql.startswith(("INSERT", "SET", "TRUNCATE")) for sql, _ in executed))
        self.assertIn("is an IDENTITY column", stderr)
        self.assertIn("--keep-identity", stderr)

        executed, stderr = self._import_with_metadata("id,s\n5,a\n", meta, "--keep-identity --truncate")
        statements = [sql for sql, _ in executed if not sql.startswith("SELECT")]
        self.assertEqual(statements, ["SET IDENTITY_INSERT [dbo].[t] ON", "TRUNCATE TABLE [dbo].[t]",
                                      "INSERT INTO [dbo].[t] ([id], [s]) VALUES (%s, %s)",
                                      "SET IDENTITY_INSERT [dbo].[t] OFF"])

        executed, _ = self._import_with_metadata("id,s\n5,a\n", meta, "--columns s")
        self.assertEqual([p for sql, p in executed if sql.startswith("INSERT")], [[("a",)]])
        self.assertFalse(any(sql.startswith("SET IDENTITY_INSERT") for sql, _ in executed))

    def test_import_identity_insert_is_turned_off_after_a_failed_batch(self):
        class FailingInsertCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                super().executemany(sql, params_seq)
                raise MSSQL.pymssql.Error("boom")

        metadata = FailingInsertCursor([{"rows": [("id", "int", "NO", 0, 1)]}])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.csv"
            path.write_text("id\n1\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.import_data(f"{quote_dot_arg(path)} t --keep-identity", {"conn": FakeConn(metadata)})
        self.assertEqual(metadata.executed[-1][0], "SET IDENTITY_INSERT [dbo].[t] OFF")

    def test_import_decodes_hex_for_clr_type_columns(self):
        _, inserted, _ = self._import_rows(
            "g,h\n0xe610,0x58\nPOINT(1 2),/1/\n", [("g", "geography"), ("h", "hierarchyid")])
        self.assertEqual(inserted, [[(b"\xe6\x10", b"\x58"), ("POINT(1 2)", "/1/")]])

    def test_import_csv_accepts_fields_beyond_csv_module_default_limit(self):
        # .export writes (max) values far over csv.reader's 128 KiB default.
        big = "x" * 300_000
        _, inserted, stderr = self._import_rows(f'id,s\n1,"{big}"\n2,{big}\n', [("id", "int"), ("s", "nvarchar")])
        self.assertEqual([len(r[1]) for r in inserted[0]], [300_000, 300_000], stderr)

    def test_mssql_import_sends_stacked_inserts_per_request(self):
        # One round trip per chunk of single-row INSERTs (not one multi-row
        # VALUES list, which would unify types across rows), within the
        # parameter limit, and the same batch transaction as before.
        class RawCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))

        metadata = RawCursor([{"rows": [("a", "int", "YES"), ("b", "nvarchar", "YES"), ("c", "int", "YES")]}])
        conn = FakeConn(metadata)
        rows = "".join(f"{i},x{i},{i}\n" for i in range(1000))
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.csv"
            path.write_text("a,b,c\n" + rows, encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.import_data(f"{quote_dot_arg(path)} t --batch 700",
                                  {"conn": conn, "driver_name": "pymssql", "paramstyle": "format"})
        calls = [(sql, params) for sql, params in metadata.executed if sql.startswith("BEGIN TRY")]
        # 2000 parameters / 3 columns = 666 rows per request; batch 700 -> 666 + 34, then 300
        self.assertEqual([[sql.count("INSERT INTO") for _ in params] for sql, params in calls],
                         [[666], [34], [300]])
        sql, params = calls[0]
        self.assertTrue(sql.endswith("END TRY\nBEGIN CATCH\n    THROW;\nEND CATCH"))
        self.assertIn("INSERT INTO [dbo].[t] ([a], [b], [c]) VALUES (%(p0)s, %(p1)s, %(p2)s);\n"
                      "INSERT INTO [dbo].[t] ([a], [b], [c]) VALUES (%(p3)s, %(p4)s, %(p5)s);", sql)
        self.assertEqual(len(params[0]), 1998)
        self.assertEqual(params[0]["p1997"], "665")
        self.assertEqual(conn.commit_count, 2)

    def test_import_skip_isolates_bad_rows_by_halving_failed_groups(self):
        # A failed batch is retried in halves: clean groups go in one request,
        # only groups containing a bad row are split further.
        class BadRowCursor(FakeCursor):
            def __init__(self, states):
                super().__init__(states)
                self.requests = []

            def executemany(self, sql, params_seq):
                for params in params_seq:
                    rows = split_stacked_insert(sql, params)[1]
                    self.requests.append(len(rows))
                    if any("bad" in row for row in rows):
                        raise MSSQL.pymssql.Error("conversion failed")
                    self.executed.append(("INSERT", rows))

            def execute(self, sql, params=()):
                if sql.startswith("INSERT"):
                    self.requests.append(1)
                    if "bad" in params:
                        raise MSSQL.pymssql.Error("conversion failed")
                    self.executed.append(("INSERT", [params]))
                    return
                super().execute(sql, params)

        cursor = BadRowCursor([{"rows": [("id", "int", "YES")]}])
        values = [str(i) for i in range(64)]
        values[5] = values[40] = "bad"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.csv"
            path.write_text("id\n" + "\n".join(values) + "\n", encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(path)} t --on-error skip", {"conn": FakeConn(cursor)})
        inserted = [row[0] for sql, rows in cursor.executed if sql == "INSERT" for row in rows]
        self.assertEqual(inserted, [v for v in values if v != "bad"])  # order kept
        self.assertIn("Skipped row 6:", stderr.getvalue())
        self.assertIn("Skipped row 41:", stderr.getvalue())
        self.assertIn("2 row(s) skipped", stderr.getvalue())
        # 1 failed batch + a halving search per bad row, far fewer than 64 requests
        self.assertLess(len(cursor.requests), 30)

    def test_mssql_import_request_size_cap_splits_large_values(self):
        class RawCursor(FakeCursor):
            def executemany(self, sql, params_seq):
                self.executed.append((sql, list(params_seq)))

        metadata = RawCursor([{"rows": [("s", "nvarchar", "YES")]}])
        big = "y" * 1000
        with tempfile.TemporaryDirectory() as tmpdir, \
             mock.patch.object(MSSQL, "IMPORT_CHARS_PER_REQUEST", 2500):
            path = Path(tmpdir) / "rows.csv"
            path.write_text("s\n" + f"{big}\n" * 7, encoding="utf-8")
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.import_data(f"{quote_dot_arg(path)} t", {"conn": FakeConn(metadata)})
        calls = [(sql, params) for sql, params in metadata.executed if sql.startswith("BEGIN TRY")]
        # 3 rows reach 3000 chars > 2500, so chunks of 3, 3, 1; same-sized chunks share a statement
        self.assertEqual([(sql.count("INSERT INTO"), len(params)) for sql, params in calls], [(3, 2), (1, 1)])

    def test_import_empty_json_and_jsonl_import_no_rows(self):
        for suffix, text in (("json", "[\n\n]\n"), ("jsonl", "")):
            with self.subTest(suffix=suffix):
                _, inserted, stderr = self._import_rows(text, [("id", "int")], suffix=suffix)
                self.assertEqual(inserted, [])
                self.assertIn("Done. 0 rows", stderr)
                self.assertNotIn("Error", stderr)

    def test_export_csv_distinguishes_empty_string_from_null(self):
        class ExportCursor(FakeCursor):
            def __init__(self, description, rows):
                super().__init__([{"description": description}])
                self.chunks = [rows]

            def fetchmany(self, size):
                return self.chunks.pop(0) if self.chunks else []

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "out.csv"
            cursor = ExportCursor([("s",), ("n",), ("q",)], [("", None, "a,b"), (None, 7, 'q"')])
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.export_data(f"t {quote_dot_arg(path)}", {"conn": FakeConn(cursor)})
            self.assertEqual(path.read_bytes(), b's,n,q\r\n"",,"a,b"\r\n,7,"q"""\r\n')

            # A one-column NULL row is a blank line; "" stays an empty string.
            cursor = ExportCursor([("s",)], [(None,), ("",)])
            with mock.patch("sys.stderr", new=io.StringIO()):
                MSSQL.export_data(f"t {quote_dot_arg(path)}", {"conn": FakeConn(cursor)})
            self.assertEqual(path.read_bytes(), b's\r\n\r\n""\r\n')

    def test_import_csv_quoted_empty_is_empty_string_only_for_text_columns(self):
        _, inserted, _ = self._import_rows(
            's,n,d\n"","",""\n,,\n"x",1,2020-01-01\n',
            [("s", "nvarchar"), ("n", "int"), ("d", "date")])
        self.assertEqual(inserted, [[("", None, None), (None, None, None), ("x", "1", "2020-01-01")]])

    def test_import_csv_blank_line_is_null_for_single_column(self):
        _, inserted, _ = self._import_rows('s\n""\n\nx\n', [("s", "varchar")])
        self.assertEqual(inserted, [[("",), (None,), ("x",)]])

    def test_iter_csv_records_matches_csv_reader_and_reports_quoting(self):
        self.assertEqual(
            list(MSSQL._iter_csv_records(io.StringIO('a,"",\n"x\r\ny",""""\n', newline=""), ",")),
            [["a", "", None], ["x\r\ny", '"']],
        )
        rng = __import__("random").Random(1)
        pieces = ["a", ",", ";", '"', '""', "\n", "\r\n", "\r", " ", "é", "\x00"]
        for _ in range(3000):
            text = "".join(rng.choice(pieces) for _ in range(rng.randint(0, 20)))
            delimiter = rng.choice([",", ";"])
            expected = list(csv.reader(io.StringIO(text, newline=""), delimiter=delimiter))
            got = list(MSSQL._iter_csv_records(io.StringIO(text, newline=""), delimiter))
            self.assertEqual([[v or "" for v in row] for row in got], expected, repr(text))

    def test_import_invalid_binary_hex_obeys_abort_and_skip(self):
        for policy in ("abort", "skip"):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "rows.csv"
                path.write_text("payload\n0x0\n0xzz\n0x01\n", encoding="utf-8")
                cursor = FakeCursor([{"rows": [("payload", "varbinary", "YES")]}])
                conn = FakeConn(cursor)
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.import_data(f"{quote_dot_arg(path)} t --on-error {policy}", {"conn": conn})
                inserted = [params for sql, params in cursor.executed if sql.startswith("INSERT")]
                self.assertEqual(inserted, [] if policy == "abort" else [[(b"\x01",)]])
                self.assertIn("Invalid binary hex", stderr.getvalue())
                self.assertEqual(conn.rollback_count, int(policy == "abort"))

    def test_sql_server_insert_literals_preserve_unicode_and_timezone(self):
        for text in ("雪😀", "O'Brien", ""):
            with self.subTest(text=text):
                self.assertEqual(MSSQL.sql_literal(text), "N'" + text.replace("'", "''") + "'")
        for offset in (datetime.timedelta(hours=5, minutes=30), datetime.timedelta(hours=-7)):
            with self.subTest(offset=offset):
                value = datetime.datetime(2026, 10, 10, 12, 13, 14, 123456,
                                          tzinfo=datetime.timezone(offset))
                literal = MSSQL.sql_literal(value)
                restored = datetime.datetime.fromisoformat(literal.strip("'"))
                self.assertEqual(restored.utcoffset(), offset)
                self.assertEqual(restored.replace(microsecond=0), value.replace(microsecond=0))
                self.assertEqual(restored.microsecond, 123000)
        self.assertIn("N'雪😀'", MSSQL.format_insert([("雪😀",)], ["text"], "[t]"))

    def test_output_reopening_same_file_flushes_before_truncation(self):
        for append in (False, True):
            for alias in (False, True):
                with self.subTest(append=append, alias=alias), tempfile.TemporaryDirectory() as tmpdir:
                    path = Path(tmpdir) / "out.txt"
                    destination = Path(tmpdir) / "alias.txt" if alias else path
                    old = open(path, "w", encoding="utf-8")
                    old.write("long buffered previous output\n")
                    if alias:
                        os.link(path, destination)
                    ctx = {"conn": FakeConn(), "output": old}
                    with mock.patch("sys.stderr", new=io.StringIO()):
                        MSSQL.run_dot_command(f".output {quote_dot_arg(destination)}" +
                                              (" --append" if append else ""), ctx)
                    self.assertTrue(old.closed)
                    ctx["output"].write("new\n")
                    ctx["output"].close()
                    self.assertEqual(path.read_text(),
                                     ("long buffered previous output\n" if append else "") + "new\n")

    def test_output_same_file_flush_failure_preserves_handle_and_contents(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "out.txt"
            path.write_text("original", encoding="utf-8")
            old = mock.Mock(name=str(path))
            old.name = str(path)
            old.flush.side_effect = OSError("disk full")
            ctx = {"conn": FakeConn(), "output": old}
            with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.run_dot_command(f".output {quote_dot_arg(path)}", ctx)
            self.assertIs(ctx["output"], old)
            old.close.assert_not_called()
            self.assertEqual(path.read_text(), "original")
            self.assertIn("disk full", stderr.getvalue())

    def test_repl_preserves_required_merge_semicolon(self):
        sql = "MERGE t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET t.x=s.x;"
        ctx = {"conn": FakeConn(), "output": io.StringIO()}
        with mock.patch("builtins.input", side_effect=[sql, EOFError()]), \
             mock.patch.object(MSSQL, "setup_readline"), \
             mock.patch.object(MSSQL, "execute_sql", return_value=True) as execute, \
             mock.patch("sys.stdout", new=io.StringIO()):
            MSSQL.interactive_loop(ctx)
        execute.assert_called_once_with(sql, ctx)

    def test_multiline_identifiers_hide_go_terminators_and_transaction_keywords(self):
        for dialect, opening, closing in (("mssql", "[", "]"), ("mysql", "`", "`")):
            with self.subTest(dialect=dialect):
                sql = f"SELECT {opening}first\nGO\nCOMMIT;\nlast{closing} FROM t;"
                self.assertEqual(MSSQL.split_batches(sql + "\nGO\nSELECT 2;", dialect),
                                 [sql, "SELECT 2;"])
                self.assertFalse(MSSQL._ends_with_statement_terminator(
                    f"SELECT {opening}first\nCOMMIT;".splitlines(), dialect))
                self.assertNotIn("COMMIT", MSSQL._sql_parser_tokens(sql, dialect))
                ctx = {"dialect": dialect}
                with mock.patch("builtins.input", side_effect=sql.splitlines() + [EOFError()]), \
                     mock.patch.object(MSSQL, "setup_readline"), \
                     mock.patch.object(MSSQL, "execute_sql", return_value=True) as execute, \
                     mock.patch("sys.stdout", new=io.StringIO()):
                    MSSQL.interactive_loop(ctx)
                execute.assert_called_once_with(sql, ctx)

    def test_import_csv_header_parser_error_closes_resources_without_truncating(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.csv"
            path.write_text("oversized_header\n1\n", encoding="utf-8")
            cursor = FakeCursor([{"rows": [("id", "int", "NO")]}])
            opened = []
            real_open = open
            def track_open(*args, **kwargs):
                stream = real_open(*args, **kwargs)
                opened.append(stream)
                return stream
            with mock.patch.object(MSSQL, "MAX_CSV_IMPORT_FIELD_CHARS", 4), \
                 mock.patch("builtins.open", side_effect=track_open), \
                 mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                MSSQL.import_data(f"{quote_dot_arg(path)} t --truncate", {"conn": FakeConn(cursor)})
            self.assertIn("Error reading", stderr.getvalue())
            self.assertTrue(all(stream.closed for stream in opened))
            self.assertEqual(cursor.close_count, 1)
            self.assertFalse(any(sql.startswith("TRUNCATE") for sql, _ in cursor.executed))

    def test_streamed_utf8_errors_preserve_prior_batches_and_explicit_transaction(self):
        for fmt in ("csv", "jsonl"):
            for explicit in (False, True):
                with self.subTest(fmt=fmt, explicit=explicit), tempfile.TemporaryDirectory() as tmpdir:
                    path = Path(tmpdir) / ("rows." + fmt)
                    # Put invalid bytes past TextIOWrapper's initial read-ahead.
                    prefix = b"id\n" + b"1\n" * 10000 if fmt == "csv" else b'{"id":1}\n' * 10000
                    path.write_bytes(prefix + b"\xff\n")
                    ctx = self._transaction_ctx()
                    if explicit:
                        MSSQL.execute_sql("BEGIN TRAN", ctx)
                    opened = []
                    real_open = open
                    def track_open(*args, **kwargs):
                        stream = real_open(*args, **kwargs)
                        opened.append(stream)
                        return stream
                    with mock.patch("builtins.open", side_effect=track_open), \
                         mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                        MSSQL.import_data(f"{quote_dot_arg(path)} t --batch 100", ctx)
                    conn = ctx["conn"]
                    self.assertIn("Error reading", stderr.getvalue())
                    self.assertIn("utf-8", stderr.getvalue())
                    self.assertTrue(all(stream.closed for stream in opened))
                    self.assertGreater(len(conn.pending if explicit else conn.durable), 0)
                    self.assertEqual(conn.depth, int(explicit))
                    self.assertEqual(ctx["transaction_depth"], int(explicit))
                    if explicit:
                        self.assertEqual(conn.rollback_count, 0)
                        self.assertTrue(MSSQL.execute_sql("ROLLBACK", ctx))

    def test_repl_output_failure_recovers_for_sql_go_and_dot_commands(self):
        for lines in (["SELECT id FROM t;"], ["SELECT id FROM t", "GO 3"], [".tables"]):
            with self.subTest(lines=lines):
                ctx = self._transaction_ctx()
                conn = ctx["conn"]
                conn.durable = [1]
                MSSQL.execute_sql("BEGIN TRAN", ctx)
                ctx["output"].write = mock.Mock(side_effect=OSError("disk full"))
                with mock.patch("builtins.input", side_effect=lines + [".output -", "COMMIT;", EOFError()]), \
                     mock.patch.object(MSSQL, "setup_readline"), \
                     mock.patch("sys.stdout", new=io.StringIO()) as stdout, \
                     mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.interactive_loop(ctx)
                    self.assertIs(ctx["output"], stdout)
                self.assertIn("Output error", stderr.getvalue())
                self.assertNotIn("Unknown command", stderr.getvalue())
                self.assertEqual(conn.rollback_count, 0)
                self.assertEqual(conn.durable, [1])
                self.assertEqual(conn.depth, 0)
                if lines[-1] == "GO 3":
                    self.assertEqual(sum(sql == "SELECT id FROM t" for sql, _ in conn.executed), 1)

    def test_main_final_output_flush_error_exits_nonzero(self):
        for interactive in (False, True):
            with self.subTest(interactive=interactive):
                output = io.StringIO()
                output.close = mock.Mock(side_effect=OSError("final flush failed"))
                conn = FakeConn()
                def redirect(ctx):
                    ctx["output"] = output
                argv = ["mssql.py", "-U", "app"] + ([] if interactive else ["-c", "SELECT 1", "-o", "out.json"])
                with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)), \
                     mock.patch.object(MSSQL, "interactive_loop", side_effect=redirect), \
                     mock.patch.object(MSSQL, "execute_sql", return_value=True), \
                     mock.patch("builtins.open", return_value=output), \
                     mock.patch("sys.argv", argv), \
                     mock.patch("sys.stdin.isatty", return_value=interactive), \
                     mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    with self.assertRaises(SystemExit) as error:
                        MSSQL.main()
                self.assertEqual(error.exception.code, 1)
                self.assertTrue(conn.closed)
                self.assertIn("Error closing output file", stderr.getvalue())

    def test_rowcounts_and_timers_do_not_corrupt_json_and_dml_toggle_is_respected(self):
        cursor = FakeCursor([{"description": [("id",)], "rows": [(1,)]}])
        ctx = {"conn": FakeConn(cursor), "output": io.StringIO(), "mode": "json",
               "rowcount": True, "timer": True}
        with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            self.assertTrue(MSSQL.execute_sql("SELECT id FROM t", ctx))
        self.assertEqual(json.loads(ctx["output"].getvalue()), [{"id": 1}])
        self.assertIn("(1 row)", stderr.getvalue())
        self.assertIn("Elapsed:", stderr.getvalue())
        for count in (False, True):
            with self.subTest(rowcount=count):
                ctx = {"conn": FakeConn(FakeCursor([{"rowcount": 2}])),
                       "output": io.StringIO(), "rowcount": count}
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    self.assertTrue(MSSQL.execute_sql("UPDATE t SET id=2", ctx))
                self.assertEqual(ctx["output"].getvalue(), "")
                self.assertEqual(stderr.getvalue(), "(2 rows affected)\n" if count else "")

    def test_json_duplicate_columns_are_rejected_even_for_empty_results(self):
        for rows in ([], [(1, 2)]):
            with self.subTest(rows=rows), self.assertRaisesRegex(MSSQL.ResultFormatError, "alias duplicate"):
                MSSQL.format_json(rows, ["id", "id"])

    def test_export_duplicate_json_columns_preserves_overwrite_and_append_targets(self):
        for fmt, append in (("json", False), ("jsonl", False), ("jsonl", True)):
            with self.subTest(fmt=fmt, append=append), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / ("rows." + fmt)
                path.write_text("original\n", encoding="utf-8")
                cursor = FakeCursor([{"description": [("id",), ("id",)]}])
                with mock.patch("sys.stderr", new=io.StringIO()) as stderr:
                    MSSQL.export_data(f"t {quote_dot_arg(path)}" + (" --append" if append else ""),
                                      {"conn": FakeConn(cursor)})
                self.assertIn("alias duplicate", stderr.getvalue())
                self.assertEqual(path.read_text(), "original\n")
                self.assertEqual(list(Path(tmpdir).iterdir()), [path])
                self.assertEqual(cursor.close_count, 1)

    def test_duplicate_json_output_error_still_commits_completed_dml(self):
        ctx = self._transaction_ctx()
        ctx["mode"] = "json"
        cursor_factory = ctx["conn"].cursor
        def duplicate_columns_cursor():
            cursor = cursor_factory()
            execute = cursor.execute
            def execute_with_duplicates(sql, params=()):
                execute(sql, params)
                if "SELECT id FROM t" in sql:
                    cursor.description = [("id",), ("id",)]
                    cursor.rows = [(1, 2)]
            cursor.execute = execute_with_duplicates
            return cursor
        ctx["conn"].cursor = duplicate_columns_cursor
        with mock.patch("sys.stderr", new=io.StringIO()):
            with self.assertRaisesRegex(MSSQL.ResultFormatError, "alias duplicate"):
                MSSQL.execute_sql("INSERT INTO t VALUES (1); SELECT id FROM t", ctx)
        self.assertEqual(ctx["conn"].durable, [1])
        self.assertEqual(ctx["conn"].depth, 0)
        self.assertEqual(ctx["output"].getvalue(), "")

    def test_repl_duplicate_json_labels_allow_mode_change(self):
        cursor = FakeCursor([{"description": [("id",), ("id",)], "rows": [(1, 2)]}])
        ctx = {"conn": FakeConn(cursor), "mode": "json", "output": io.StringIO()}
        with mock.patch("builtins.input", side_effect=["SELECT 1 AS id, 2 AS id;", ".mode csv", EOFError()]), \
             mock.patch.object(MSSQL, "setup_readline"), \
             mock.patch("sys.stdout", new=io.StringIO()), \
             mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            MSSQL.interactive_loop(ctx)
        self.assertIn("Result format error", stderr.getvalue())
        self.assertEqual(ctx["mode"], "csv")
        self.assertEqual(ctx["conn"].rollback_count, 0)

    def test_main_duplicate_json_labels_exit_with_clean_error(self):
        conn = FakeConn(FakeCursor([{"description": [("id",), ("id",)], "rows": [(1, 2)]}]))
        with mock.patch.object(MSSQL, "build_connection", return_value=(conn, "test", FAKE_DRIVER)), \
             mock.patch("sys.argv", ["mssql.py", "-U", "app", "-c", "SELECT 1 AS id, 2 AS id", "--mode", "json"]), \
             mock.patch("sys.stderr", new=io.StringIO()) as stderr:
            with self.assertRaises(SystemExit) as error:
                MSSQL.main()
        self.assertEqual(error.exception.code, 1)
        self.assertTrue(conn.closed)
        self.assertIn("alias duplicate", stderr.getvalue())

if __name__ == "__main__":
    unittest.main()

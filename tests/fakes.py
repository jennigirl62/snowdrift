"""Minimal stand-ins for the parts of PySpark and Snowpark that snowdrift calls.

They let the tests exercise the Spark/Snowpark code paths without either library installed.
They check snowdrift's own logic; they are not proof that the real APIs behave identically.
"""

import io
import re
import sys
import types


class Pred:
    def __init__(self, fn):
        self.fn = fn

    def __and__(self, other):
        return Pred(lambda r: self.fn(r) and other.fn(r))


class Col:
    def __init__(self, name):
        self.name = name

    def __eq__(self, v):
        return Pred(lambda r: r.get(self.name) == v)

    def __lt__(self, v):
        return Pred(lambda r: r.get(self.name) is not None and r.get(self.name) < v)

    __hash__ = object.__hash__


class Max:
    def __init__(self, c):
        self.name = c.name if isinstance(c, Col) else c


class Row(dict):
    def asDict(self):  # noqa: N802 (PySpark name)
        return dict(self)

    def as_dict(self):
        return dict(self)

    def __getitem__(self, k):
        return list(self.values())[k] if isinstance(k, int) else super().__getitem__(k)


class Writer:
    def __init__(self, df):
        self.df, self._mode = df, "errorifexists"

    def mode(self, m):
        self._mode = m
        return self

    def option(self, *a):
        return self

    def saveAsTable(self, name):  # noqa: N802
        self.df.owner._save(name, self.df.rows, self._mode)

    save_as_table = saveAsTable


class DF:
    def __init__(self, owner, rows, name=None):
        self.owner, self.rows, self.name = owner, rows, name

    def __getitem__(self, k):
        return Col(k)

    def filter(self, pred):
        return DF(self.owner, [r for r in self.rows if pred.fn(r)], self.name)

    where = filter

    def agg(self, m):
        vals = [r[m.name] for r in self.rows if r.get(m.name) is not None]
        return DF(self.owner, [{"max": max(vals) if vals else None}])

    select = agg

    def first(self):
        return Row(self.rows[0]) if self.rows else None

    def collect(self):
        return [Row(r) for r in self.rows]

    def delete(self, pred):
        self.owner.tables[self.name] = [r for r in self.owner.tables[self.name] if not pred.fn(r)]

    @property
    def write(self):
        return Writer(self)


class _Base:
    def __init__(self):
        self.tables: dict[str, list[dict]] = {}
        self.queries: list[str] = []
        self.query_rows: list[dict] = []

    def _save(self, name, rows, mode):
        if mode == "overwrite":
            self.tables[name] = [dict(r) for r in rows]
        else:
            self.tables.setdefault(name, []).extend(dict(r) for r in rows)

    def table(self, name):
        return DF(self, [dict(r) for r in self.tables[name]], name)


class FakeSpark(_Base):
    def __init__(self):
        super().__init__()
        self.catalog = types.SimpleNamespace(tableExists=lambda n: n in self.tables)

    def createDataFrame(self, data, schema):  # noqa: N802
        cols = [part.strip().split()[0] for part in schema.split(",")]
        return DF(self, [dict(zip(cols, row, strict=True)) for row in data])

    def sql(self, q, args=None):
        self.queries.append(q)
        if q.startswith("DELETE FROM"):
            name, cond = re.match(r"DELETE FROM (\S+) WHERE (.+)", q).groups()
            preds = []
            for part in cond.split(" AND "):
                col, op, val = re.match(r"(\w+) (=|<) '(.*)'", part).groups()
                preds.append(Col(col) == val if op == "=" else Col(col) < val)
            self.tables[name] = [r for r in self.tables[name] if not all(p.fn(r) for p in preds)]
            return DF(self, [])
        if "ai_query(" in q:
            return DF(self, [{"answer": f"  summary for: {args['prompt'][:20]}  "}])
        return DF(self, self.query_rows)

    @property
    def read(self):
        spark = self

        class Reader:
            def __init__(self):
                self.opts = {}

            def format(self, f):
                self.opts["format"] = f
                return self

            def options(self, **kw):
                self.opts.update(kw)
                return self

            def option(self, k, v):
                self.opts[k] = v
                return self

            def load(self):
                spark.queries.append(self.opts.get("query", ""))
                return DF(spark, spark.query_rows)

        return Reader()


class FakeSession(_Base):
    def __init__(self, files=None):
        super().__init__()
        self.files = files or {}
        session = self

        class FileOps:
            def get_stream(self, path):
                data = session.files[path]
                return io.BytesIO(data if isinstance(data, bytes) else data.encode())

        self.file = FileOps()

    def create_dataframe(self, data, schema):
        cols = [f.name for f in schema.fields]
        return DF(self, [dict(zip(cols, row, strict=True)) for row in data])

    def sql(self, q, params=None):
        self.queries.append(q)
        if q.startswith("TRUNCATE TABLE "):
            self.tables[q.split()[2]] = []
            return DF(self, [])
        m = re.match(r"SHOW TABLES LIKE '(\w+)' IN SCHEMA (\S+)", q)
        if m:
            name = f"{m.group(2)}.{m.group(1)}"
            return DF(self, [{"name": m.group(1)}] if name in self.tables else [])
        if "CORTEX.COMPLETE" in q:
            return DF(self, [{"ANSWER": f" cortex says: {params[0][:15]} "}])
        return DF(self, self.query_rows)


def install_fake_modules():
    """Register fake pyspark / snowflake.snowpark modules so the backends' lazy imports work."""
    fs = types.ModuleType("pyspark.sql.functions")
    fs.col, fs.max = Col, Max
    sp = types.ModuleType("snowflake.snowpark.functions")
    sp.col, sp.max = Col, Max
    st = types.ModuleType("snowflake.snowpark.types")

    class StructField:
        def __init__(self, name, typ):
            self.name, self.datatype = name, typ

    class StructType:
        def __init__(self, fields):
            self.fields = fields

    st.StructField, st.StructType = StructField, StructType
    st.StringType = st.LongType = st.BooleanType = lambda: None
    mods = {
        "pyspark": types.ModuleType("pyspark"),
        "pyspark.sql": types.ModuleType("pyspark.sql"),
        "pyspark.sql.functions": fs,
        "snowflake": types.ModuleType("snowflake"),
        "snowflake.snowpark": types.ModuleType("snowflake.snowpark"),
        "snowflake.snowpark.functions": sp,
        "snowflake.snowpark.types": st,
    }
    mods["pyspark.sql"].functions = fs
    for name, mod in mods.items():
        sys.modules.setdefault(name, mod)

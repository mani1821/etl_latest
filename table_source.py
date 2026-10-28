"""
Tables to validate from ONE query instead of a `tables:` list in the YAML.

    table_source:
      query: |                       # one row per table (the row count can change from run to run)
        SELECT table_name, watermark_from, water_mark_to, load_type, incremental_column, key_list FROM ...
    overrides:                       # optional, per table ('*' = every table)
      BLSBSH: {exclude_cols: [CHANGE_TIMESTAMP]}

Per row:
  keys         key_list not empty -> split on ','; empty / NULL -> no keys (whole-row comparison)
               (if the query also returns key_status: keys only when key_status = pk_value, 'PRIMARY KEY')
  load type    in full_values (FULL)                       -> full comparison of the whole table
               in incremental_values (DELTA, INCREMENTAL)  -> on source AND target:
                   <incremental_column> > '<watermark_from>' AND <incremental_column> <= '<water_mark_to>'
                   (window: between = both ends included), delta_only (default true)
  exclude_cols table_source.exclude_cols + overrides (a key column is never excluded)
Rows that cannot be validated (unknown load type; DELTA without incremental column, watermark_from or
water_mark_to; watermark_from not before water_mark_to) are reported as ERROR rows (check table_source).
Overrides: any table option (keys, exclude_cols, num_tolerance, ts_truncate, target_name, filter, tolerances, ...);
  keys replace the queried keys, exclude_cols are added to the global ones, incremental: {...} is merged
  (e.g. {delta_only: false} = full comparison of the table as well).
Older setup (still works): tables_query + keys_query (key_status / key_list), window from 00:00 of the watermark day.
"""
from __future__ import annotations

import datetime as dt
import re
from typing import Optional

from .delta_yaml import _fmt

# framework name -> column name in the query (table_source.columns changes them)
DEFAULT_COLUMNS = dict(table_name="table_name", load_type="load_type", incr_column="incremental_column",
                       watermark_from="watermark_from", watermark_to="water_mark_to",
                       key_status="key_status", key_list="key_list")
ALLOWED = {"query", "tables_query", "keys_query", "columns", "pk_value", "key_separator", "full_values",
           "incremental_values", "delta_only", "exclude_cols", "window", "window_start"}
WINDOWS = {"exclusive_start", "between"}
_DB2_TS = re.compile(r"^(\d{4}-\d{2}-\d{2})-(\d{2})\.(\d{2})\.(\d{2})(\.\d+)?$")


def check(ts: dict):
    """Validate the table_source block (called by load_config; reads no data)."""
    if not isinstance(ts, dict):
        raise ValueError("table_source must be a block with query: ...")
    unknown = set(ts) - ALLOWED
    if unknown:
        raise ValueError(f"table_source: unknown option(s) {sorted(unknown)}; allowed: {sorted(ALLOWED)}")
    if ts.get("query") and ts.get("tables_query"):
        raise ValueError("table_source: use query OR tables_query, not both")
    cols = dict(ts.get("columns") or {})
    if "watermark" in cols:                                     # 3.13.0 name
        cols.setdefault("watermark_to", cols.pop("watermark"))
    bad = set(cols) - set(DEFAULT_COLUMNS) - {"watermark"}
    if bad:
        raise ValueError(f"table_source.columns: unknown name(s) {sorted(bad)}; allowed: {sorted(DEFAULT_COLUMNS)}")
    if str(ts.get("window", "exclusive_start")) not in WINDOWS:
        raise ValueError(f"table_source.window must be one of {sorted(WINDOWS)}")


def _name(v) -> str:
    return str(v or "").strip().split(".")[-1].upper()


def _blank(v) -> bool:
    return v is None or not str(v).strip() or str(v).strip().lower() == "null"


def _wm(v) -> str:
    """Watermark as Spark SQL timestamp text; DB2 text 2024-10-24-11.36.37.123456 -> 2024-10-24 11:36:37.123456."""
    s = _fmt(v)
    m = _DB2_TS.match(s)
    return f"{m.group(1)} {m.group(2)}:{m.group(3)}:{m.group(4)}{m.group(5) or ''}" if m else s


def _ts(s: str):
    """Parsed watermark for the from < to check (None if it cannot be parsed: no check then)."""
    m = re.match(r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(\.\d+)?", s)
    if not m:
        return None
    try:
        base = dt.datetime.fromisoformat(m.group(1).replace("T", " "))
    except ValueError:
        return None
    frac = (m.group(2) or ".0")[1:]
    return base + dt.timedelta(microseconds=int((frac + "000000")[:6]))


def _collect(spark, query, names: dict, required: tuple, what: str) -> tuple:
    """(rows as dicts keyed by framework name, set of framework names the query returns).
    query: SQL text or a Spark DataFrame."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    df = query if hasattr(query, "collect") else spark.sql(query)
    timeish = (T.TimestampType, T.DateType) + ((T.TimestampNTZType,) if hasattr(T, "TimestampNTZType") else ())
    # timestamps as text inside Spark: collected as Python datetimes they would move to the driver's time zone
    df = df.select(*[F.col(f"`{f.name}`").cast("string").alias(f.name) if isinstance(f.dataType, timeish)
                     else F.col(f"`{f.name}`") for f in df.schema.fields])
    lower = {c.lower(): i for i, c in enumerate(df.columns)}
    idx = {k: lower[str(col).lower()] for k, col in names.items() if str(col).lower() in lower}
    missing = [k for k in required if k not in idx]
    if missing:
        raise ValueError(f"table_source.{what} has no column(s) "
                         + ", ".join(f"{names[k]!r} ({k})" for k in missing)
                         + f"; it returns {df.columns}. Rename them in the query or set table_source.columns")
    return [{k: r[i] for k, i in idx.items()} for r in df.collect()], set(idx)


def build_tables(spark, ts: Optional[dict] = None, overrides: Optional[dict] = None, load_type: Optional[str] = None,
                 only: Optional[set] = None, table_list=None) -> tuple:
    """
    Table dicts (the same shape as a YAML `tables:` entry) from the query.
    table_list: a Spark DataFrame (or SQL text) built in the notebook; used instead of table_source.query.
    load_type: None = every table; 'full' / 'incremental' (or any value of full_values / incremental_values).
    Returns (tables, skipped) with skipped = [(table, reason)].
    """
    ts = dict(ts or {})
    cols = dict(ts.get("columns") or {})
    if "watermark" in cols:
        cols.setdefault("watermark_to", cols.pop("watermark"))
    names = {**DEFAULT_COLUMNS, **{k: v for k, v in cols.items() if v}}
    full_vals = {str(v).strip().upper() for v in (ts.get("full_values") or ["FULL"])}
    inc_vals = {str(v).strip().upper() for v in (ts.get("incremental_values") or ["DELTA", "INCREMENTAL", "INCR"])}
    pk_value = str(ts.get("pk_value", "PRIMARY KEY")).strip().upper()
    sep = ts.get("key_separator", ",")
    delta_only = bool(ts.get("delta_only", True))
    between = str(ts.get("window", "exclusive_start")) == "between"
    global_excl = [str(c).upper() for c in (ts.get("exclude_cols") or [])]
    ov = {str(k).upper(): (v or {}) for k, v in (overrides or {}).items()}

    want = None
    if load_type:
        lt = str(load_type).strip().upper()
        want = "FULL" if lt in full_vals or lt == "FULL" else \
            "INCREMENTAL" if lt in inc_vals or lt.startswith("INC") else None
        if not want:
            raise ValueError(f"load_type must be full or incremental (or one of {sorted(full_vals | inc_vals)}), "
                             f"got {load_type!r}")

    def parse_keys(status, klist, has_status):
        if has_status and str(status or "").strip().upper() != pk_value:
            return []
        return [c.strip().upper() for c in str(klist or "").split(sep) if c.strip() and c.strip().lower() != "null"]

    # ---- rows
    query = table_list if table_list is not None else (ts.get("query") or ts.get("tables_query"))
    what = "table_list" if table_list is not None else "query" if ts.get("query") else "tables_query"
    if query is None:
        raise ValueError("no table list: pass run_parity(..., table_list=<DataFrame>) or set table_source.query")
    rows, have = _collect(spark, query, names, ("table_name", "load_type"), what)
    legacy = "watermark_from" not in have           # 3.13.0 style: window from 00:00 of the watermark day
    keys_from_query = "key_list" in have
    keys = {}
    if ts.get("keys_query"):                         # older setup: separate keys query
        krows, khave = _collect(spark, ts["keys_query"], names, ("table_name", "key_list"), "keys_query")
        for r in krows:
            name = _name(r["table_name"])
            ks = parse_keys(r.get("key_status"), r["key_list"], "key_status" in khave)
            if name and (name not in keys or (ks and not keys[name])):
                keys[name] = ks

    tables, skipped, seen = [], [], set()
    for r in rows:
        name = _name(r["table_name"])
        if not name or (only and name not in only):
            continue
        if name in seen:
            skipped.append((name, "listed more than once in the query: first row used"))
            continue
        seen.add(name)
        lt = str(r["load_type"] or "").strip().upper()
        kind = "FULL" if lt in full_vals else "INCREMENTAL" if lt in inc_vals else None
        if kind is None:                             # reported once: in the full (or unfiltered) run
            if want != "INCREMENTAL":
                skipped.append((name, f"load_type {r['load_type']!r} is neither {sorted(full_vals)} "
                                      f"nor {sorted(inc_vals)}"))
            continue
        if want and kind != want:
            continue

        o = dict(ov.get("*") or {})
        for k, v in (ov.get(name) or {}).items():    # table override on top of '*'
            o[k] = {**(o.get(k) or {}), **v} if k == "incremental" and isinstance(v, dict) else v

        if keys_from_query:
            tkeys = parse_keys(r.get("key_status"), r.get("key_list"), "key_status" in have)
        else:
            tkeys = keys.get(name, [])
        if o.get("keys") is not None:
            tkeys = [str(k).upper() for k in o["keys"]]
        o.pop("keys", None)
        entry = {"name": name, "keys": tkeys}
        excl = global_excl + [str(c).upper() for c in (o.pop("exclude_cols", None) or [])]
        entry["exclude_cols"] = [c for c in dict.fromkeys(excl) if c not in tkeys]

        inc_over = o.pop("incremental", None)
        if kind == "INCREMENTAL":
            col = "" if _blank(r.get("incr_column")) else str(r["incr_column"]).strip()
            wm_to = None if _blank(r.get("watermark_to")) else _wm(r["watermark_to"])
            if not col or not wm_to:
                skipped.append((name, f"{lt} table without "
                                      + " / ".join(n for n, v in ((names["incr_column"], col),
                                                                  (names["watermark_to"], wm_to)) if not v)))
                continue
            if legacy:
                start = ts.get("window_start") or f"{wm_to[:10]} 00:00:00"
                where = f"{col} BETWEEN '{start}' AND '{wm_to}'"
            else:
                if _blank(r.get("watermark_from")):
                    skipped.append((name, f"{lt} table without {names['watermark_from']} "
                                          f"({names['watermark_to']} = {wm_to})"))
                    continue
                wm_from = _wm(r["watermark_from"])
                a, b = _ts(wm_from), _ts(wm_to)
                if a and b and a >= b:
                    skipped.append((name, f"{names['watermark_from']} {wm_from} is not before "
                                          f"{names['watermark_to']} {wm_to}"))
                    continue
                where = (f"{col} BETWEEN '{wm_from}' AND '{wm_to}'" if between
                         else f"{col} > '{wm_from}' AND {col} <= '{wm_to}'")
            entry["incremental"] = {"source_delta": where, "target_delta": where,
                                    "delta_only": delta_only, **(inc_over or {})}
        elif inc_over:
            print(f"  {name}: full-load table, overrides.incremental ignored")
        entry.update(o)                              # tolerances, ts_truncate, target_name, filter, ...
        tables.append(entry)
    for n in sorted(set(ov) - seen - {"*"}):
        if not only or n in only:
            print(f"  overrides.{n}: table not in the query (ignored)")
    return tables, skipped


def describe(tables: list, skipped: list, load_type: Optional[str]) -> str:
    lines = [f"tables from table_source ({load_type or 'all load types'}): {len(tables)} to validate"
             + (f", {len(skipped)} not validated" if skipped else "")]
    for t in tables:
        inc = t.get("incremental")
        lines.append(f"  {t['name']:<24} {'INCREMENTAL' if inc else 'FULL':<12} keys {t['keys'] or '[] (whole-row)'}"
                     + (f"   {inc['source_delta']}" + ("" if inc.get('delta_only', True) else "  (+ full table)")
                        if inc else ""))
    for n, why in skipped:
        lines.append(f"  {n:<24} ERROR        {why}")
    return "\n".join(lines)

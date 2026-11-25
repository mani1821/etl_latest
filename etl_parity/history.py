"""
History-table checks: tables that keep every version of a record (grain = business id + version, e.g.
CIRCUIT_DESIGN_ID + LAST_MODIFIED_DATE; latest version of an id = the highest version).

    history: {id: CIRCUIT_DESIGN_ID, version: LAST_MODIFIED_DATE, hash: MD5_CHECK_SUM}

Checks (validation "history"), always over the WHOLE table (never sampled), one pass per side:
  versions_per_id     number of versions of every id, source vs target            (history.tolerance_pct)
  latest_version      latest version of every id and its hash, source vs target  (history.tolerance_pct)
  redundant_versions  consecutive versions of an id with the same hash (a version without a real change);
                      FAIL when the target has more than the source
  run_activity        (needs an incremental block) rows the run touched, by kind, source vs target counts:
                        run_new_ids           rows of ids whose every version is in the run (new ids; an
                                              existing id whose every version was rewritten also lands here)
                        run_new_versions      versions newer than every version the run did not touch
                        run_older_versions    versions not newer than an untouched version of the same id
                                              (in-place updates of history, or late-arriving versions)
                      (delta count tolerance)
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .checks import key_canon, norm_col

SEP = "|"


def _id(cols: list, types: dict, ts_truncate: Optional[str]):
    """The id as one text value, equal on both sides whenever the comparison sees it equal (5 = 5.00 = '5')."""
    parts = []
    for c in cols:
        v = key_canon(F.col(c), types[c], ts_truncate)
        v = F.regexp_replace(F.regexp_replace(v, r"(\.[0-9]*?)0+$", "$1"), r"\.$", "")   # 5.0000000000 -> 5
        parts.append(F.coalesce(v, F.lit("<NULL>")))
    return F.concat_ws(SEP, *parts)


def _where(*conds) -> Optional[str]:
    conds = [c for c in conds if c]
    return " AND ".join(f"({c})" for c in conds) if conds else None


def per_id(reader, table: str, h, filt: Optional[str], delta: Optional[str], ts_truncate: Optional[str]) -> DataFrame:
    """One row per id: versions, latest version (+ hash), redundant versions, and (with delta) the run's rows by kind."""
    cols = list(dict.fromkeys(h.id + [h.version] + ([h.hash] if h.hash else [])))
    df = reader.read(table, cols, filt)
    types = {f.name: f.dataType for f in df.schema.fields}
    hid = _id(h.id, types, ts_truncate)
    ver = norm_col(F.col(h.version), types[h.version], ts_truncate)
    hsh = F.upper(F.trim(F.col(h.hash).cast("string"))) if h.hash else F.lit(None).cast("string")
    rows = df.select(hid.alias("hid"), ver.alias("ver"), hsh.alias("hsh"))

    if delta:                                         # rows this run wrote: flag them on the full table
        d = reader.read(table, list(dict.fromkeys(h.id + [h.version])), _where(filt, delta))
        dt = {f.name: f.dataType for f in d.schema.fields}
        dhid = _id(h.id, dt, ts_truncate)
        d = d.select(dhid.alias("hid"), norm_col(F.col(h.version), dt[h.version], ts_truncate).alias("ver")) \
             .dropDuplicates().withColumn("touched", F.lit(True))
        rows = rows.join(F.broadcast(d), ["hid", "ver"], "left").fillna({"touched": False})
    else:
        rows = rows.withColumn("touched", F.lit(False))

    w = Window.partitionBy("hid").orderBy("ver")
    wall = Window.partitionBy("hid")
    rows = (rows.withColumn("prev_hsh", F.lag("hsh").over(w))
                .withColumn("untouched_max", F.max(F.when(~F.col("touched"), F.col("ver"))).over(wall)))
    kind = (F.when(~F.col("touched"), F.lit(None))
             .when(F.col("untouched_max").isNull(), F.lit("new_id"))
             .when(F.col("ver") > F.col("untouched_max"), F.lit("new_version"))
             .otherwise(F.lit("older_version")))
    rows = rows.withColumn("kind", kind)
    return rows.groupBy("hid").agg(
        F.count(F.lit(1)).alias("n"),
        F.max("ver").cast("string").alias("last_ver"),
        F.max_by("hsh", "ver").alias("last_hsh"),
        F.sum(F.when(F.col("hsh").isNotNull() & (F.col("hsh") == F.col("prev_hsh")), 1).otherwise(0)).alias("redundant"),
        F.sum(F.when(F.col("kind") == "new_id", 1).otherwise(0)).alias("new_ids"),
        F.sum(F.when(F.col("kind") == "new_version", 1).otherwise(0)).alias("new_versions"),
        F.sum(F.when(F.col("kind") == "older_version", 1).otherwise(0)).alias("older_versions"))


def compare(src: DataFrame, tgt: DataFrame) -> DataFrame:
    """Source and target per-id summaries side by side (full outer join on the id)."""
    s = src.select("hid", *[F.col(c).alias(f"s_{c}") for c in src.columns if c != "hid"])
    t = tgt.select("hid", *[F.col(c).alias(f"t_{c}") for c in tgt.columns if c != "hid"])
    return s.join(t, "hid", "full")

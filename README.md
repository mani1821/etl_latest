# etl_parity

Validates that a migrated ETL produces the same tables as the existing one. Both sides are read into
Spark (from Oracle or from Databricks tables) and compared there.

## Layout
```
etl_parity/              the package (code) - users only call run_parity()
  __init__.py            exposes run_parity, version
  runner.py              the flow per table  <- start reading here
  config.py              YAML, environments, ${PARAM} placeholders, table specs
  readers.py             Oracle (JDBC) and Databricks table readers
  sampling.py            full read or sample-and-lookup
  checks.py              schema, row comparison, column profiles
  results.py             results / mismatch tables, summary
  keys.py                key discovery for tables without declared keys
configs/tables.yaml      what to compare (one file, all environments) - commented template
notebooks/run_parity.py          run it (widgets: config, env, tables, params)
notebooks/create_sample_data.py  sample *_src / *_tgt tables with planted differences
notebooks/discover_keys.py       propose key columns for tables without declared keys
pyproject.toml           only needed to build a wheel
```

## What runs per table
| validation | checks |
|---|---|
| schema | columns, types (Oracle `ALL_TAB_COLUMNS` / Unity Catalog `information_schema.columns`) |
| row_count | exact `COUNT(*)` on both sides (with the table's `filter`); `count_tolerance_pct`: within = WARN |
| row_hash | null keys (src, tgt), duplicate keys (src, tgt; the duplicated keys are left out of the value comparison, every other key is compared), missing keys, extra keys (both with `count_tolerance_pct`: % of the keys compared, within = WARN), hash match, one `column_diff:<COL>` per differing column (that column's % of differing rows vs `row_mismatch_tolerance_pct`: within = WARN, beyond = FAIL) |
| column_profiles | one check per non-key column: nulls and distinct (every column), min/max text length (text), min/max (date, timestamp); no numeric min/max/sum - on rows whose key exists on both sides |
| column_distributions | one check per text / date / timestamp column: row counts per value (text with <= 1,000 distinct values), per text length (more distinct values), per day or month, and per hour of day (timestamps) - same rows as the profiles; a bucket may differ by `distribution_tolerance_pct` % or `distribution_tolerance_rows` rows (within either = WARN) |
| incremental | delta row count, delta keys, delta values (tables with an `incremental:` block); each with a tolerance (`delta_count_tolerance_pct`, `delta_key_tolerance_pct`, `delta_value_tolerance_pct`, or per table `count_tolerance_pct` / `key_tolerance_pct` / `value_tolerance_pct` in the incremental block): within tolerance = WARN |

**Which rows `row_hash` compares**: every row if the table has <= `sample_threshold_rows` (500,000), otherwise
a sample of `sample_pct` (10%) capped at `max_sample_rows` (100,000) per side, picked by **sample-and-lookup**
in both directions:
1. sample the source (Oracle `SAMPLE BLOCK`: reads only the sampled blocks), fetch the same keys from the target;
2. sample the target, fetch those keys from the source.

Fetching keys from Oracle uses the key's index when there is one (batched `IN` lists); without an index the table is
read once in ROWID-range slices with the key list. The `sampling` INFO row in the results says which was used.
Large deltas are sampled the same way.

## Use it on Databricks
**Option A - shared folder (no build):** put this folder in the workspace (or a Git folder), then
```python
import sys; sys.path.append("/Workspace/Shared/etl_parity")      # the folder that CONTAINS etl_parity/
from etl_parity import run_parity
run = run_parity("/Workspace/Shared/etl_parity/configs/tables.yaml", env="databricks",
                 params={"SRC_BATCH_ID": "101", "TGT_BATCH_ID": "9001",          # values matching the sample data
                         "SRC_RUN_START": "2026-10-13 02:00:00", "TGT_RUN_START": "2026-10-13 04:00:00"})
display(run.summary)
```
or open `notebooks/run_parity.py` and fill in the widgets.

**Option B - wheel (versioned):** `pip install build && python -m build` -> `dist/etl_parity-2.0.0-py3-none-any.whl`.
Upload it and install with `%pip install <path>.whl`, as a cluster library, a job task library, or a serverless
environment dependency. Then `from etl_parity import run_parity` - no path setup.
Job: a "Python wheel" task, entry point `etl-parity`, parameters `["<config>", "--env", "oracle", "--param", "SRC_BATCH_ID=101"]`;
the task fails (exit code 1) when any check FAILs.

**Compute**: `env=databricks` runs on serverless or classic. `env=oracle` needs a classic cluster with the Oracle JDBC
driver (Maven `com.oracle.database.jdbc:ojdbc11`) and network access to both databases. Passwords: `password_secret`
(Databricks secret scope/key) or `password_env`.

**Writes**: only the two result tables named in `settings` (created on first run, appended after). No catalog,
schema or volume is created. The sample-data notebook writes `<schema>.<table>_src` / `_tgt` into an existing schema.

## Parallel tables
`parallel_tables: 4` in `settings` (or `run_parity(..., parallel_tables=4)`, job flag `--parallel 4`) validates 4 tables
at the same time, as threads sharing one Spark session; results are identical to a one-by-one run. It helps most with
many small / medium tables, or when Oracle reads leave the cluster idle; one big table can already use the whole cluster.
With `env=oracle` each table opens its own JDBC sessions (up to read_partitions each): check the database's session limit.
Example bad rows are collected per table and written with the results at the end of the run (one write per run).

## Run report and e-mail
Every run builds a report: `run.report_html` (in a notebook: `displayHTML(run.report_html)`) and `run.report_text`.
`settings.report_path` (or `run_parity(..., report_path=...)`) also writes it as `parity_<run_id>.html`;
`settings.email` (or `run_parity(..., email_to="a@x.com,b@y.com")`, job flag `--email-to`) mails it through the SMTP
relay (plain text + HTML). `send_on: always | issues | failure | never`. A mail error never fails the run:
it is printed and kept in `run.email_error`.

The e-mail is the short version: run summary, PASS / FAIL per table and validation, and a link to the
Databricks job run that sent it (its notebook output shows the full report with checks, row counts and failures).
`settings.email.report_link` overrides the link. `run.report_html` / `report.report_html` = full report,
`run.email_html` / `report.email_html` = the e-mail version.

## Incremental YAML generated from a watermark query: `build_delta_yaml`
```python
from etl_parity import build_delta_yaml, run_parity
delta_yaml = build_delta_yaml(base_config=FULL_YAML,      # keys / exclude_cols / settings
                              watermarks="SELECT table_name, incr_column, watermark FROM ...",
                              out_path=f"{ROOT}/configs/icd_delta_generated.yaml")
delta = run_parity(delta_yaml, env="databricks", send_email=False)
```
Per table: `<column> BETWEEN '<watermark day> 00:00:00' AND '<watermark>'` (`start=` overrides the start).
Watermarks are turned into text inside Spark (session time zone), so they match the filter exactly.

## One notebook for 1 or 2 YAMLs: `notebooks/validate_and_report.py`
Widgets `full_yaml` and / or `delta_yaml` (empty = skipped), env, params per YAML, optional tables / parallel_tables /
email_to. It validates each YAML, sends ONE summary e-mail (settings.email of the first YAML that has it) and shows the
detailed report as its output; as a workflow task, the e-mail link opens this run.

## One report for several runs (full-load YAML + incremental YAML)
```python
from etl_parity import run_parity, send_report
full  = run_parity(full_yaml,  env="databricks", send_email=False)
delta = run_parity(delta_yaml, env="databricks", send_email=False)
send_report([full, delta], config=full_yaml)          # one combined e-mail; [full] or [delta] alone works too
```
Runs made separately (other notebooks / sessions): `send_report(latest=[full_yaml, delta_yaml], config=full_yaml)`
takes the latest run of each YAML from the last 24 h (`since_hours`); a YAML that did not run is skipped.
`send_report` also takes run ids (strings) and skips empty entries, and reads the runs back from `parity_results`, so it
can be the last task of a job (`notebooks/send_report.py`, **Run if: All done**): each validation task publishes its
run id as task value `run_id`; the report task collects the ones that ran. Each run also writes one `summary` INFO row
per table per validation and one `run_info` row (table_name `__run__`) to `parity_results` for this.

## Results
- Tables without keys: for every failing distribution column, up to `mismatch_sample_limit` whole rows per side from
  the failing buckets that have no identical row on the other side: kind `distribution_rows`, `key` = the row as JSON,
  `column`, `src_value` = the bucket (e.g. `day 2026-08-01`), `tgt_value` = `only in source` / `only in target`.
- Every comparison records how many rows it compared: INFO check `rows_compared` (row_hash) / `delta_rows_compared`
  (incremental), `src_value` / `tgt_value` = rows compared, detail = sample % of the table (or "all rows"). The summary
  line says the same: `sample 114,109 vs 114,104 rows: ...` or `all 2,000 vs 1,999 rows: ...`.
- `row_copy_counts` (no keys): `src_value` / `tgt_value` = copies of the affected rows in source / target.
- `parity_results`: one row per check (`run_id, run_ts, env, table_name, validation, scope, check_name, status, src_value, tgt_value, detail`).
- `parity_mismatches`: example bad rows (`kind` = missing_in_target, extra_in_target, value_mismatch, delta_...; `key` as JSON; `column`, `src_value`, `tgt_value` for value differences).
- The console / `run.summary` shows one row per table per validation with pass/fail counts.

Status of a validation = worst of its checks: ERROR > FAIL > WARN > SKIP > PASS (INFO rows are not counted).

## Key discovery (`etl_parity.keys`)
For tables without declared keys, `discover_keys(spark, tables)` proposes the key per table, without brute-force
distinct counts:
0. metadata: declared (informational) primary key if any; technical columns (`*_SK`, `ETL_*`, batch ids, load
   timestamps) are set aside and suggested as `exclude_cols`;
1. profile ~1% of the table's files: drop columns with NULLs, constants, floats, long text;
2. one pass over the full table, reading only the candidate columns, with HyperLogLog distinct estimates for every
   column, then pairs, then triples (no shuffle);
3. exact uniqueness check: every row for tables up to 5M rows, otherwise 10 of 100 hash buckets of the key
   (duplicates of a key always share a bucket);
4. source key values are looked up in the target: surrogate keys generated by each ETL fail this test.
Output: `parity_key_candidates` table (key, confidence, evidence, alternatives) and `to_yaml()` entries to review.

## Tables without keys
Leave `keys` empty (`keys: []`) and rows are matched by **all compared columns**: each distinct row is counted on
both sides. Checks under `row_hash`: `rows_only_in_src`, `rows_only_in_tgt`, `row_copy_counts` (the same row appears
a different number of times) - exact, also with fully duplicated rows.

- Small tables (<= sample_threshold_rows): every row compared directly (`row_hash [rows]`).
- Big tables (v3 default, `keyless_method: fingerprint`, `row_hash [fingerprint]`):
  1. one pass over every row: hash each row, put it in a bucket (hash mod N), and per bucket keep
     COUNT(*) and SUM(hash) - a fingerprint of that bucket's rows (only N small rows per side are shuffled);
  2. compare the fingerprints of both sides: equal = every row in the bucket matches (check `bucket_fingerprints`);
  3. only the rows of differing buckets are compared row by row (up to `max_detail_rows`), giving the exact rows
     only in source / only in target / with different copy counts.
  Every row of the table is checked; the cost is one scan per side (the v2 sample also needed a full scan).
  `keyless_method: sample` restores the v2 behaviour (10% of rows by row-hash buckets).
- A changed value shows as one row only in source + one only in target (no column-level diff without keys).
- Profiles run over all rows; incremental runs delta row count + delta rows (fingerprinted when big).

## Not in this version
SCD2 history checks (planned). The Oracle path has not yet been run against a real database; its SQL was checked
with a stubbed connection. Run it on one small and one large table first.

## 3.13.0: tables from one query (`table_source`)

Instead of a `tables:` list, the YAML holds one query (see `configs/icd_dpi.yaml`) returning, per table:
`table_name, watermark_from, water_mark_to, load_type, incremental_column, key_list`.

* keys: `key_list` split on `,`; empty / NULL = no keys (whole-row comparison)
* `load_type` FULL: full comparison; DELTA: `<incremental_column> > '<watermark_from>' AND <= '<water_mark_to>'` on
  source and target, `delta_only: true` (DB2-style watermarks such as `2026-10-26-10.00.00.000000` are converted)
* not validated, reported as ERROR rows (`check_name = table_source`): unknown load type; DELTA without
  incremental column / watermark_from / water_mark_to; watermark_from not before water_mark_to
* `overrides:` sets options per table (`keys` replace, `exclude_cols` add, `incremental` merges)

    tables_df = spark.sql("""<table-list query>""")          # or: table_source.query in the YAML
    full  = run_parity(YAML, env="databricks", load_type="full",        table_list=tables_df, send_email=False)
    delta = run_parity(YAML, env="databricks", load_type="incremental", table_list=tables_df, send_email=False)
    send_report([full, delta], config=YAML)
    # or later: send_report(latest=[f"{YAML}#full", f"{YAML}#incremental"], config=YAML)

## 3.14.0: stored source counts, CHAR vs VARCHAR

* `settings.row_count_source: counts` + `run_parity(..., counts=counts_df)` (DataFrame with `table_name, row_count`,
  or `settings.counts_query`): `row_count` (FULL) and `delta_row_count` (DELTA) compare the stored source count with
  the live target count, with the usual tolerances. Tables not in the counts: live source count. `live` = as before.
* `settings.schema_type_warn` (default `[[char, varchar], [char, varchar2], [char, string], [nchar, nvarchar2]]`):
  type differences in these pairs make `types` WARN instead of FAIL (any length, either direction).

## 3.14.1: every tolerance per table

`count_tolerance_pct`, `row_mismatch_tolerance_pct`, `delta_count_tolerance_pct`, `delta_key_tolerance_pct`,
`delta_value_tolerance_pct`, `distribution_tolerance_pct`, `distribution_tolerance_rows` can be set per table (in
`overrides:` or a `tables:` entry) under the same name; the table's value wins over `settings`. For DELTA tables the
table's `incremental:` block (`count_tolerance_pct`, `key_tolerance_pct`, `value_tolerance_pct`) wins over both.

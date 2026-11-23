# Databricks notebook source
# Oracle-to-Oracle incremental parity test, with configs/oracle_incremental_test.yaml
#   1. setup  2. passwords  3. watermarks  4. connection check  5. validation  6. report  7. failed checks
# Fill the <...> placeholders in the YAML first. Do NOT commit this notebook with real passwords in it.

# COMMAND ----------

# 1. setup: the repo folder is the parent of notebooks/
import os
import sys

ROOT = os.path.dirname(os.getcwd())
sys.path.insert(0, ROOT)
import etl_parity
from etl_parity import run_parity
from etl_parity.config import load_config

YAML = f"{ROOT}/configs/oracle_incremental_test.yaml"
print("etl_parity", etl_parity.__version__, "from", os.path.dirname(etl_parity.__file__))

# COMMAND ----------

# 2. passwords: read by password_env: SRC_DB_PWD / TGT_DB_PWD in the YAML
#    (they live only in this notebook session; clear the cell output before sharing the notebook)
os.environ["SRC_DB_PWD"] = "<SOURCE_PASSWORD>"
os.environ["TGT_DB_PWD"] = "<TARGET_PASSWORD>"

# COMMAND ----------

# 3. the incremental window: rows with  from < incremental column <= to  on both sides
PARAMS = {
    "WM_FROM": "2026-11-22 00:00:00",
    "WM_TO":   "2026-11-23 00:00:00",
}

# COMMAND ----------

# 4. connection check: login + table + delta row count on both sides, with the YAML's own settings
cfg = load_config(YAML, "oracle")
t = cfg.tables[0]
filled = {k: str(v) for k, v in PARAMS.items()}


def fill(text):
    for k, v in filled.items():
        text = text.replace("${" + k + "}", v)
    return text


for side, conn, table, delta in (("SOURCE", cfg.source, t["name"], t["incremental"]["source_delta"]),
                                 ("TARGET", cfg.target, t.get("target_name") or t["name"], t["incremental"]["target_delta"])):
    try:
        q = (f"SELECT COUNT(*) AS total, SUM(CASE WHEN {fill(delta)} THEN 1 ELSE 0 END) AS in_window "
             f"FROM {conn.schema}.{table}")
        r = (spark.read.format("jdbc").option("url", conn.url).option("user", conn.user)
             .option("password", conn.secret(spark)).option("driver", "oracle.jdbc.OracleDriver")
             .option("query", q).load().first())
        print(f"OK    {side}: {conn.user} @ {conn.url}\n      {conn.schema}.{table}: "
              f"{int(r['TOTAL']):,} rows, {int(r['IN_WINDOW'] or 0):,} in the window")
    except Exception as e:
        print(f"FAIL  {side}: {conn.user} @ {conn.url}\n      {str(e).splitlines()[0][:300]}")

# COMMAND ----------

# 5. validation (one run id); send_email=True to e-mail the report (needs settings.email in the YAML)
run = run_parity(YAML, env="oracle", params=PARAMS, send_email=False)
print("run id:", run.run_id)

# COMMAND ----------

# 6. report
displayHTML(run.report_html)

# COMMAND ----------

# 7. every check that did not pass, with example rows in the mismatch table
s = cfg.settings
display(spark.sql(f"""
    SELECT table_name, validation, check_name, status, src_value, tgt_value, detail
    FROM {s.results_table}
    WHERE run_id = '{run.run_id}' AND status NOT IN ('PASS', 'INFO')
    ORDER BY validation, check_name"""))
display(spark.sql(f"""
    SELECT validation, kind, key, `column`, src_value, tgt_value
    FROM {s.mismatch_table}
    WHERE run_id = '{run.run_id}'
    LIMIT 200"""))

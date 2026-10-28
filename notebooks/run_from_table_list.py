# Databricks notebook source
# 1. setup: the repo folder is the parent of notebooks/ (works in a Git folder or an uploaded copy)
import os
import sys

ROOT = os.path.dirname(os.getcwd())
sys.path.insert(0, ROOT)
from etl_parity import run_parity, send_report
import etl_parity

YAML = f"{ROOT}/configs/icd_dpi.yaml"
print("etl_parity", etl_parity.__version__, "from", os.path.dirname(etl_parity.__file__))

# COMMAND ----------

# 2. the table list: one row per table
#    table_name, watermark_from, water_mark_to, load_type, incremental_column, key_list
tables_df = spark.sql("""
    <your query>
""")
display(tables_df)

# COMMAND ----------

# 3. all tables (FULL and DELTA), one run id
run = run_parity(YAML, env="databricks", table_list=tables_df, send_email=False)

# COMMAND ----------

# 4. e-mail + full report
report = send_report([run], config=YAML)
displayHTML(report.report_html)

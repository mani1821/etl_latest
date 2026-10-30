# Databricks notebook source
# Oracle connection check: run before the validations. Each cell checks one more step
# (network -> login -> table access), so the first failing cell tells you what is wrong.
#
#   message                                         meaning
#   host name ... not found (DNS)                   wrong host, or the cluster cannot resolve it (try adding .corp.pvt)
#   not reachable / timeout                         firewall: no route from the cluster to host:1521
#   ClassNotFoundException oracle.jdbc.OracleDriver the Oracle JDBC driver (ojdbc8) is not on the cluster
#   ORA-01017 invalid username/password             wrong user, or the wrong secret for that user
#   ORA-12514 service not known                     wrong service name
#   ORA-00942 table or view does not exist          logged in, but the table is not in that schema / no SELECT grant
#   Secret does not exist / permission denied       wrong scope / key name, or no read access to the scope

# COMMAND ----------

# 1. connection details: fill in the scope / key names (and the PROD user)
DBS = {
    "TEST (Talend)": dict(
        host="odwinfwldtp01", port=1521, service="odstst01.corp.pvt",
        user="edw_talend", schema="STAGE_TQ_SBR",
        scope="<scope>", key="<key>"),
    "PROD (Informatica)": dict(
        host="odwinfwldpp03.corp.pvt", port=1521, service="odsprd01.corp.pvt",
        user="<prod user>", schema="CE_CORE",
        scope="<scope>", key="<key>"),
}
TABLE = "FACT_ORD_TASK_JPRDY_INTVL_SUM"

# COMMAND ----------

# 2. network: can the cluster reach the host and port?
import socket

for name, d in DBS.items():
    try:
        ip = socket.gethostbyname(d["host"])
        socket.create_connection((d["host"], d["port"]), timeout=10).close()
        print(f"OK    {name}: {d['host']} ({ip}) port {d['port']} reachable")
    except socket.gaierror:
        print(f"FAIL  {name}: host name {d['host']} not found (DNS); try adding .corp.pvt")
    except Exception as e:
        print(f"FAIL  {name}: {d['host']}:{d['port']} not reachable: {type(e).__name__}: {e}")

# COMMAND ----------

# 3. login: do the user and password work?
def jdbc(d, query):
    return (spark.read.format("jdbc")
            .option("url", f"jdbc:oracle:thin:@//{d['host']}:{d['port']}/{d['service']}")
            .option("user", d["user"])
            .option("password", dbutils.secrets.get(d["scope"], d["key"]))
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("query", query).load())


for name, d in DBS.items():
    try:
        r = jdbc(d, "SELECT USER AS login, SYS_CONTEXT('USERENV','DB_NAME') AS db, "
                    "SYS_CONTEXT('USERENV','SERVICE_NAME') AS service FROM dual").first()
        print(f"OK    {name}: logged in as {r['LOGIN']} on {r['DB']} ({r['SERVICE']})")
    except Exception as e:
        print(f"FAIL  {name}: {str(e).splitlines()[0][:300]}")

# COMMAND ----------

# 4. table access: can the user read the table? rows, columns, primary key (use it as keys: in the YAML)
for name, d in DBS.items():
    try:
        n = jdbc(d, f"SELECT COUNT(*) AS n FROM {d['schema']}.{TABLE}").first()["N"]
        cols = jdbc(d, f"SELECT column_name, data_type FROM all_tab_columns "
                       f"WHERE owner = '{d['schema'].upper()}' AND table_name = '{TABLE}' ORDER BY column_id").collect()
        pk = jdbc(d, f"SELECT cc.column_name FROM all_constraints c JOIN all_cons_columns cc "
                     f"ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name "
                     f"WHERE c.owner = '{d['schema'].upper()}' AND c.table_name = '{TABLE}' "
                     f"AND c.constraint_type = 'P' ORDER BY cc.position").collect()
        print(f"OK    {name}: {d['schema']}.{TABLE}: {int(n):,} rows, {len(cols)} columns, "
              f"PK {[r[0] for r in pk] or 'none'}")
    except Exception as e:
        print(f"FAIL  {name}: {str(e).splitlines()[0][:300]}")

# ───────────────────────── 2. CATÁLOGO, ESQUEMAS Y VOLUME ─────────────────────────
try:
    spark.conf.set("spark.sql.session.timeZone", "America/El_Salvador")
except Exception:
    pass

if RESET:
    for s in ("gold", "silver", "bronze"):
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{s} CASCADE")

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    existentes = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    if CATALOG not in existentes:
        raise Exception(f"No pude crear el catálogo '{CATALOG}'. Cambia CATALOG = 'workspace' en la Celda 1 y vuelve a correr.") from e

spark.sql(f"USE CATALOG {CATALOG}")
for s in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# ───────────────────────── 10. VALIDACIONES Y IDEMPOTENCIA ─────────────────────────
from datetime import datetime, timezone
from pyspark.sql import functions as F

def snapshot():
    c = spark.table(T_CLEAN)
    return {
        "silver_events":       c.count(),
        "silver_distinct_ids": c.select("event_id").distinct().count(),
        "quarantine":          spark.table(T_QUAR).count(),
        "collisions":          spark.table(T_COLL).count(),
        "total_current_stock": spark.table(T_INV).agg(F.sum("current_stock")).first()[0],
        "units_sold":          spark.table(T_INV).agg(F.sum("units_sold")).first()[0],
        "silver_checksum":     c.agg(F.sum(F.pmod(F.xxhash64("event_id", "resolution_status", "stock_before", "stock_after"), F.lit(2147483647)))).first()[0],
    }

antes = snapshot()
bronze_antes = spark.table(T_RAW).count()

# Re-enviar exactamente todos los datos otra vez para probar idempotencia
replay_ts = datetime.now(timezone.utc).replace(microsecond=0)
append_to_bronze([r[:7] + (replay_ts, r[8]) for r in HISTORY + LIVE])
run_pipeline()

despues = snapshot()

print("\n--- PRUEBAS AUTOMÁTICAS DE RÚBRICA ---")
def check(nombre, condicion):
    print(("✅ " if condicion else "❌ ") + nombre)

c = spark.table(T_CLEAN)
p99 = {r["channel"]: r["resolution_status"] for r in c.filter("product_id = 'P099' AND event_type = 'SALE'").collect()}

check("Sin event_id duplicados en Silver", c.count() == c.select("event_id").distinct().count())
check("Ningún estado PENDING sin resolver", c.filter("resolution_status = 'PENDING'").count() == 0)
check("Stock nunca negativo en ningún punto", c.filter("stock_after < 0").count() == 0)
check("Gold sin stock negativo", spark.table(T_INV).filter("current_stock < 0").count() == 0)
check("Colisión P099: POS y APP aceptados, WEB rechazado (por jerarquía)",
      p99 == {"POS": "ACCEPTED", "APP": "ACCEPTED", "WEB": "REJECTED_NO_STOCK"})
check("P099 termina en OUT_OF_STOCK", spark.table(T_ALERT).filter("product_id = 'P099' AND alert_level = 'OUT_OF_STOCK'").count() == 1)
check("1 colisión registrada (la de P099)", spark.table(T_COLL).count() == 1)
check("4 registros en cuarentena", spark.table(T_QUAR).count() == 4)
check("Idempotencia: Silver y Gold idénticos tras reenviar todo",
      all(antes[k] == despues[k] for k in antes))
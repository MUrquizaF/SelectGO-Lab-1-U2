# ───────────────────────── 8. EJECUCIÓN LOTE 1 (HISTORIAL) ─────────────────────────
run_pipeline()

print("Bronze:", spark.table(T_RAW).count(), "| Silver clean:", spark.table(T_CLEAN).count(),
      "| Cuarentena:", spark.table(T_QUAR).count(), "| Colisiones:", spark.table(T_COLL).count())
display(spark.table(T_INV).orderBy("store_id", "product_id"))
# ───────────────────────── 9. LOTE EN VIVO Y REEJECUCIÓN ─────────────────────────
append_to_bronze(LIVE)
run_pipeline()

print("Estado tras Lote 2:")
print("Bronze:", spark.table(T_RAW).count(), "| Silver clean:", spark.table(T_CLEAN).count(),
      "| Cuarentena:", spark.table(T_QUAR).count(), "| Colisiones:", spark.table(T_COLL).count())

print("\n--- Resultado de la colisión en P099 (WEB llegó primero pero gana POS) ---")
display(spark.table(T_CLEAN).filter("product_id = 'P099'").orderBy("event_time")
        .select("event_id", "channel", "channel_priority", "event_time", "quantity", "resolution_status", "stock_before", "stock_after"))

print("\n--- Registros enviados a Cuarentena ---")
display(spark.table(T_QUAR).select("event_id", "product_id", "store_id", "channel", "quantity", "reject_reason"))
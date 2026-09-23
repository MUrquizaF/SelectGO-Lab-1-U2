# ───────────────────────── 11. CONSULTAS DEL DASHBOARD ─────────────────────────
DASH = {
    "kpi_inventario_total":     f"SELECT SUM(current_stock) AS inventario_total FROM {T_INV}",
    "kpi_agotados":             f"SELECT COUNT(*) AS productos_agotados FROM {T_ALERT} WHERE alert_level = 'OUT_OF_STOCK'",
    "kpi_criticos":             f"SELECT COUNT(*) AS productos_criticos FROM {T_ALERT} WHERE alert_level = 'CRITICAL'",
    "kpi_en_riesgo":            f"SELECT COUNT(*) AS productos_en_riesgo FROM {T_ALERT} WHERE alert_level = 'WARNING'",
    "kpi_sobreventas_evitadas": f"SELECT COALESCE(SUM(units_rejected), 0) AS unidades_sobrevendidas_evitadas FROM {T_INV}",
    "ventas_por_canal":         f"SELECT channel AS canal, SUM(units_sold) AS unidades_vendidas, SUM(units_rejected) AS unidades_rechazadas FROM {T_CHAN} GROUP BY channel ORDER BY unidades_vendidas DESC",
    "inventario_por_tienda":    f"SELECT store_name AS tienda, SUM(current_stock) AS stock_total FROM {T_INV} GROUP BY store_name ORDER BY stock_total DESC",
    "alertas":                  f"""SELECT store_name AS tienda, product_name AS producto, current_stock AS stock, units_per_hour AS ventas_por_hora,
                                           hours_to_stockout AS horas_para_agotarse, alert_level AS nivel
                                    FROM {T_ALERT} WHERE alert_level <> 'OK'
                                    ORDER BY CASE alert_level WHEN 'OUT_OF_STOCK' THEN 0 WHEN 'CRITICAL' THEN 1 ELSE 2 END, hours_to_stockout""",
    "bitacora_colisiones":      f"SELECT window_start, store_id, product_id, collision_type, channels_involved, units_requested, stock_available, accepted_channels, rejected_channels FROM {T_COLL} ORDER BY window_start DESC",
}

for nombre, sql in DASH.items():
    print(f"\n-- {nombre}")
    display(spark.sql(sql))
# ───────────────────────── 7. CONSTRUCCIÓN CAPA GOLD ─────────────────────────
def inventory_sql(clean_ref, base_ref):
    return f"""
    WITH agg AS (
      SELECT store_id, product_id,
        SUM(CASE WHEN event_type = 'RESTOCK' AND resolution_status = 'ACCEPTED' THEN quantity ELSE 0 END) AS units_restocked,
        SUM(CASE WHEN event_type = 'SALE' AND resolution_status = 'ACCEPTED' THEN quantity ELSE 0 END) AS units_sold,
        SUM(CASE WHEN event_type = 'SALE' AND resolution_status = 'REJECTED_NO_STOCK' THEN quantity ELSE 0 END) AS units_rejected,
        MAX(event_time) AS last_event_time
      FROM {clean_ref}
      GROUP BY store_id, product_id
    )
    SELECT b.store_id, b.store_name, b.product_id, b.product_name, b.initial_stock, b.min_stock,
           CAST(COALESCE(a.units_restocked, 0) AS INT) AS units_restocked,
           CAST(COALESCE(a.units_sold, 0) AS INT)      AS units_sold,
           CAST(COALESCE(a.units_rejected, 0) AS INT)  AS units_rejected,
           CAST(b.initial_stock + COALESCE(a.units_restocked, 0) - COALESCE(a.units_sold, 0) AS INT) AS current_stock,
           a.last_event_time,
           current_timestamp() AS refreshed_at
    FROM {base_ref} b
    LEFT JOIN agg a ON a.store_id = b.store_id AND a.product_id = b.product_id
    """

def channel_sql(clean_ref):
    return f"""
    SELECT store_id, channel,
           CAST(SUM(CASE WHEN event_type = 'SALE' AND resolution_status = 'ACCEPTED' THEN quantity ELSE 0 END) AS INT) AS units_sold,
           CAST(SUM(CASE WHEN event_type = 'SALE' AND resolution_status = 'REJECTED_NO_STOCK' THEN quantity ELSE 0 END) AS INT) AS units_rejected,
           CAST(COUNT(CASE WHEN event_type = 'SALE' THEN 1 END) AS INT) AS sale_events
    FROM {clean_ref}
    GROUP BY store_id, channel
    """

def alerts_sql(clean_ref, inv_ref):
    return f"""
    WITH asof AS (SELECT MAX(event_time) AS t FROM {clean_ref}),
    velocity AS (
      SELECT c.store_id, c.product_id, SUM(c.quantity) / {VELOCITY_HOURS}.0 AS units_per_hour
      FROM {clean_ref} c CROSS JOIN asof
      WHERE c.event_type = 'SALE' AND c.resolution_status = 'ACCEPTED'
        AND c.event_time > asof.t - INTERVAL {VELOCITY_HOURS} HOURS
      GROUP BY c.store_id, c.product_id
    )
    SELECT i.store_id, i.store_name, i.product_id, i.product_name, i.current_stock, i.min_stock,
           ROUND(COALESCE(v.units_per_hour, 0), 3) AS units_per_hour,
           ROUND(i.current_stock / NULLIF(v.units_per_hour, 0), 1) AS hours_to_stockout,
           CASE
             WHEN i.current_stock <= 0 THEN 'OUT_OF_STOCK'
             WHEN i.current_stock <= i.min_stock
               OR i.current_stock / NULLIF(v.units_per_hour, 0) <= {CRITICAL_HOURS} THEN 'CRITICAL'
             WHEN i.current_stock / NULLIF(v.units_per_hour, 0) <= {WARNING_HOURS} THEN 'WARNING'
             ELSE 'OK'
           END AS alert_level,
           current_timestamp() AS generated_at
    FROM {inv_ref} i
    LEFT JOIN velocity v ON v.store_id = i.store_id AND v.product_id = i.product_id
    """

def build_gold():
    spark.sql(f"CREATE OR REPLACE TABLE {T_INV}   AS {inventory_sql(T_CLEAN, T_BASE)}")
    spark.sql(f"CREATE OR REPLACE TABLE {T_CHAN}  AS {channel_sql(T_CLEAN)}")
    spark.sql(f"CREATE OR REPLACE TABLE {T_ALERT} AS {alerts_sql(T_CLEAN, T_INV)}")

def run_pipeline():
    run_stream()
    resolve_inventory()
    build_gold()
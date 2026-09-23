# ───────────────────────── 6. RESOLUCIÓN DE COLISIONES ─────────────────────────
import pandas as pd
from pyspark.sql import functions as F

RES_SCHEMA = "event_id string, resolution_status string, stock_before int, stock_after int"

def simulate_stock(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["window_start", "type_order", "channel_priority", "event_time", "event_id"])
    stock = int(pdf["initial_stock"].iloc[0])
    out = []
    for r in pdf.itertuples(index=False):
        before, qty = stock, int(r.quantity)
        if r.event_type == "RESTOCK":
            stock += qty
            status = "ACCEPTED"
        elif qty <= stock:
            stock -= qty
            status = "ACCEPTED"
        else:
            status = "REJECTED_NO_STOCK"
        out.append((r.event_id, status, before, stock))
    return pd.DataFrame(out, columns=["event_id", "resolution_status", "stock_before", "stock_after"]).astype(
        {"stock_before": "int32", "stock_after": "int32"})

def compute_resolution(clean_df, base_df):
    ev_df = (clean_df.join(base_df.select("store_id", "product_id", "initial_stock"), ["store_id", "product_id"])
             .withColumn("type_order", F.when(F.col("event_type") == "RESTOCK", 0).otherwise(1))
             .select("store_id", "product_id", "event_id", "event_type", "quantity", "channel_priority",
                     "event_time", "window_start", "type_order", "initial_stock"))
    return ev_df.groupBy("store_id", "product_id").applyInPandas(simulate_stock, schema=RES_SCHEMA)

def collisions_sql(clean_ref):
    return f"""
    SELECT
      concat_ws('|', store_id, product_id, CAST(unix_seconds(window_start) AS STRING)) AS collision_id,
      store_id, product_id, window_start,
      CASE WHEN COUNT(DISTINCT channel) > 1 THEN 'STOCK_COLLISION' ELSE 'OVERSELL_ATTEMPT' END AS collision_type,
      array_join(transform(array_sort(collect_set(struct(channel_priority, channel))), x -> x.channel), ',') AS channels_involved,
      CAST(COUNT(*) AS INT) AS events_count,
      CAST(SUM(quantity) AS INT) AS units_requested,
      CAST(MAX(stock_before) AS INT) AS stock_available,
      CAST(SUM(CASE WHEN resolution_status = 'ACCEPTED' THEN quantity ELSE 0 END) AS INT) AS units_accepted,
      CAST(SUM(CASE WHEN resolution_status = 'REJECTED_NO_STOCK' THEN quantity ELSE 0 END) AS INT) AS units_rejected,
      array_join(transform(array_sort(collect_set(CASE WHEN resolution_status = 'ACCEPTED'
                 THEN struct(channel_priority, channel) END)), x -> x.channel), ',') AS accepted_channels,
      array_join(transform(array_sort(collect_set(CASE WHEN resolution_status = 'REJECTED_NO_STOCK'
                 THEN struct(channel_priority, channel) END)), x -> x.channel), ',') AS rejected_channels,
      current_timestamp() AS resolved_at
    FROM {clean_ref}
    WHERE event_type = 'SALE'
    GROUP BY store_id, product_id, window_start
    HAVING SUM(CASE WHEN resolution_status = 'REJECTED_NO_STOCK' THEN 1 ELSE 0 END) > 0
    """

def resolve_inventory():
    res = compute_resolution(spark.table(T_CLEAN), spark.table(T_BASE))
    res.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_STG)

    spark.sql(f"""
      MERGE INTO {T_CLEAN} AS t USING {T_STG} AS s
      ON t.event_id = s.event_id
      WHEN MATCHED AND (NOT (t.resolution_status <=> s.resolution_status)
                     OR NOT (t.stock_before <=> s.stock_before)
                     OR NOT (t.stock_after  <=> s.stock_after))
      THEN UPDATE SET resolution_status = s.resolution_status,
                      stock_before      = s.stock_before,
                      stock_after       = s.stock_after,
                      processed_at      = current_timestamp()
    """)

    spark.sql(f"""
      MERGE INTO {T_COLL} AS t USING ({collisions_sql(T_CLEAN)}) AS s
      ON t.collision_id = s.collision_id
      WHEN MATCHED AND (NOT (t.units_requested <=> s.units_requested)
                     OR NOT (t.units_rejected   <=> s.units_rejected)
                     OR NOT (t.stock_available  <=> s.stock_available)
                     OR NOT (t.accepted_channels <=> s.accepted_channels)
                     OR NOT (t.rejected_channels <=> s.rejected_channels))
      THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE THEN DELETE
    """)
# ───────────────────────── 5. STREAMING BRONZE -> SILVER ─────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def channel_priority_col(col_name="channel"):
    expr = F.lit(99)
    for ch, p in CHANNEL_PRIORITY.items():
        expr = F.when(F.col(col_name) == ch, F.lit(p)).otherwise(expr)
    return expr

def prepare_batch(batch_df, base_df):
    N = COLLISION_WINDOW_SECONDS
    fk = base_df.select("store_id", "product_id").distinct().withColumn("_fk_ok", F.lit(True))
    checked = (batch_df.join(F.broadcast(fk), ["store_id", "product_id"], "left")
        .withColumn("reject_reason",
            F.when(F.col("event_id").isNull(), "NULL_EVENT_ID")
             .when(F.col("channel").isNull() | ~F.col("channel").isin(list(CHANNEL_PRIORITY.keys())), "INVALID_CHANNEL")
             .when(F.col("event_type").isNull() | ~F.col("event_type").isin("SALE", "RESTOCK"), "INVALID_EVENT_TYPE")
             .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY")
             .when(F.col("event_time").isNull(), "NULL_EVENT_TIME")
             .when(F.col("_fk_ok").isNull(), "UNKNOWN_PRODUCT_STORE")))

    w = Window.partitionBy("event_id").orderBy(F.col("channel_priority").asc(), F.col("_ingested_at").asc())
    valid = (checked.filter(F.col("reject_reason").isNull())
        .withColumn("channel_priority", channel_priority_col())
        .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
        .withColumn("window_start", F.expr(f"timestamp_seconds(floor(unix_seconds(event_time) / {N}) * {N})"))
        .withColumn("resolution_status", F.lit("PENDING"))
        .withColumn("stock_before", F.lit(None).cast("int"))
        .withColumn("stock_after",  F.lit(None).cast("int"))
        .withColumn("processed_at", F.current_timestamp())
        .select(*CLEAN_COLS))

    bad = (checked.filter(F.col("reject_reason").isNotNull())
        .withColumn("row_hash", F.xxhash64("event_id", "product_id", "store_id", "channel", "event_type", "quantity", "event_time"))
        .dropDuplicates(["row_hash"])
        .withColumn("quarantined_at", F.current_timestamp())
        .select(*QUAR_COLS))
    return valid, bad

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    sess = batch_df.sparkSession
    valid, bad = prepare_batch(batch_df, sess.table(T_BASE))

    valid.createOrReplaceTempView("_valid_events")
    sess.sql(f"""
      MERGE INTO {T_CLEAN} AS t USING _valid_events AS s
      ON t.event_id = s.event_id
      WHEN NOT MATCHED THEN INSERT *
    """)

    bad.createOrReplaceTempView("_bad_events")
    sess.sql(f"""
      MERGE INTO {T_QUAR} AS t USING _bad_events AS s
      ON t.row_hash = s.row_hash
      WHEN NOT MATCHED THEN INSERT *
    """)

def run_stream():
    q = (spark.readStream.table(T_RAW)
         .writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", CKPT)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
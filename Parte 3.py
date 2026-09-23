# ───────────────────────── 3. TABLAS BRONZE Y SILVER (DDL) ─────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_RAW} (
  event_id     STRING    COMMENT 'ID único del evento',
  product_id   STRING,
  store_id     STRING,
  channel      STRING    COMMENT 'POS | APP | WEB',
  event_type   STRING    COMMENT 'SALE | RESTOCK',
  quantity     INT,
  event_time   TIMESTAMP COMMENT 'Ocurrencia en origen',
  _ingested_at TIMESTAMP COMMENT 'Llegada a Bronze',
  _source      STRING    COMMENT 'Sistema de origen'
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_BASE} (
  store_id      STRING,
  store_name    STRING,
  product_id    STRING,
  product_name  STRING,
  initial_stock INT COMMENT 'Stock de apertura',
  min_stock     INT COMMENT 'Stock mínimo',
  updated_at    TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_CLEAN} (
  event_id          STRING,
  product_id        STRING,
  store_id          STRING,
  channel           STRING,
  channel_priority  INT       COMMENT '1 = POS, 2 = APP, 3 = WEB',
  event_type        STRING,
  quantity          INT,
  event_time        TIMESTAMP,
  window_start      TIMESTAMP COMMENT 'Ventana de colisión 10s',
  _ingested_at      TIMESTAMP,
  _source           STRING,
  resolution_status STRING    COMMENT 'PENDING | ACCEPTED | REJECTED_NO_STOCK',
  stock_before      INT,
  stock_after       INT,
  processed_at      TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_QUAR} (
  row_hash       BIGINT,
  event_id       STRING,
  product_id     STRING,
  store_id       STRING,
  channel        STRING,
  event_type     STRING,
  quantity       INT,
  event_time     TIMESTAMP,
  _ingested_at   TIMESTAMP,
  _source        STRING,
  reject_reason  STRING,
  quarantined_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_COLL} (
  collision_id      STRING,
  store_id          STRING,
  product_id        STRING,
  window_start      TIMESTAMP,
  collision_type    STRING,
  channels_involved STRING,
  events_count      INT,
  units_requested   INT,
  stock_available   INT,
  units_accepted    INT,
  units_rejected    INT,
  accepted_channels STRING,
  rejected_channels STRING,
  resolved_at       TIMESTAMP
) USING DELTA
""")

display(spark.sql(f"SHOW TABLES IN {CATALOG}.silver"))

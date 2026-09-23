# ───────────────────────── INTEGRANTES DEL GRUPO ─────────────────────────
# Integrante 1: Fabricio Abrego (20240064@ujmd.edu.sv)
# Integrante 2: Gerardo Chávez (202401062@ujmd.edu.sv)
# Integrante 3: Ricardo Ramírez (202401177@ujmd.edu.sv)
# Integrante 4: Mario Urquiza (202401197@ujmd.edu.sv)
# ─────────────────────────────────────────────────────────────────────────

# ───────────────────────── 1. CONFIGURACIÓN ─────────────────────────
from datetime import datetime, timedelta, timezone
import random
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "selectgo"     # Nombre de tu catálogo
RESET   = False           # True limpia esquemas previos; déjalo en True para este primer reinicio

CHANNEL_PRIORITY = {"POS": 1, "APP": 2, "WEB": 3}
COLLISION_WINDOW_SECONDS = 10   # Ventana de colisión (10 s)
VELOCITY_HOURS = 24             # Ventana para medir velocidad de venta (24 h)
CRITICAL_HOURS = 24             # Agotamiento en <= 24 h -> CRITICAL
WARNING_HOURS  = 72             # Agotamiento en <= 72 h -> WARNING

T_RAW   = f"{CATALOG}.bronze.inventory_events_raw"
T_BASE  = f"{CATALOG}.silver.inventory_baseline"
T_CLEAN = f"{CATALOG}.silver.inventory_events_clean"
T_QUAR  = f"{CATALOG}.silver.inventory_events_quarantine"
T_COLL  = f"{CATALOG}.silver.inventory_collisions"
T_STG   = f"{CATALOG}.silver._stg_event_resolution"
T_INV   = f"{CATALOG}.gold.inventory_by_store"
T_CHAN  = f"{CATALOG}.gold.sales_by_channel"
T_ALERT = f"{CATALOG}.gold.stock_alerts"
CKPT    = f"/Volumes/{CATALOG}/bronze/checkpoints/silver_events"

CLEAN_COLS = ["event_id", "product_id", "store_id", "channel", "channel_priority", "event_type", "quantity",
              "event_time", "window_start", "_ingested_at", "_source",
              "resolution_status", "stock_before", "stock_after", "processed_at"]
QUAR_COLS  = ["row_hash", "event_id", "product_id", "store_id", "channel", "event_type", "quantity",
              "event_time", "_ingested_at", "_source", "reject_reason", "quarantined_at"]
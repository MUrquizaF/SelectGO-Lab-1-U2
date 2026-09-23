# ───────────────────────── 4. SIMULACIÓN DE DATOS ─────────────────────────
RAW_SCHEMA = ("event_id string, product_id string, store_id string, channel string, event_type string, "
              "quantity int, event_time timestamp, _ingested_at timestamp, _source string")
SOURCE_OF = {"POS": "pos-terminals", "APP": "mobile-api", "WEB": "web-checkout"}

STORES = {"S001": ("Tienda San Salvador", 1.0), "S002": ("Tienda Santa Tecla", 0.75), "S003": ("Tienda San Miguel", 0.5)}
PRODUCTS = {"P001": ("Camiseta básica", 1.0), "P002": ("Jeans slim", 0.75), "P003": ("Tenis runner", 0.5),
            "P004": ("Mochila urbana", 0.5), "P005": ("Gorra logo", 0.5)}
DAYS_LEFT = {("S001", "P001"): 6.0, ("S001", "P002"): 2.0, ("S001", "P003"): 0.5, ("S001", "P004"): 4.0, ("S001", "P005"): 8.0,
             ("S002", "P001"): 5.0, ("S002", "P002"): 0.4, ("S002", "P003"): 1.5, ("S002", "P004"): 6.0, ("S002", "P005"): 3.5,
             ("S003", "P001"): 4.0, ("S003", "P002"): 7.0, ("S003", "P003"): 2.5, ("S003", "P004"): 0.3, ("S003", "P005"): 5.0}

def ev(event_id, product_id, store_id, channel, event_type, qty, event_time, ingested_at=None):
    return (event_id, product_id, store_id, channel, event_type, qty, event_time,
            ingested_at or event_time + timedelta(seconds=2), SOURCE_OF.get(channel, "unknown"))

def build_history(base_time, seed=2026):
    rnd = random.Random(seed)
    events, baseline = [], []
    for (sid, pid), days_left in DAYS_LEFT.items():
        rate = STORES[sid][1] * PRODUCTS[pid][1]
        sales = []
        for h in range(48, 0, -1):
            k = int(rate) + (1 if rnd.random() < rate - int(rate) else 0)
            for _ in range(k):
                t  = base_time - timedelta(hours=h) + timedelta(seconds=rnd.randint(0, 3599))
                ch = rnd.choices(["POS", "APP", "WEB"], weights=[55, 25, 20])[0]
                q  = rnd.choices([1, 2], weights=[85, 15])[0]
                sales.append((t, ch, q))
        sales.sort()
        sold_total = sum(q for _, _, q in sales)
        sold_24h   = max(1, sum(q for t, _, q in sales if t > base_time - timedelta(hours=24)))
        stock_end  = max(6, round(days_left * sold_24h))
        baseline.append((sid, STORES[sid][0], pid, PRODUCTS[pid][0], sold_total + stock_end, 5))
        for i, (t, ch, q) in enumerate(sales, 1):
            events.append(ev(f"H-{sid}-{pid}-{i:04d}", pid, sid, ch, "SALE", q, t))
    baseline.append(("S001", STORES["S001"][0], "P099", "Edición limitada", 2, 1))
    return events, baseline

def build_live(base_time):
    b = lambda s: base_time + timedelta(seconds=s)
    return [
        ev("L-0001", "P001", "S001", "POS", "SALE", 1, b(5), ingested_at=b(6)),
        ev("L-0001", "P001", "S001", "POS", "SALE", 1, b(5), ingested_at=b(9)),
        ev("L-0002", "P099", "S001", "WEB", "SALE", 1, b(1)),
        ev("L-0003", "P099", "S001", "APP", "SALE", 1, b(2)),
        ev("L-0004", "P099", "S001", "POS", "SALE", 1, b(3)),
        ev("L-0005", "P002", "S001", "POS", "SALE", 1, b(1)),
        ev("L-0006", "P002", "S001", "WEB", "SALE", 1, b(2)),
        ev("L-0007", "P003", "S002", "POS", "RESTOCK", 30, b(8)),
        ev("L-0008", "P001", "S001", "POS",   "SALE", -3, b(10)),
        ev("L-0009", "P001", "S002", "KIOSK", "SALE",  1, b(10)),
        ev("L-0010", "P404", "S001", "WEB",   "SALE",  1, b(10)),
        ev(None,     "P002", "S001", "APP",   "SALE",  1, b(10)),
    ]

def append_to_bronze(rows):
    spark.createDataFrame(rows, schema=RAW_SCHEMA).write.mode("append").saveAsTable(T_RAW)

NOW  = datetime.now(timezone.utc).replace(microsecond=0)
BASE = NOW - timedelta(minutes=5)
BASE = BASE.replace(second=(BASE.second // 10) * 10)

HISTORY, BASELINE = build_history(BASE)
LIVE = build_live(BASE)

# Cargar inventario base
spark.createDataFrame(BASELINE, "store_id string, store_name string, product_id string, product_name string, initial_stock int, min_stock int") \
     .withColumn("updated_at", F.current_timestamp()).createOrReplaceTempView("_baseline_seed")
spark.sql(f"""
  MERGE INTO {T_BASE} AS t USING _baseline_seed AS s
  ON t.store_id = s.store_id AND t.product_id = s.product_id
  WHEN NOT MATCHED THEN INSERT *
""")

append_to_bronze(HISTORY)
print(f"Historial cargado. Filas en Bronze: {spark.table(T_RAW).count()}")

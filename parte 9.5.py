# ───────────────────────── 9.5 INGESTA EXTERNA VÍA API (ZENODO) ─────────────────────────
import requests
import pandas as pd
from pyspark.sql import functions as F

url = "https://zenodo.org/records/18342253/files/dataRSM.csv?download=1"
resp = requests.get(url, timeout=60)
resp.raise_for_status()

with open("/tmp/dataRSM.csv", "wb") as f:
    f.write(resp.content)

pdf = pd.read_csv("/tmp/dataRSM.csv", sep=";", decimal=",")
ext_df = (spark.createDataFrame(pdf)
          .withColumn("_source", F.lit("zenodo_api"))
          .withColumn("_doi", F.lit("10.5281/zenodo.18342253"))
          .withColumn("_ingested_at", F.current_timestamp()))

ext_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.market_reference_raw")
display(ext_df.limit(10))
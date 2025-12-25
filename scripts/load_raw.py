import glob
import os
import pandas as pd
from sqlalchemy import create_engine

DB = os.getenv("POSTGRES_DB", "de_db")
USER = os.getenv("POSTGRES_USER", "de_user")
PWD = os.getenv("POSTGRES_PASSWORD", "de_password")
HOST = os.getenv("POSTGRES_HOST", "postgres")
PORT = os.getenv("POSTGRES_PORT", "5432")

engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

files = sorted(glob.glob("/data/*.parquet"))
if not files:
    raise SystemExit("Нет файлов *.parquet в папке data/")

print("files:", len(files))

for f in files:
    df = pd.read_parquet(f)
    df.columns = [c.strip() for c in df.columns]

    if "item_replaced_id" in df.columns:
        df["item_replaced_id"] = pd.to_numeric(df["item_replaced_id"], errors="coerce").astype("Int64")

    print("loading", f, "rows", len(df))
    df.to_sql(
        "raw_data",
        engine,
        schema="dwh",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    break

print("done")

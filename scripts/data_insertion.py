import os
import sys
import logging
from typing import List, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ============================================================
# 6. DATA INSERTION (Python + psycopg2)
# - env vars (no hardcoded SQL credentials)
# - cleaning: NULLs, type conversion, duplicate removal
# - validation before insertion
# - parameterized batch inserts (execute_values)
# - FK-safe insertion order (parent -> child)
# - transaction management (COMMIT/ROLLBACK)
# - error handling with rollback
# - logging/progress tracking
# ============================================================

# -----------------------------
# (A) Environment variables
# -----------------------------
# NOTE: For coursework/local use. Avoid committing real passwords to Git.
os.environ["PGHOST"] = "localhost"
os.environ["PGPORT"] = "5432"
os.environ["PGDATABASE"] = "car_market"
os.environ["PGUSER"] = "sergey"
os.environ["PGPASSWORD"] = "1111"

# -----------------------------
# (B) Paths / Settings
# -----------------------------
BASE_DIR = "/home/sergey/Desktop/Project"
CSV_FILES = {
    "core": f"{BASE_DIR}/core.csv",
    "pricing": f"{BASE_DIR}/pricing.csv",
    "vehicle": f"{BASE_DIR}/vehicle.csv",
    "specs": f"{BASE_DIR}/specs.csv",
    "appearance": f"{BASE_DIR}/appearance.csv",
    "status": f"{BASE_DIR}/status.csv",
}
SCHEMA = "car_market"
BATCH_SIZE = 5000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


# -----------------------------
# DB connection (from env vars)
# -----------------------------
def get_conn():
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pwd = os.getenv("PGPASSWORD")

    missing = [k for k in ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"] if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            f"Missing env vars: {missing}. Please set them before running."
        )

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)


# -----------------------------
# CSV loading / validation
# -----------------------------
def read_csv(name: str) -> pd.DataFrame:
    path = CSV_FILES[name]
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    df = pd.read_csv(path)
    logging.info(f"Loaded {name}: {len(df):,} rows from {path}")
    return df


def validate_columns(df: pd.DataFrame, required_cols: List[str], table_name: str):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{table_name}: missing columns {missing}")


# -----------------------------
# Cleaning helpers
# -----------------------------
def clean_core(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["listing_id"]).copy()
    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
    df["url"] = df["url"].astype("string").str.strip()
    df = df.dropna(subset=["listing_id", "url"])
    return df


def clean_pricing(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["listing_id"]).copy()
    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df.loc[df["price"].notna() & (df["price"] < 0), "price"] = pd.NA

    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        current_year = pd.Timestamp.today().year
        df.loc[df["year"].notna() & ((df["year"] < 1950) | (df["year"] > current_year)), "year"] = pd.NA

    if "mileage" in df.columns:
        df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce").astype("Int64")
        df.loc[df["mileage"].notna() & (df["mileage"] < 0), "mileage"] = pd.NA

    df = df.dropna(subset=["listing_id"])
    return df


def clean_vehicle(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["listing_id"]).copy()
    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
    for col in ["make", "model"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    df = df.dropna(subset=["listing_id"])
    return df


def clean_specs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["listing_id"]).copy()
    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")

    if "engine_size" in df.columns:
        df["engine_size"] = pd.to_numeric(df["engine_size"], errors="coerce")
        df.loc[df["engine_size"].notna() & (df["engine_size"] <= 0), "engine_size"] = pd.NA

    if "wheel_size" in df.columns:
        df["wheel_size"] = pd.to_numeric(df["wheel_size"], errors="coerce")
        df.loc[df["wheel_size"].notna() & (df["wheel_size"] <= 0), "wheel_size"] = pd.NA

    for col in ["engine_type", "transmission", "drive_type", "steering_wheel", "comfort"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    df = df.dropna(subset=["listing_id"])
    return df


def clean_appearance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["listing_id"]).copy()
    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")

    for col in ["body_type", "color", "interior_material"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    if "sunroof" in df.columns:
        s = df["sunroof"].astype("string").str.lower().str.strip()
        df["sunroof"] = s.map(
            {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}
        )

    df = df.dropna(subset=["listing_id"])
    return df


def clean_status(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["listing_id"]).copy()
    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")

    if "cleared_customs" in df.columns:
        s = df["cleared_customs"].astype("string").str.lower().str.strip()
        df["cleared_customs"] = s.map(
            {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}
        )

    if "condition" in df.columns:
        df["condition"] = df["condition"].astype("string").str.strip()

    df = df.dropna(subset=["listing_id"])
    return df


# -----------------------------
# numpy/pandas scalar -> python scalar
# (fix for: can't adapt type 'numpy.int64')
# -----------------------------
def to_py(x):
    if pd.isna(x):
        return None
    if hasattr(x, "item"):
        try:
            return x.item()
        except Exception:
            pass
    return x


def rows_as_tuples(df: pd.DataFrame, cols: List[str]) -> List[Tuple]:
    out: List[Tuple] = []
    for row in df[cols].itertuples(index=False, name=None):
        out.append(tuple(to_py(x) for x in row))
    return out


# -----------------------------
# Batch insert (parameterized)
# -----------------------------
def insert_batches(cur, table: str, cols: List[str], rows: List[Tuple]):
    if not rows:
        logging.warning(f"{table}: 0 rows to insert")
        return

    col_sql = ", ".join(cols)
    sql = f"INSERT INTO {SCHEMA}.{table} ({col_sql}) VALUES %s ON CONFLICT DO NOTHING"

    total = len(rows)
    for start in range(0, total, BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        execute_values(cur, sql, batch, page_size=len(batch))
        logging.info(
            f"{table}: inserted batch {start:,}..{min(start+BATCH_SIZE, total):,} / {total:,}"
        )


def main():
    # -----------------------------
    # Load CSVs
    # -----------------------------
    core = read_csv("core")
    pricing = read_csv("pricing")
    vehicle = read_csv("vehicle")
    specs = read_csv("specs")
    appearance = read_csv("appearance")
    status = read_csv("status")

    # -----------------------------
    # Validate expected columns
    # -----------------------------
    validate_columns(core, ["listing_id", "url"], "core")
    validate_columns(pricing, ["listing_id"], "pricing")
    validate_columns(vehicle, ["listing_id"], "vehicle")
    validate_columns(specs, ["listing_id"], "specs")
    validate_columns(appearance, ["listing_id"], "appearance")
    validate_columns(status, ["listing_id"], "status")

    # -----------------------------
    # Clean data
    # -----------------------------
    core = clean_core(core)
    pricing = clean_pricing(pricing)
    vehicle = clean_vehicle(vehicle)
    specs = clean_specs(specs)
    appearance = clean_appearance(appearance)
    status = clean_status(status)

    logging.info(
        "After cleaning: "
        f"core={len(core):,}, pricing={len(pricing):,}, vehicle={len(vehicle):,}, "
        f"specs={len(specs):,}, appearance={len(appearance):,}, status={len(status):,}"
    )

    # -----------------------------
    # Insert with transaction + FK order
    # -----------------------------
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False  # transaction start

        with conn.cursor() as cur:
            # Parent first (FK safe)
            insert_batches(
                cur,
                "core",
                ["listing_id", "url"],
                rows_as_tuples(core, ["listing_id", "url"]),
            )

            # Children next
            insert_batches(
                cur,
                "pricing",
                ["listing_id", "price", "year", "mileage"],
                rows_as_tuples(pricing, ["listing_id", "price", "year", "mileage"]),
            )

            insert_batches(
                cur,
                "vehicle",
                ["listing_id", "make", "model"],
                rows_as_tuples(vehicle, ["listing_id", "make", "model"]),
            )

            insert_batches(
                cur,
                "specs",
                ["listing_id", "engine_size", "engine_type", "transmission", "drive_type",
                 "steering_wheel", "wheel_size", "comfort"],
                rows_as_tuples(specs, ["listing_id", "engine_size", "engine_type", "transmission", "drive_type",
                                       "steering_wheel", "wheel_size", "comfort"]),
            )

            insert_batches(
                cur,
                "appearance",
                ["listing_id", "body_type", "color", "interior_material", "sunroof"],
                rows_as_tuples(appearance, ["listing_id", "body_type", "color", "interior_material", "sunroof"]),
            )

            insert_batches(
                cur,
                "status",
                ["listing_id", "cleared_customs", "condition"],
                rows_as_tuples(status, ["listing_id", "cleared_customs", "condition"]),
            )

        conn.commit()
        logging.info("✅ All inserts committed successfully.")

    except Exception as e:
        logging.exception(f"❌ Insert failed: {e}")
        if conn is not None:
            try:
                conn.rollback()
                logging.info("Rolled back transaction.")
            except Exception:
                pass
        raise

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()

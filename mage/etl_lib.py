# etl_lib.py
import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# Config helpers
# =========================
def get_db_url_from_env() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "admin")
    pw   = os.getenv("POSTGRES_PASSWORD", "passw0rd")
    db   = os.getenv("POSTGRES_DB", "pgdb")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"


PARQUET_ENGINE = "fastparquet"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# =========================
# Extract
# =========================
def extract(engine: str = PARQUET_ENGINE) -> pd.DataFrame:
    # Get the list of parquet files
    files = sorted(glob.glob("/home/src/data_sample/*.parquet"))
    # Check if any files were found
    if not files:
        raise FileNotFoundError(f"No parquet files found")
    # Read the parquet files into DataFrames
    dfs = [pd.read_parquet(p, engine=engine) for p in files]

    # Concatenate all DataFrames into a single DataFrame
    return pd.concat(dfs, ignore_index=True)

# =========================
# Transform
# =========================
def transform(df: pd.DataFrame) -> dict:

    df_department = (
        df[["department_name"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"department_name": "name"})
        .reset_index(drop=True)
    )

    df_sensor = (
        df[["sensor_serial", "department_name"]]
        .dropna(subset=["sensor_serial", "department_name"])
        .drop_duplicates()
        .rename(columns={"sensor_serial": "serial"})
        .reset_index(drop=True)
    )
    df_product = (
        df[["product_name"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"product_name": "name"})
        .reset_index(drop=True)
    )

    df_sensor_logs = pd.DataFrame({
        "serial": df["sensor_serial"],
        "product_name": df["product_name"],
        "create_at": df["create_at"],
        "product_expire": df["product_expire"],
    })

    return {
        "departments": df_department,
        "sensors": df_sensor,
        "products": df_product,
        "logs": df_sensor_logs,
    }

# =========================
# Load (Postgres)
# =========================
def run_schema_sql(schema_path: str = "/home/src/sql/schema.sql", db_url: str | None = None) -> None:
    """
    read schema.sql file and run all DDL statements
    """
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    engine = create_engine(db_url or get_db_url_from_env(), future=True)
    with engine.begin() as conn:
        conn.execute(text(ddl_sql))


def execute_chunks(conn, sql: str, rows: list, chunk: int = BATCH_SIZE):
    if not rows:
        return
    for i in range(0, len(rows), chunk):
        conn.execute(text(sql), rows[i:i+chunk])

def load_into_postgres(parts: dict, db_url: str | None = None, batch_size: int = BATCH_SIZE):
    """
    parts:
      - departments: columns -> name
      - sensors:     columns -> serial, department_name
      - products:    columns -> name
      - logs:      columns -> serial, product_name, create_at, product_expire
    """
    db_url = db_url or get_db_url_from_env()
    engine = create_engine(db_url, future=True)

    df_department  = parts["departments"]
    df_sensor = parts["sensors"]
    df_product = parts["products"]
    df_logs  = parts["logs"].copy()

    # สร้างตารางจากไฟล์ schema ก่อน
    run_schema_sql(schema_path="/home/src/sql/schema.sql", db_url=db_url)

    with engine.begin() as conn:
        # 1) departments
        dep_rows = [{"name": n} for n in df_department["name"].tolist()]
        execute_chunks(conn,
            "INSERT INTO departments(name) VALUES (:name) "
            "ON CONFLICT (name) DO NOTHING;",
            dep_rows, batch_size
        )

        # dep map {name:id}
        dep_map = {name: _id for _id, name in conn.execute(text(
            "SELECT id, name FROM departments;"
        )).all()}

        # 2) sensors (map department_name -> department_id)
        sens_rows = []
        for r in df_sensor.to_dict("records"):
            dep_id = dep_map.get(r["department_name"])
            if dep_id is None:
                continue
            sens_rows.append({"serial": r["serial"], "department_id": dep_id})

        execute_chunks(conn,
            "INSERT INTO sensors(serial, department_id) VALUES (:serial, :department_id) "
            "ON CONFLICT (serial) DO UPDATE SET department_id = EXCLUDED.department_id;",
            sens_rows, batch_size
        )

        # sensor map {serial:id}
        sens_map = dict(conn.execute(text(
            "SELECT serial, id FROM sensors;"
        )).all())

        # 3) products
        prod_rows = [{"name": r["name"]} for r in df_product.to_dict("records")]
        execute_chunks(conn,
            "INSERT INTO products(name) VALUES (:name) "
            "ON CONFLICT (name) DO NOTHING;",
            prod_rows, batch_size
        )

        # product map {name:id}
        prod_map = dict(conn.execute(text(
            "SELECT name, id FROM products;"
        )).all())

        log_rows = []
        for r in df_logs.to_dict("records"):
            sensor_id = sens_map.get(r["serial"])
            if not sensor_id:
                continue
            pname = r.get("product_name")
            product_id = prod_map.get(pname) if pd.notna(pname) else None

            log_rows.append({
                "sensor_id": int(sensor_id),
                "product_id": None if product_id is None else int(product_id),
                "create_at": r["create_at"],
                "product_expire": r["product_expire"],
            })

        if log_rows:
            execute_chunks(conn,
                "INSERT INTO sensor_logs(sensor_id, product_id, create_at, product_expire) "
                "VALUES (:sensor_id, :product_id, :create_at, :product_expire) "
                "ON CONFLICT (sensor_id, product_id, create_at) DO NOTHING;",
                log_rows, batch_size
            )

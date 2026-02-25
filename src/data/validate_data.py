import pandas as pd
import yaml

# ---------- Load schema ----------
def load_schema(schema_path: str) -> dict:
    with open(schema_path) as f:
        raw = yaml.safe_load(f)

    return {
        col: meta["dtype"]
        for col, meta in raw.items()
    }
# ---------- Change term ---------
def normalize_term(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
              .str.extract(r"(\d+)")
              .astype("float32")
    )

# ---------- Enforce structure ----------
def enforce_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    df = df.copy()

    missing = set(schema) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[list(schema.keys())]

    for col, dtype in schema.items():

        if col == "term":
            df[col] = normalize_term(df[col])
            continue

        if "datetime" in dtype:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[col] = df[col].astype(dtype)

    return df
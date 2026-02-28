import pandas as pd
from pathlib import Path


def add_year_columns(df: pd.DataFrame, date_col: str = "issue_d") -> pd.DataFrame:
    """Add year and month columns from a datetime column."""
    df = df.copy()

    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        raise TypeError(f"{date_col} must already be datetime")

    df["year"] = df[date_col].dt.year
    df["month"] = df[date_col].dt.month

    return df


def split_by_year(df: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """Split dataframe into a dictionary of yearly dataframes."""
    return {
        int(year): data.reset_index(drop=True)
        for year, data in df.groupby("year")
    }


def save_yearly_parquets(year_splits: dict[int, pd.DataFrame], output_dir: str):
    """Save each yearly dataframe to parquet."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for year, data in year_splits.items():
        data.to_parquet(output_dir / f"loans_{year}.parquet", index=False)
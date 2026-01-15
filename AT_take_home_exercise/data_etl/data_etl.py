import os
import sys
import pandas as pd

# ---------------------------------
# Configuration
# ---------------------------------
INPUT_PATH = os.path.expanduser(
    "~/Downloads/AT_take_home_exercise/tick_data/date=2024-03-01/sample_fx_data_A.csv.gz"
)

OUTPUT_PATH = os.path.expanduser(
    "~/Downloads/AT_take_home_exercise/data_etl/ohlc_data/date=2024-03-01/data.csv.gz"
)

EXPECTED_DATE = pd.Timestamp("2023-12-01")

# ---------------------------------
# Extract
# ---------------------------------
def extract(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    return pd.read_csv(
        path,
        compression="gzip",
        parse_dates=["datetime"]
    )

# ---------------------------------
# Transform
# ---------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"datetime", "currency_pair", "bid", "ask"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

    # Filter expected date and FORCE COPY
    df = df.loc[df["datetime"].dt.date == EXPECTED_DATE.date()].copy()

    if df.empty:
        raise RuntimeError("No data found for expected trading date.")

    # Mid price
    df.loc[:, "mid"] = (df["bid"] + df["ask"]) / 2

    # Normalize currency pair (XXXYYY → XXX/YYY)
    df.loc[:, "currency_pair"] = (
        df["currency_pair"].str[:3] + "/" + df["currency_pair"].str[3:]
    )

    # Floor timestamps to 1-minute bars
    df.loc[:, "minute"] = df["datetime"].dt.floor("1min")

    # Sort for OHLC accuracy
    df = df.sort_values(["currency_pair", "minute", "datetime"])

    # 1-minute OHLC aggregation
    ohlc = (
        df.groupby(["currency_pair", "minute"], as_index=False)
        .agg(
            open=("mid", "first"),
            high=("mid", "max"),
            low=("mid", "min"),
            close=("mid", "last"),
        )
        .rename(columns={"minute": "timestamp"})
    )

    return ohlc

# ---------------------------------
# Load
# ---------------------------------
def load(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, compression="gzip")

# ---------------------------------
# Main ETL
# ---------------------------------
def main():
    try:
        raw_df = extract(INPUT_PATH)
        ohlc_df = transform(raw_df)
        load(ohlc_df, OUTPUT_PATH)
        print("ETL completed successfully (1-minute OHLC).")

    except Exception as e:
        print(f"ETL failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

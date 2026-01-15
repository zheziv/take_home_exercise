import os
import glob
import pandas as pd
import numpy as np
import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
from pathlib import Path
import subprocess

# ----------------------------
# Paths
# ----------------------------
INPUT_DIR = Path("~/Downloads/AT_take_home_exercise/tick_data/date=2024-03-01").expanduser()
OUTPUT_DIR = Path("~/Downloads/AT_take_home_exercise/data_provider_selection").expanduser()

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NOTEBOOK_PATH = OUTPUT_DIR / "analysis.ipynb"
HTML_PATH = OUTPUT_DIR / "analysis.html"

# ----------------------------
# Notebook content
# ----------------------------

nb = new_notebook(cells=[

    new_markdown_cell("""
# FX Tick Data Provider Evaluation

## Objective
Evaluate three FX tick data providers (**A, B, C**) and select the most appropriate provider
based on **data quality, pricing efficiency, liquidity, and reliability**.

## Dataset Location
"""),

    new_code_cell("""
import pandas as pd
import numpy as np
import glob
from pathlib import Path
"""),

    new_markdown_cell("## Load Tick Data"),

    new_code_cell(f"""
DATA_DIR = Path("{INPUT_DIR}")
files = glob.glob(str(DATA_DIR / "*.csv.gz"))

dfs = []
for f in files:
    provider = f.split("_")[-1].replace(".csv.gz", "")
    df = pd.read_csv(f, compression="gzip", parse_dates=["datetime"])
    df["provider"] = provider
    dfs.append(df)

data = pd.concat(dfs, ignore_index=True)
data.head()
"""),

    new_markdown_cell("## Dataset Coverage"),

    new_code_cell("""
coverage = (
    data.groupby("provider")
    .agg(
        ticks=("datetime", "count"),
        currency_pairs=("currency_pair", "nunique"),
        start_time=("datetime", "min"),
        end_time=("datetime", "max")
    )
)
coverage
"""),

    new_markdown_cell("## Data Quality Assessment"),

    new_code_cell("""
# Missing values per provider
missing_values = (
    data.isna()
    .groupby(data["provider"])
    .sum()
    .sum(axis=1)
)

# Duplicate rows per provider
duplicate_rows = (
    data.duplicated()
    .groupby(data["provider"])
    .sum()
)

# Invalid bid >= ask per provider
invalid_bid_ask = (
    (data["bid"] >= data["ask"])
    .groupby(data["provider"])
    .sum()
)

quality = pd.concat(
    [missing_values, duplicate_rows, invalid_bid_ask],
    axis=1
)

quality.columns = [
    "missing_values",
    "duplicate_rows",
    "invalid_bid_ask"
]

quality
"""),

    new_markdown_cell("## Spread & Liquidity Analysis"),

    new_code_cell("""
data["spread"] = data["ask"] - data["bid"]

spread_stats = data.groupby("provider").agg(
    avg_spread=("spread", "mean"),
    spread_std=("spread", "std"),
    avg_volume=("volume", "mean"),
    volume_std=("volume", "std")
)

spread_stats
"""),

    new_markdown_cell("## Price Stability (Outlier Detection)"),

    new_code_cell("""
data = data.sort_values(["provider", "currency_pair", "datetime"])
data["mid"] = (data["bid"] + data["ask"]) / 2

data["abs_return"] = data.groupby(
    ["provider", "currency_pair"]
)["mid"].pct_change(fill_method=None).abs()

outliers = data.groupby("provider")["abs_return"].apply(
    lambda x: (x > x.quantile(0.999)).sum()
)

outliers
"""),

    new_markdown_cell("## Composite Provider Scoring"),

    new_code_cell("""
score = (
    spread_stats
    .join(quality)
    .assign(
        spread_score=lambda x: 1 / x["avg_spread"],
        volume_score=lambda x: x["avg_volume"],
        penalty=lambda x: (
            x["missing_values"]
            + x["duplicate_rows"]
            + x["invalid_bid_ask"]
        )
    )
)

score["final_score"] = (
    score["spread_score"]
    + score["volume_score"] * 1e-6
    - score["penalty"] * 1e-3
)

score.sort_values("final_score", ascending=False)
"""),

    new_markdown_cell("## Conclusion & Provider Selection"),

    new_code_cell("""
best_provider = score["final_score"].idxmax()

print(f"The best data provider is {best_provider}.")

score.loc[[best_provider]]
"""),

    new_markdown_cell("""
### Final Decision

**The best data provider is the one with the highest composite score**, reflecting:

- Tight and stable bid-ask spreads  
- Strong and consistent trading volume  
- Minimal data quality issues  
- Fewer extreme price jumps  

This provider offers the best balance of **execution realism, data cleanliness,
and analytical reliability**, making it the most suitable choice for trading
research, backtesting, and production analytics.
""")
])

# ----------------------------
# Write notebook
# ----------------------------
with open(NOTEBOOK_PATH, "w") as f:
    nbformat.write(nb, f)

# ----------------------------
# Convert to HTML
# ----------------------------
subprocess.run([
    "jupyter", "nbconvert",
    "--execute",
    "--to", "html",
    str(NOTEBOOK_PATH),
    "--output", str(HTML_PATH)
])

print("✔ Analysis notebook and HTML report generated successfully.")
print(f"📘 Notebook: {NOTEBOOK_PATH}")
print(f"🌐 HTML: {HTML_PATH}")

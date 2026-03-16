import os
import sys

cwd = os.getcwd()
root = cwd

while root != "/" and not os.path.isdir(os.path.join(root, "src")):
    root = os.path.dirname(root)

if root == "/" or not os.path.isdir(os.path.join(root, "src")):
    raise RuntimeError(f"Could not find project root containing src/. Started from: {cwd}")

if root not in sys.path:
    sys.path.insert(0, root)

os.environ.setdefault("LOAD_MODE", "backfill")
os.environ.setdefault("START_DATE", "2020-01-01")
os.environ.setdefault("FX_BASE_CURRENCY", "GBP")
os.environ.setdefault("FX_QUOTE_CURRENCIES", "USD,EUR,JPY,CHF")

from src.bronze.ingest_fx_data import main

if __name__ == "__main__":
    main()

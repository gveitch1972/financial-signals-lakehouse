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
os.environ.setdefault("START_DATE", "2026-06-09")
os.environ.setdefault("END_DATE", "2026-06-09")

from src.bronze.ingest_market_data import main

if __name__ == "__main__":
    main()

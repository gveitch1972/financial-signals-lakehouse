import os
import sys

try:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:
    repo_root = "/Workspace/Users/microsoft@grahamveitch.com/.bundle/financial-signals-lakehouse/dev/files"

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.bronze.ingest_market_data import main

if __name__ == "__main__":
    main()
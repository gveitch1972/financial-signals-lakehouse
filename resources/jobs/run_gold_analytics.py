import os
import sys

cwd = os.getcwd()
root = cwd

while root != "/" and not os.path.isdir(os.path.join(root, "src")):
    root = os.path.dirname(root)

print("Running daily_market_snapshot")
print("MARKER: gold-runner-v1")
print("CWD:", cwd)
print("DISCOVERED_ROOT:", root)
print("SRC_EXISTS:", os.path.isdir(os.path.join(root, "src")))

if root == "/" or not os.path.isdir(os.path.join(root, "src")):
    raise RuntimeError(f"Could not find project root containing src/. Started from: {cwd}")

if root not in sys.path:
    sys.path.insert(0, root)

print("sys.path[0]:", sys.path[0])

from src.gold.build_cross_signal_summary import main as build_cross_signal_summary
from src.gold.build_daily_market_snapshot import main as build_daily_market_snapshot
from src.gold.build_fx_trend_signals import main as build_fx_trend_signals
from src.gold.build_macro_indicator_trends import main as build_macro_indicator_trends


if __name__ == "__main__":
    print("Running gold layer pipelines")
    
    build_daily_market_snapshot()
    build_fx_trend_signals()
    #build_macro_indicator_trends()
    #build_cross_signal_summary()

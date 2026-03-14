import os
import sys

cwd = os.getcwd()
root = cwd

while root != "/" and not os.path.isdir(os.path.join(root, "src")):
    root = os.path.dirname(root)

print("MARKER: silver-runner-v1")
print("CWD:", cwd)
print("DISCOVERED_ROOT:", root)
print("SRC_EXISTS:", os.path.isdir(os.path.join(root, "src")))

if root == "/" or not os.path.isdir(os.path.join(root, "src")):
    raise RuntimeError(f"Could not find project root containing src/. Started from: {cwd}")

if root not in sys.path:
    sys.path.insert(0, root)

print("sys.path[0]:", sys.path[0])

from src.silver.transform_market_data import main

if __name__ == "__main__":
    main()
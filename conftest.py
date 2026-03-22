from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

# Ensure the real app.config is loaded before any tests try to monkeypatch
# sys.modules["app.config"] with a lightweight stub at import time.
import app.config  # noqa: F401,E402

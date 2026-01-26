# scripts/export_openapi.py
import json
from pathlib import Path

from ap_explanation.main import app

# Use path relative to this script file
output_path = Path(__file__).parent.parent / "docs" / \
    "docs" / "content" / "openapi.json"
output_path.parent.mkdir(parents=True, exist_ok=True)

with open(output_path, "w") as f:
    json.dump(app.openapi(), f, indent=2)

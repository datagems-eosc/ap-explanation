# scripts/export_openapi.py
import json

from provenance_demo.main import app

with open("docs/docs/content/openapi.json", "w") as f:
    json.dump(app.openapi(), f, indent=2)

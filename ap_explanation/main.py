import logging
from os import getenv
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from tomllib import loads as loads_toml

from ap_explanation.api.v1.routes import router
from ap_explanation.di import container_lifespan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retrieve current project version from toml
pyproject = loads_toml(Path("pyproject.toml").read_text())
project_version = pyproject["project"]["version"]

ROOT_PATH = getenv("ROOT_PATH", "")

app = FastAPI(
    title="AP Explanation API",
    description="API for explaining Analytical Patterns with provenance tracking using ProvSQL",
    version=project_version,
    lifespan=container_lifespan,
    root_path=ROOT_PATH,

)


@app.get("/")
def index():
    return {
        "service": "AP Explanation",
        "version": app.version
    }


app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)

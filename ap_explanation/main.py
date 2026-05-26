import logging
from os import getenv
from pathlib import Path
from tomllib import loads as loads_toml

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ap_explanation.api.v1.routes import router
from ap_explanation.di import container_lifespan
from ap_explanation.errors.exceptions import DatabaseNotFoundError
from ap_explanation.middlewares.correlation_id_passtrough import (
    correlation_id_passtrough,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retrieve current project version from toml (relative to this file)
pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
pyproject = loads_toml(pyproject_path.read_text())
project_version = pyproject["project"]["version"]

ROOT_PATH = getenv("ROOT_PATH", "")

app = FastAPI(
    title="AP Explanation API",
    description="API for explaining Analytical Patterns with provenance tracking using ProvSQL",
    version=project_version,
    lifespan=container_lifespan,
    root_path=ROOT_PATH,

)


@app.exception_handler(DatabaseNotFoundError)
async def database_not_found_handler(request: Request, exc: DatabaseNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"detail": exc.message},
    )


@app.get("/")
def index():
    return {
        "service": "AP Explanation",
        "version": app.version
    }


app.include_router(router)

############
# Middlewares
############
app.middleware("http")(correlation_id_passtrough)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)

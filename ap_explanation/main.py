import logging

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from ap_explanation.api.v1.routes import router
from ap_explanation.di import container_lifespan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(
    title="AP Explanation API",
    description="API for explaining Analytical Patterns with provenance tracking using ProvSQL",
    version="0.0.1",
    lifespan=container_lifespan,
)


@app.get("/")
def index():
    return {
        "service": "AP Explanation",
        "version": "0.0.1"
    }


app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)

from logging import getLogger

from fastapi import Depends, HTTPException, Request
from psycopg import AsyncConnection
from psycopg.errors import OperationalError

from provenance_demo.di import get_db_conn

logger = getLogger(__name__)


async def health_check(rq: Request, db: AsyncConnection = Depends(get_db_conn)):
    try:
        await db.execute("SELECT 1;")

        return {"status": "healthy"}

    except OperationalError as e:
        logger.error("Database connection failed", exc_info=e)
        raise HTTPException(
            status_code=503, detail="Database unavailable") from e

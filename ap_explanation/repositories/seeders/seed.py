from contextlib import AbstractAsyncContextManager, nullcontext
from functools import singledispatch
from pathlib import Path

from psycopg import AsyncConnection

from ap_explanation.domains.provenance.data_sources import DataSource


@singledispatch
def seed_database(
    data_source: DataSource, conn: AsyncConnection, src_dir: Path
) -> AbstractAsyncContextManager[None]:
    """
    Perform any necessary setup for a data source, such as creating tables or
    loading data, undone when the context exits. A relational database needs none.
    """
    return nullcontext()

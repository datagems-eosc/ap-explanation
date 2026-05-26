import logging
import sys

import structlog


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configures the logs to look like:
        {
            "timestamp": "2026-05-22T13:00:00.000000Z",
            "level": "info",
            "msg": "Sample log message",
            "DGCorrelationId": "...",
            "UserId": "...",
            "ClientId": "..."
        }
    Some request-scoped elements are required, such as the calling user id and the correaltion id.
    These can be set using contextvars. 
    """
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also format Python default logs records
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # Rename structlog's default "event" key to "msg".
            structlog.processors.EventRenamer("msg"),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

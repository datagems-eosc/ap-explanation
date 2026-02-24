import hashlib
import json
from typing import Any, Optional, Protocol, cast

import redis


class CacheProvider(Protocol):
    """Protocol for caching serialisable results by a string key.

    Implementations must be safe to call from multiple threads (the Celery
    worker pool uses threads by default).
    """

    def get(self, key: str) -> Optional[Any]:
        """Return the deserialised value stored under *key*, or ``None`` if absent."""
        ...

    def set(self, key: str, value: Any, ttl: int = ...) -> None:
        """Serialise *value* and store it under *key* with the given TTL (seconds)."""
        ...

    def delete(self, key: str) -> None:
        """Remove the entry stored under *key* (no-op if absent)."""
        ...


class RedisCacheProvider:
    """``CacheProvider`` backed by Redis.

    Values are serialised as JSON so that any JSON-compatible Python object can
    be round-tripped.  The default TTL matches the Celery result expiry (1 h).
    """

    DEFAULT_TTL = 3600  # seconds

    def __init__(self, redis_url: str):
        self._client = redis.from_url(redis_url)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def make_key(*parts: Any, prefix: str = "prov_cache") -> str:
        """Build a deterministic cache key from arbitrary positional *parts*.

        The parts are JSON-serialised, concatenated, and SHA-256 hashed so the
        key length stays bounded regardless of query size.

        Example::

            key = RedisCacheProvider.make_key(db_name, tables, schema, query, semiring)
        """
        raw = json.dumps(parts, sort_keys=True, default=str)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        return f"{prefix}:{digest}"

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[Any]:
        """Return the deserialised value stored under *key*, or ``None``."""
        raw = cast(Optional[bytes], self._client.get(key))
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, value: Any, ttl: int = DEFAULT_TTL) -> None:
        """JSON-serialise *value* and store it under *key* with *ttl* seconds expiry."""
        self._client.setex(key, ttl, json.dumps(value))

    def delete(self, key: str) -> None:
        """Delete *key* (no-op if absent)."""
        self._client.delete(key)

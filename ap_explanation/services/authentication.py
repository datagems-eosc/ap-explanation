import base64
import logging
import threading
import time

import requests
from jose import jwt

logger = logging.getLogger(__name__)


class Authentication:
    """Handles JWT validation and OIDC token exchange against a Keycloak realm."""

    def __init__(
        self,
        issuer: str,
        ttl: int = 300,
        client_id: str | None = None,
        client_secret: str | None = None,
        exchange_scope: str | None = None,
    ) -> None:
        self._issuer = issuer.rstrip("/")
        self._ttl = ttl
        self._client_id = client_id
        self._client_secret = client_secret
        self._exchange_scope = exchange_scope
        self._lock = threading.Lock()
        self._jwks: dict | None = None
        self._fetched_at: float = 0.0

        if not (self._client_id and self._client_secret and self._exchange_scope):
            raise ValueError(
                "Token exchange requires client_id, client_secret and exchange_scope"
            )

    def _get_jwks(self) -> dict:
        """Return the JWKS, refreshing the cache when stale."""
        now = time.monotonic()
        with self._lock:
            if self._jwks is None or (now - self._fetched_at) > self._ttl:
                url = f"{self._issuer}/protocol/openid-connect/certs"
                logger.info("Refreshing JWKS from %s", url)
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                self._jwks = resp.json()
                self._fetched_at = now
        return self._jwks

    def validate(self, token: str) -> dict:
        """Decode and validate *token*.

        Verifies signature (RS256), issuer, and audience (when configured).
        Raises ``jose.JWTError`` on any validation failure.
        """
        return jwt.decode(
            token,
            self._get_jwks(),
            algorithms=["RS256"],
            issuer=self._issuer,
            audience=self._client_id,
        )

    def exchange_token(self, subject_token: str) -> str:
        """Exchange *subject_token* for a dg-app-api scoped token via the OIDC Token Exchange flow.

        Raises
        ------
        ValueError
            When ``client_id``, ``client_secret`` or ``exchange_scope`` are not configured.
        requests.HTTPError
            When the AAI service returns an error response.
        """

        credentials = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode()
        ).decode()
        url = f"{self._issuer}/protocol/openid-connect/token"
        logger.debug("Exchanging token via %s", url)
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token": subject_token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "scope": self._exchange_scope,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

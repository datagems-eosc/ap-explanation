import logging
from typing import Optional

import structlog
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError

from ap_explanation.services.authentication import Authentication

logger = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)


def _authenticate(
    credentials: HTTPAuthorizationCredentials | None,
    authentication: Authentication,
) -> tuple[str, dict]:
    """Validate bearer credentials and return (raw_token, JWT claims). Raises 401 on failure."""
    if credentials is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = credentials.credentials
    try:
        claims = authentication.validate(token)
        structlog.contextvars.bind_contextvars(
            UserId=claims.get("sub", ""),
            ClientId=claims.get("azp", ""),
        )
        return token, claims
    except JWTError as exc:
        logger.warning("JWT validation failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    except Exception:
        logger.exception("Unexpected error during token validation")
        raise HTTPException(status_code=401, detail="Token validation failed")


def _exchange(authentication: Authentication, token: str) -> str:
    """Exchange *token* for a dg-app-api scoped token. Raises 502 on failure."""
    try:
        return authentication.exchange_token(token)
    except Exception:
        logger.exception("Token exchange failed")
        raise HTTPException(status_code=502, detail="Token exchange failed")


def require_authentication():
    """Validate the caller's bearer token without enforcing any RBAC role.

    Returns the JWT claims dict when authentication is enabled, ``None`` when
    it is disabled (``OIDC_ISSUER`` not set).
    """
    from ap_explanation.di import get_authentication_service  # Avoid circular import

    async def _check(
        credentials: HTTPAuthorizationCredentials | None = Depends(
            bearer_scheme),
        authentication: Optional[Authentication] = Depends(
            get_authentication_service),
    ) -> dict | None:
        if authentication is None:
            return None
        _, user = _authenticate(credentials, authentication)
        return user

    return _check

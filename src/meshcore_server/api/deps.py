"""Závislosti FastAPI: kontext serveru a ověření tokenu."""

from __future__ import annotations

from typing import Callable

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ..auth import Principal
from .context import ServerContext


def get_ctx(request: Request) -> ServerContext:
    return request.app.state.ctx


# Bearer schéma zaregistrované v OpenAPI – díky němu má /docs tlačítko Authorize.
# auto_error=False: chybějící token hlásíme sami (401 s vlastní hláškou).
bearer_scheme = HTTPBearer(auto_error=False, description="API token z `meshcore-server token create`")


def require(scope: str) -> Callable:
    """Dependency: vyžaduje platný token s daným (nebo vyšším) oprávněním."""

    async def dependency(
        ctx: ServerContext = Depends(get_ctx),
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    ) -> Principal:
        token = credentials.credentials if credentials else None
        principal = await ctx.tokens.authenticate(token)
        if principal is None:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail="chybí nebo je neplatný API token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if not principal.allows(scope):
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail=f"token nemá oprávnění {scope}")
        return principal

    return dependency

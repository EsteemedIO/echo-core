"""
Tenant Tracking Middleware for Echo Multi-Tenant Architecture.

This middleware extracts tenant information from authenticated requests and sets
context variables for downstream services. It supports both schema-based isolation
(default Onyx behavior) and full database isolation (Oceanic multi-tenant).

Author: Tig (AI Engineer) - Oceanic Platform
Feature: ECHO-044 Multi-Tenant Architecture
"""

import logging
import os
from collections.abc import Awaitable
from collections.abc import Callable

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response

from ee.onyx.auth.users import decode_anonymous_user_jwt_token
from onyx.auth.utils import extract_tenant_from_auth_header
from onyx.configs.constants import ANONYMOUS_USER_COOKIE_NAME
from onyx.configs.constants import TENANT_ID_COOKIE_NAME
from onyx.db.engine.sql_engine import is_valid_schema_name
from onyx.redis.redis_pool import retrieve_auth_token_data_from_redis
from shared_configs.configs import MULTI_TENANT
from shared_configs.configs import POSTGRES_DEFAULT_SCHEMA
from shared_configs.contextvars import CURRENT_TENANT_ID_CONTEXTVAR
from shared_configs.contextvars import set_tenant_context


# Environment variable for tenant isolation mode
# "schema" = schema-based isolation (default Onyx behavior)
# "database" = full database isolation (Oceanic multi-tenant)
TENANT_ISOLATION_MODE = os.getenv("TENANT_ISOLATION_MODE", "schema")


def add_api_server_tenant_id_middleware(
    app: FastAPI, logger: logging.LoggerAdapter
) -> None:
    @app.middleware("http")
    async def set_tenant_id(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Extracts the tenant id from multiple locations and sets the context var.

        This is very specific to the api server and probably not something you'd want
        to use elsewhere.

        In full database isolation mode (TENANT_ISOLATION_MODE="database"), this
        middleware also:
        - Looks up the tenant's dedicated database URL
        - Looks up the tenant's dedicated Vespa URL
        - Sets additional context variables for downstream routing
        """
        try:
            if MULTI_TENANT:
                tenant_id, org_id, db_url, vespa_url = await _get_tenant_info_from_request(
                    request, logger
                )

                # Determine isolation mode
                isolation_mode = TENANT_ISOLATION_MODE

                # Set all context variables at once
                set_tenant_context(
                    tenant_id=tenant_id,
                    org_id=org_id,
                    db_url=db_url,
                    vespa_url=vespa_url,
                    isolation_mode=isolation_mode,
                )
            else:
                CURRENT_TENANT_ID_CONTEXTVAR.set(POSTGRES_DEFAULT_SCHEMA)

            return await call_next(request)

        except Exception as e:
            logger.exception(f"Error in tenant ID middleware: {str(e)}")
            raise


async def _get_tenant_info_from_request(
    request: Request, logger: logging.LoggerAdapter
) -> tuple[str, str | None, str | None, str | None]:
    """
    Extract tenant information from the request.

    Returns a tuple of (tenant_id, org_id, db_url, vespa_url).

    The tenant_id is used for schema-based isolation.
    The org_id, db_url, and vespa_url are used for full database isolation.
    """
    tenant_id = POSTGRES_DEFAULT_SCHEMA
    org_id: str | None = None
    db_url: str | None = None
    vespa_url: str | None = None

    # Check for X-Tenant-Id header from Oceanic proxy (BFF multi-tenant pattern)
    # This takes precedence as Oceanic has already validated the user's org membership
    tenant_from_header = request.headers.get("X-Tenant-Id")
    if tenant_from_header and is_valid_schema_name(tenant_from_header):
        logger.debug(f"Using tenant from X-Tenant-Id header: {tenant_from_header}")
        org_id = tenant_from_header
        tenant_id = tenant_from_header

        # In full database isolation mode, look up tenant infrastructure
        if TENANT_ISOLATION_MODE == "database":
            db_url, vespa_url = await _lookup_tenant_infrastructure(
                org_id, request, logger
            )

        return tenant_id, org_id, db_url, vespa_url

    # Check for API key or PAT in Authorization header
    auth_tenant_id = extract_tenant_from_auth_header(request)
    if auth_tenant_id is not None:
        tenant_id = auth_tenant_id
        org_id = auth_tenant_id

        if TENANT_ISOLATION_MODE == "database":
            db_url, vespa_url = await _lookup_tenant_infrastructure(
                org_id, request, logger
            )

        return tenant_id, org_id, db_url, vespa_url

    try:
        # Look up token data in Redis
        token_data = await retrieve_auth_token_data_from_redis(request)

        if token_data:
            tenant_id_from_payload = token_data.get(
                "tenant_id", POSTGRES_DEFAULT_SCHEMA
            )

            tenant_id = (
                str(tenant_id_from_payload)
                if tenant_id_from_payload is not None
                else POSTGRES_DEFAULT_SCHEMA
            )

            if tenant_id and not is_valid_schema_name(tenant_id):
                raise HTTPException(status_code=400, detail="Invalid tenant ID format")

            # Extract organization_id if available in token
            org_id = token_data.get("organization_id") or tenant_id

            if TENANT_ISOLATION_MODE == "database" and org_id:
                db_url, vespa_url = await _lookup_tenant_infrastructure(
                    org_id, request, logger
                )

            return tenant_id, org_id, db_url, vespa_url

        # Check for anonymous user cookie
        anonymous_user_cookie = request.cookies.get(ANONYMOUS_USER_COOKIE_NAME)
        if anonymous_user_cookie:
            try:
                anonymous_user_data = decode_anonymous_user_jwt_token(
                    anonymous_user_cookie
                )
                tenant_id = anonymous_user_data.get(
                    "tenant_id", POSTGRES_DEFAULT_SCHEMA
                )
                org_id = anonymous_user_data.get("organization_id") or tenant_id

                if not tenant_id or not is_valid_schema_name(tenant_id):
                    raise HTTPException(
                        status_code=400, detail="Invalid tenant ID format"
                    )

                if TENANT_ISOLATION_MODE == "database" and org_id:
                    db_url, vespa_url = await _lookup_tenant_infrastructure(
                        org_id, request, logger
                    )

                return tenant_id, org_id, db_url, vespa_url

            except Exception as e:
                logger.error(f"Error decoding anonymous user cookie: {str(e)}")
                # Continue and attempt to authenticate

        logger.debug(
            "Token data not found or expired in Redis, defaulting to POSTGRES_DEFAULT_SCHEMA"
        )

        # Return POSTGRES_DEFAULT_SCHEMA, so non-authenticated requests are sent to the default schema
        # The CURRENT_TENANT_ID_CONTEXTVAR is initialized with POSTGRES_DEFAULT_SCHEMA,
        # so we maintain consistency by returning it here when no valid tenant is found.
        return POSTGRES_DEFAULT_SCHEMA, None, None, None

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in _get_tenant_info_from_request: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

    finally:
        if tenant_id and tenant_id != POSTGRES_DEFAULT_SCHEMA:
            # Check for explicit tenant_id cookie as fallback
            tenant_id_cookie = request.cookies.get(TENANT_ID_COOKIE_NAME)
            if tenant_id_cookie and is_valid_schema_name(tenant_id_cookie):
                return tenant_id_cookie, tenant_id_cookie, db_url, vespa_url

        # Final fallback
        return POSTGRES_DEFAULT_SCHEMA, None, None, None


async def _lookup_tenant_infrastructure(
    org_id: str,
    request: Request,
    logger: logging.LoggerAdapter,
) -> tuple[str | None, str | None]:
    """
    Look up tenant infrastructure from the tenant registry.

    This is called when TENANT_ISOLATION_MODE="database" to retrieve
    the dedicated database URL and Vespa URL for an Organization.

    Args:
        org_id: The Organization ID to look up
        request: The FastAPI request (for accessing app state if needed)
        logger: Logger adapter for debug output

    Returns:
        Tuple of (database_url, vespa_url), either may be None if not found
    """
    try:
        # Import here to avoid circular imports
        from onyx.db.tenant_registry import get_tenant_infrastructure
        from onyx.db.tenant_registry import TenantNotProvisionedError
        from onyx.db.tenant_registry import TenantSuspendedError

        # Get a session to the platform database
        # The platform database stores tenant_infrastructure table
        from onyx.db.engine.sql_engine import get_session_with_shared_schema

        with get_session_with_shared_schema() as platform_session:
            try:
                infrastructure = get_tenant_infrastructure(
                    organization_id=org_id,
                    platform_session=platform_session,
                )

                logger.debug(
                    f"Tenant infrastructure found for org {org_id}: "
                    f"db={infrastructure.database_url}, vespa={infrastructure.vespa_url}"
                )

                return infrastructure.database_url, infrastructure.vespa_url

            except TenantNotProvisionedError:
                logger.warning(
                    f"Tenant infrastructure not provisioned for org {org_id}"
                )
                # In this case, we allow the request to proceed with schema-based
                # isolation as a fallback. The Organization may be in the process
                # of being provisioned.
                return None, None

            except TenantSuspendedError:
                logger.error(f"Tenant infrastructure suspended for org {org_id}")
                raise HTTPException(
                    status_code=403,
                    detail="Organization access suspended. Please contact support.",
                )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error looking up tenant infrastructure for org {org_id}: {str(e)}"
        )
        # On lookup failure, fall back to schema-based isolation
        # This maintains backwards compatibility and allows the service to
        # continue functioning even if the tenant registry is unavailable
        return None, None


async def _get_tenant_id_from_request(
    request: Request, logger: logging.LoggerAdapter
) -> str:
    """
    Legacy function for backwards compatibility.

    Attempt to extract tenant_id from:
    0) X-Tenant-Id header (from Oceanic proxy - highest priority for BFF pattern)
    1) The API key or PAT (Personal Access Token) header
    2) The Redis-based token (stored in Cookie: fastapiusersauth)
    3) The anonymous user cookie
    Fallback: POSTGRES_DEFAULT_SCHEMA
    """
    tenant_id, _, _, _ = await _get_tenant_info_from_request(request, logger)
    return tenant_id

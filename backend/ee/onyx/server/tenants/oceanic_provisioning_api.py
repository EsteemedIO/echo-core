"""
Oceanic Tenant Provisioning API

Provides endpoints for Oceanic to lazily provision Echo tenant schemas.
This enables the BFF pattern where Oceanic manages authentication and
delegates RAG functionality to Echo.

Security: These endpoints are protected by X-Internal-Service header
verification to ensure only Oceanic agent-runner can call them.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from ee.onyx.server.tenants.provisioning import provision_tenant
from ee.onyx.server.tenants.schema_management import create_schema_if_not_exists
from onyx.db.engine.sql_engine import is_valid_schema_name
from shared_configs.configs import MULTI_TENANT

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tenants")

# Internal service key for Oceanic agent-runner
OCEANIC_INTERNAL_SERVICE_KEY = "oceanic-agent-runner"


class OceanicProvisionRequest(BaseModel):
    """Request payload for tenant provisioning from Oceanic"""
    tenant_id: str
    email: str
    organization_id: str


class ProvisionResponse(BaseModel):
    """Response from tenant provisioning"""
    success: bool
    tenant_id: str
    message: str


def verify_internal_service(x_internal_service: Optional[str] = Header(None)) -> None:
    """Verify the request is from Oceanic agent-runner"""
    if x_internal_service != OCEANIC_INTERNAL_SERVICE_KEY:
        raise HTTPException(
            status_code=403,
            detail="Access denied: Invalid internal service header"
        )


@router.get("/check/{tenant_id}")
async def check_tenant_exists(
    tenant_id: str,
    x_internal_service: Optional[str] = Header(None)
) -> dict:
    """
    Check if a tenant schema exists.

    This endpoint allows Oceanic to check if a tenant has been provisioned
    before making requests to Echo.

    Returns:
        200 if tenant exists
        404 if tenant does not exist
    """
    verify_internal_service(x_internal_service)

    if not MULTI_TENANT:
        # In single-tenant mode, always return exists
        return {"exists": True, "tenant_id": tenant_id}

    if not is_valid_schema_name(tenant_id):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid tenant_id format: {tenant_id}"
        )

    # Check if schema exists using the schema management utility
    try:
        # create_schema_if_not_exists returns True if schema was created, False if exists
        # We just need to check without creating, so we use a direct query approach
        from sqlalchemy import text
        from sqlalchemy.orm import Session
        from onyx.db.engine.sql_engine import get_sqlalchemy_engine

        with Session(get_sqlalchemy_engine()) as db_session:
            result = db_session.execute(
                text(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name"
                ),
                {"schema_name": tenant_id},
            )
            schema_exists = result.scalar() is not None

        if schema_exists:
            return {"exists": True, "tenant_id": tenant_id}
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Tenant {tenant_id} does not exist"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking tenant existence: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to check tenant existence"
        )


@router.post("/provision")
async def provision_oceanic_tenant(
    request: OceanicProvisionRequest,
    x_internal_service: Optional[str] = Header(None)
) -> ProvisionResponse:
    """
    Provision a new tenant schema for an Oceanic organization.

    This is called by Oceanic agent-runner when a user from a new organization
    first accesses Echo. It creates the PostgreSQL schema and runs migrations.

    Args:
        request: Contains tenant_id, email, and organization_id

    Returns:
        ProvisionResponse with success status and message

    Raises:
        409 Conflict if tenant already exists
        500 if provisioning fails
    """
    verify_internal_service(x_internal_service)

    if not MULTI_TENANT:
        return ProvisionResponse(
            success=True,
            tenant_id=request.tenant_id,
            message="Multi-tenant mode disabled, using default schema"
        )

    if not is_valid_schema_name(request.tenant_id):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid tenant_id format: {request.tenant_id}"
        )

    logger.info(
        f"Provisioning tenant {request.tenant_id} for org {request.organization_id} "
        f"(user: {request.email})"
    )

    try:
        # Check if schema already exists
        schema_created = create_schema_if_not_exists(request.tenant_id)

        if not schema_created:
            # Schema already exists
            logger.info(f"Tenant {request.tenant_id} already exists")
            raise HTTPException(
                status_code=409,
                detail=f"Tenant {request.tenant_id} already exists"
            )

        # Run the full provisioning flow (migrations, default settings, etc.)
        await provision_tenant(request.tenant_id, request.email)

        logger.info(f"Successfully provisioned tenant {request.tenant_id}")

        return ProvisionResponse(
            success=True,
            tenant_id=request.tenant_id,
            message=f"Tenant {request.tenant_id} provisioned successfully"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to provision tenant {request.tenant_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to provision tenant: {str(e)}"
        )

"""
Tenant Infrastructure Registry for Echo Multi-Tenant Architecture.

This module provides lookup and caching of tenant infrastructure configurations
from the tenant_infrastructure table. It supports full database isolation
per Organization as required for SOC2 compliance.

Author: Tig (AI Engineer) - Oceanic Platform
Feature: ECHO-044 Multi-Tenant Architecture
"""

import threading
import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from onyx.utils.logger import setup_logger


logger = setup_logger()


# Cache TTL in seconds (5 minutes as per spec)
TENANT_CACHE_TTL_SECONDS = 300


class TenantNotProvisionedError(Exception):
    """Raised when a tenant's infrastructure has not been provisioned."""

    def __init__(self, tenant_id: str, message: str | None = None):
        self.tenant_id = tenant_id
        self.message = message or f"Tenant '{tenant_id}' infrastructure not provisioned"
        super().__init__(self.message)


class TenantSuspendedError(Exception):
    """Raised when a tenant's infrastructure is suspended."""

    def __init__(self, tenant_id: str, message: str | None = None):
        self.tenant_id = tenant_id
        self.message = message or f"Tenant '{tenant_id}' infrastructure is suspended"
        super().__init__(self.message)


@dataclass
class TenantInfrastructure:
    """Represents a tenant's infrastructure configuration."""

    organization_id: str
    database_host: str
    database_port: int
    database_name: str
    vespa_host: str
    vespa_port: int
    status: str
    provisioned_at: Optional[str] = None
    last_health_check: Optional[str] = None
    metadata: Optional[dict] = None

    @property
    def database_url(self) -> str:
        """Constructs the PostgreSQL connection URL for this tenant."""
        # Note: Credentials are injected from environment at connection time
        # This URL is a template that will have user/password added
        return f"postgresql://{self.database_host}:{self.database_port}/{self.database_name}"

    @property
    def vespa_url(self) -> str:
        """Constructs the Vespa endpoint URL for this tenant."""
        return f"http://{self.vespa_host}:{self.vespa_port}"

    @property
    def vespa_config_url(self) -> str:
        """Constructs the Vespa config server URL for this tenant."""
        # Config server typically runs on port 19071
        return f"http://{self.vespa_host}:19071"

    def is_active(self) -> bool:
        """Check if tenant infrastructure is active and usable."""
        return self.status == "active"

    def is_provisioning(self) -> bool:
        """Check if tenant infrastructure is still being provisioned."""
        return self.status == "provisioning"

    def is_suspended(self) -> bool:
        """Check if tenant infrastructure is suspended."""
        return self.status == "suspended"


@dataclass
class CachedTenantEntry:
    """A cached tenant infrastructure entry with expiration tracking."""

    infrastructure: TenantInfrastructure
    cached_at: float  # Unix timestamp

    def is_expired(self, ttl_seconds: int = TENANT_CACHE_TTL_SECONDS) -> bool:
        """Check if this cache entry has expired."""
        return (time.time() - self.cached_at) > ttl_seconds


class TenantRegistry:
    """
    Singleton registry for tenant infrastructure configurations.

    This class maintains a cache of tenant infrastructure lookups with
    configurable TTL to reduce database queries. Thread-safe for concurrent
    access from multiple request handlers.
    """

    _instance: Optional["TenantRegistry"] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls) -> "TenantRegistry":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._cache: dict[str, CachedTenantEntry] = {}
        self._cache_lock: threading.RLock = threading.RLock()
        self._ttl_seconds: int = TENANT_CACHE_TTL_SECONDS
        self._initialized = True
        logger.info("TenantRegistry initialized with TTL=%d seconds", self._ttl_seconds)

    def set_ttl(self, ttl_seconds: int) -> None:
        """Update the cache TTL (useful for testing)."""
        self._ttl_seconds = ttl_seconds

    def get_tenant_infrastructure(
        self,
        organization_id: str,
        platform_session: Session,
        bypass_cache: bool = False,
    ) -> TenantInfrastructure:
        """
        Retrieve tenant infrastructure configuration.

        Args:
            organization_id: The Organization UUID/ID to look up
            platform_session: SQLAlchemy session connected to the platform database
            bypass_cache: If True, skip cache and query database directly

        Returns:
            TenantInfrastructure object with database and Vespa URLs

        Raises:
            TenantNotProvisionedError: If tenant infrastructure doesn't exist
            TenantSuspendedError: If tenant infrastructure is suspended
        """
        # Check cache first (unless bypassed)
        if not bypass_cache:
            cached = self._get_from_cache(organization_id)
            if cached is not None:
                return cached

        # Query the platform database
        infrastructure = self._query_tenant_infrastructure(
            organization_id, platform_session
        )

        # Cache the result
        self._add_to_cache(organization_id, infrastructure)

        return infrastructure

    def invalidate_cache(self, organization_id: str | None = None) -> None:
        """
        Invalidate cached tenant infrastructure.

        Args:
            organization_id: Specific tenant to invalidate, or None for all
        """
        with self._cache_lock:
            if organization_id is None:
                self._cache.clear()
                logger.info("TenantRegistry: Cleared all cached entries")
            elif organization_id in self._cache:
                del self._cache[organization_id]
                logger.info(
                    "TenantRegistry: Invalidated cache for tenant %s", organization_id
                )

    def _get_from_cache(self, organization_id: str) -> TenantInfrastructure | None:
        """Retrieve tenant infrastructure from cache if not expired."""
        with self._cache_lock:
            entry = self._cache.get(organization_id)
            if entry is None:
                return None

            if entry.is_expired(self._ttl_seconds):
                # Remove expired entry
                del self._cache[organization_id]
                logger.debug(
                    "TenantRegistry: Cache expired for tenant %s", organization_id
                )
                return None

            logger.debug("TenantRegistry: Cache hit for tenant %s", organization_id)
            return entry.infrastructure

    def _add_to_cache(
        self, organization_id: str, infrastructure: TenantInfrastructure
    ) -> None:
        """Add tenant infrastructure to cache."""
        with self._cache_lock:
            self._cache[organization_id] = CachedTenantEntry(
                infrastructure=infrastructure,
                cached_at=time.time(),
            )
            logger.debug("TenantRegistry: Cached tenant %s", organization_id)

    def _query_tenant_infrastructure(
        self, organization_id: str, platform_session: Session
    ) -> TenantInfrastructure:
        """
        Query the tenant_infrastructure table for a specific organization.

        This assumes the tenant_infrastructure table exists in the platform database.
        """
        query = text(
            """
            SELECT
                organization_id,
                database_host,
                database_port,
                database_name,
                vespa_host,
                vespa_port,
                status,
                provisioned_at,
                last_health_check,
                metadata
            FROM tenant_infrastructure
            WHERE organization_id = :org_id
            """
        )

        result = platform_session.execute(
            query, {"org_id": organization_id}
        ).fetchone()

        if result is None:
            logger.warning(
                "TenantRegistry: No infrastructure found for tenant %s", organization_id
            )
            raise TenantNotProvisionedError(organization_id)

        infrastructure = TenantInfrastructure(
            organization_id=str(result[0]),
            database_host=result[1],
            database_port=result[2],
            database_name=result[3],
            vespa_host=result[4],
            vespa_port=result[5],
            status=result[6],
            provisioned_at=str(result[7]) if result[7] else None,
            last_health_check=str(result[8]) if result[8] else None,
            metadata=result[9] if result[9] else None,
        )

        # Check infrastructure status
        if infrastructure.is_suspended():
            raise TenantSuspendedError(organization_id)

        if infrastructure.is_provisioning():
            logger.info(
                "TenantRegistry: Tenant %s is still provisioning", organization_id
            )

        return infrastructure


# Global singleton instance
_tenant_registry: TenantRegistry | None = None


def get_tenant_registry() -> TenantRegistry:
    """Get the global TenantRegistry singleton instance."""
    global _tenant_registry
    if _tenant_registry is None:
        _tenant_registry = TenantRegistry()
    return _tenant_registry


def get_tenant_infrastructure(
    organization_id: str,
    platform_session: Session,
    bypass_cache: bool = False,
) -> TenantInfrastructure:
    """
    Convenience function to get tenant infrastructure.

    Args:
        organization_id: The Organization UUID/ID to look up
        platform_session: SQLAlchemy session connected to the platform database
        bypass_cache: If True, skip cache and query database directly

    Returns:
        TenantInfrastructure object with database and Vespa URLs
    """
    return get_tenant_registry().get_tenant_infrastructure(
        organization_id, platform_session, bypass_cache
    )

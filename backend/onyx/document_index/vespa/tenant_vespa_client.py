"""
Tenant-Aware Vespa Client for Echo Multi-Tenant Architecture.

This module provides Vespa client routing to tenant-specific Vespa instances
for full database isolation mode. Each Organization can have its own dedicated
Vespa instance with separate document stores.

Author: Tig (AI Engineer) - Oceanic Platform
Feature: ECHO-044 Multi-Tenant Architecture
"""

import threading
import time
from dataclasses import dataclass
from typing import Any
from typing import cast

import httpx

from onyx.configs.app_configs import MANAGED_VESPA
from onyx.configs.app_configs import VESPA_CLOUD_CERT_PATH
from onyx.configs.app_configs import VESPA_CLOUD_KEY_PATH
from onyx.configs.app_configs import VESPA_REQUEST_TIMEOUT
from onyx.document_index.vespa_constants import VESPA_APP_CONTAINER_URL
from onyx.utils.logger import setup_logger
from shared_configs.contextvars import get_current_vespa_url
from shared_configs.contextvars import is_full_database_isolation


logger = setup_logger()


class TenantVespaNotConfiguredError(Exception):
    """Raised when tenant Vespa URL is not configured but required."""

    def __init__(self, message: str = "Tenant Vespa URL not configured"):
        super().__init__(message)


@dataclass
class CachedVespaClient:
    """A cached Vespa HTTP client with metadata."""

    client: httpx.Client
    vespa_url: str
    created_at: float
    last_access: float
    last_health_check: float | None = None
    is_healthy: bool = True


class TenantVespaClientPool:
    """
    Maintains HTTP clients for tenant-specific Vespa instances.

    This pool creates and caches httpx clients for each tenant's Vespa instance,
    allowing for efficient connection reuse while maintaining tenant isolation.

    Thread-safe with LRU eviction for inactive clients.
    """

    _instance: "TenantVespaClientPool | None" = None
    _lock: threading.Lock = threading.Lock()

    # Client pool settings
    MAX_CLIENTS = 50  # Maximum number of cached Vespa clients
    HEALTH_CHECK_INTERVAL = 60  # Seconds between health checks
    CLIENT_TIMEOUT = VESPA_REQUEST_TIMEOUT  # Request timeout in seconds

    def __new__(cls) -> "TenantVespaClientPool":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return

        self._clients: dict[str, CachedVespaClient] = {}
        self._client_lock: threading.RLock = threading.RLock()
        self._initialized = True
        logger.info("TenantVespaClientPool initialized (max_clients=%d)", self.MAX_CLIENTS)

    def get_client(self, vespa_url: str | None = None) -> httpx.Client:
        """
        Get or create an HTTP client for the specified Vespa URL.

        Args:
            vespa_url: The Vespa instance URL. If None, uses the context variable
                       or falls back to the default Vespa URL.

        Returns:
            httpx.Client configured for the Vespa instance
        """
        # Determine the target URL
        target_url = self._resolve_vespa_url(vespa_url)

        with self._client_lock:
            # Check if client exists and is valid
            if target_url in self._clients:
                cached = self._clients[target_url]
                cached.last_access = time.time()

                # Optionally run periodic health check
                if self._should_health_check(cached):
                    self._perform_health_check(cached)

                if cached.is_healthy:
                    return cached.client

                # If unhealthy, remove and recreate
                self._remove_client(target_url)

            # Check if we need to evict old clients
            if len(self._clients) >= self.MAX_CLIENTS:
                self._evict_oldest_client()

            # Create new client
            client = self._create_client(target_url)
            self._clients[target_url] = CachedVespaClient(
                client=client,
                vespa_url=target_url,
                created_at=time.time(),
                last_access=time.time(),
            )

            logger.info("TenantVespaClientPool: Created client for %s", target_url)
            return client

    def _resolve_vespa_url(self, vespa_url: str | None) -> str:
        """Resolve the Vespa URL to use."""
        if vespa_url:
            return vespa_url

        # Check context variable for tenant-specific URL
        if is_full_database_isolation():
            context_url = get_current_vespa_url()
            if context_url:
                return context_url

        # Fall back to default Vespa URL
        return VESPA_APP_CONTAINER_URL

    def _create_client(self, vespa_url: str) -> httpx.Client:
        """Create a new HTTP client for a Vespa instance."""
        return httpx.Client(
            base_url=vespa_url,
            cert=(
                cast(tuple[str, str], (VESPA_CLOUD_CERT_PATH, VESPA_CLOUD_KEY_PATH))
                if MANAGED_VESPA
                else None
            ),
            verify=False if not MANAGED_VESPA else True,
            timeout=self.CLIENT_TIMEOUT,
            http2=True,
        )

    def _should_health_check(self, cached: CachedVespaClient) -> bool:
        """Check if a health check should be performed."""
        if cached.last_health_check is None:
            return True
        return (time.time() - cached.last_health_check) > self.HEALTH_CHECK_INTERVAL

    def _perform_health_check(self, cached: CachedVespaClient) -> None:
        """Perform a health check on a Vespa client."""
        try:
            response = cached.client.get("/state/v1/health", timeout=5.0)
            response.raise_for_status()
            response_data = response.json()
            cached.is_healthy = response_data.get("status", {}).get("code") == "up"
            cached.last_health_check = time.time()
            logger.debug(
                "TenantVespaClientPool: Health check for %s: %s",
                cached.vespa_url,
                "healthy" if cached.is_healthy else "unhealthy",
            )
        except Exception as e:
            logger.warning(
                "TenantVespaClientPool: Health check failed for %s: %s",
                cached.vespa_url,
                str(e),
            )
            cached.is_healthy = False
            cached.last_health_check = time.time()

    def _remove_client(self, vespa_url: str) -> None:
        """Remove and close a client."""
        cached = self._clients.pop(vespa_url, None)
        if cached:
            try:
                cached.client.close()
            except Exception as e:
                logger.warning(
                    "TenantVespaClientPool: Error closing client for %s: %s",
                    vespa_url,
                    str(e),
                )

    def _evict_oldest_client(self) -> None:
        """Evict the least recently used client."""
        if not self._clients:
            return

        oldest_url = min(
            self._clients,
            key=lambda url: self._clients[url].last_access,
        )
        self._remove_client(oldest_url)
        logger.info("TenantVespaClientPool: Evicted client for %s", oldest_url)

    def invalidate_client(self, vespa_url: str) -> None:
        """Invalidate and remove a specific client."""
        with self._client_lock:
            self._remove_client(vespa_url)
            logger.info("TenantVespaClientPool: Invalidated client for %s", vespa_url)

    def close_all(self) -> None:
        """Close all cached clients."""
        with self._client_lock:
            for url in list(self._clients.keys()):
                self._remove_client(url)
            logger.info("TenantVespaClientPool: Closed all clients")

    def get_stats(self) -> dict[str, Any]:
        """Get pool statistics for monitoring."""
        with self._client_lock:
            return {
                "total_clients": len(self._clients),
                "max_clients": self.MAX_CLIENTS,
                "clients": {
                    url: {
                        "is_healthy": c.is_healthy,
                        "created_at": c.created_at,
                        "last_access": c.last_access,
                        "last_health_check": c.last_health_check,
                    }
                    for url, c in self._clients.items()
                },
            }


# Global singleton instance
_vespa_client_pool: TenantVespaClientPool | None = None


def get_tenant_vespa_client_pool() -> TenantVespaClientPool:
    """Get the global TenantVespaClientPool singleton instance."""
    global _vespa_client_pool
    if _vespa_client_pool is None:
        _vespa_client_pool = TenantVespaClientPool()
    return _vespa_client_pool


def get_tenant_vespa_client(vespa_url: str | None = None) -> httpx.Client:
    """
    Get an HTTP client for a tenant's Vespa instance.

    This is the primary function to use when making Vespa requests in
    multi-tenant mode. It will:
    1. Use the provided vespa_url if given
    2. Check context variable for tenant-specific URL (full isolation mode)
    3. Fall back to the default Vespa instance

    Args:
        vespa_url: Optional explicit Vespa URL to use

    Returns:
        httpx.Client configured for the appropriate Vespa instance
    """
    return get_tenant_vespa_client_pool().get_client(vespa_url)


def get_tenant_vespa_url() -> str:
    """
    Get the current tenant's Vespa URL.

    Returns:
        The Vespa URL from context variable (if in full isolation mode)
        or the default Vespa URL.
    """
    if is_full_database_isolation():
        context_url = get_current_vespa_url()
        if context_url:
            return context_url
    return VESPA_APP_CONTAINER_URL


def get_tenant_document_endpoint(index_name: str) -> str:
    """
    Get the document API endpoint for the current tenant's Vespa instance.

    Args:
        index_name: The Vespa schema/index name

    Returns:
        Full URL for the document API endpoint
    """
    base_url = get_tenant_vespa_url()
    return f"{base_url}/document/v1/default/{index_name}/docid"


def get_tenant_search_endpoint() -> str:
    """
    Get the search API endpoint for the current tenant's Vespa instance.

    Returns:
        Full URL for the search API endpoint
    """
    base_url = get_tenant_vespa_url()
    return f"{base_url}/search/"


def get_tenant_config_endpoint() -> str:
    """
    Get the config server endpoint for the current tenant's Vespa instance.

    Returns:
        Full URL for the Vespa config server
    """
    if is_full_database_isolation():
        context_url = get_current_vespa_url()
        if context_url:
            # Config server is typically on port 19071 at the same host
            # Parse the URL and change the port
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(context_url)
            config_netloc = f"{parsed.hostname}:19071"
            return urlunparse((parsed.scheme, config_netloc, "", "", "", ""))

    # Fall back to default config server
    from onyx.document_index.vespa_constants import VESPA_CONFIG_SERVER_URL

    return VESPA_CONFIG_SERVER_URL

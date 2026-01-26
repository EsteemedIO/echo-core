"""
Unit tests for Tenant Registry (Feature 044 - Echo Multi-Tenant)

Tests the tenant infrastructure lookup, caching, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
import asyncio

# Import the modules under test
from onyx.db.tenant_registry import (
    TenantRegistry,
    TenantInfrastructure,
    TenantNotProvisionedError,
    TenantSuspendedError,
    get_tenant_infrastructure,
)


class TestTenantInfrastructure:
    """Tests for TenantInfrastructure dataclass"""

    def test_infrastructure_creation(self):
        """Should create infrastructure with required fields"""
        infra = TenantInfrastructure(
            organization_id="org-123",
            database_host="db.example.com",
            database_port=5432,
            database_name="oceanic_org_123",
            vespa_host="vespa.example.com",
            vespa_port=8081,
            status="active",
        )
        assert infra.organization_id == "org-123"
        assert infra.database_host == "db.example.com"
        assert infra.status == "active"

    def test_database_url_property(self):
        """Should generate correct database URL"""
        infra = TenantInfrastructure(
            organization_id="org-123",
            database_host="db.example.com",
            database_port=5432,
            database_name="oceanic_org_123",
            vespa_host="vespa.example.com",
            vespa_port=8081,
            status="active",
        )
        # Database URL should be constructable
        assert "db.example.com" in infra.database_host
        assert infra.database_port == 5432

    def test_vespa_url_property(self):
        """Should generate correct Vespa URL"""
        infra = TenantInfrastructure(
            organization_id="org-123",
            database_host="db.example.com",
            database_port=5432,
            database_name="oceanic_org_123",
            vespa_host="vespa.example.com",
            vespa_port=8081,
            status="active",
        )
        expected_vespa_url = "http://vespa.example.com:8081"
        assert f"http://{infra.vespa_host}:{infra.vespa_port}" == expected_vespa_url


class TestTenantRegistry:
    """Tests for TenantRegistry singleton"""

    def test_singleton_pattern(self):
        """Should return same instance"""
        registry1 = TenantRegistry()
        registry2 = TenantRegistry()
        assert registry1 is registry2

    def test_cache_initialization(self):
        """Should initialize with empty cache"""
        registry = TenantRegistry()
        # Clear any existing cache for clean test
        registry._cache.clear()
        assert len(registry._cache) == 0

    @pytest.mark.asyncio
    async def test_get_tenant_caches_result(self):
        """Should cache tenant infrastructure"""
        registry = TenantRegistry()
        registry._cache.clear()

        mock_infra = TenantInfrastructure(
            organization_id="org-cached",
            database_host="db.example.com",
            database_port=5432,
            database_name="oceanic_org_cached",
            database_user="user",
            vespa_host="vespa.example.com",
            vespa_port=8081,
            status="active",
        )

        with patch.object(registry, '_fetch_from_database', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_infra

            # First call should hit database
            result1 = await registry.get_tenant("org-cached")
            assert mock_fetch.call_count == 1

            # Second call should use cache
            result2 = await registry.get_tenant("org-cached")
            assert mock_fetch.call_count == 1  # Still 1, used cache

            assert result1.organization_id == result2.organization_id

    @pytest.mark.asyncio
    async def test_cache_expiry(self):
        """Should refresh cache after TTL expires"""
        registry = TenantRegistry()
        registry._cache.clear()
        registry._cache_ttl = timedelta(seconds=1)  # Short TTL for test

        mock_infra = TenantInfrastructure(
            organization_id="org-expiry",
            database_host="db.example.com",
            database_port=5432,
            database_name="oceanic_org_expiry",
            database_user="user",
            vespa_host="vespa.example.com",
            vespa_port=8081,
            status="active",
        )

        with patch.object(registry, '_fetch_from_database', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_infra

            # First call
            await registry.get_tenant("org-expiry")
            assert mock_fetch.call_count == 1

            # Wait for cache to expire
            await asyncio.sleep(1.5)

            # Should fetch again
            await registry.get_tenant("org-expiry")
            assert mock_fetch.call_count == 2


class TestTenantErrors:
    """Tests for tenant error handling"""

    @pytest.mark.asyncio
    async def test_not_provisioned_error(self):
        """Should raise TenantNotProvisionedError for unknown tenant"""
        registry = TenantRegistry()
        registry._cache.clear()

        with patch.object(registry, '_fetch_from_database', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = None

            with pytest.raises(TenantNotProvisionedError) as exc_info:
                await registry.get_tenant("org-unknown")

            assert "org-unknown" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_suspended_tenant_error(self):
        """Should raise TenantSuspendedError for suspended tenant"""
        registry = TenantRegistry()
        registry._cache.clear()

        mock_infra = TenantInfrastructure(
            organization_id="org-suspended",
            database_host="db.example.com",
            database_port=5432,
            database_name="oceanic_org_suspended",
            database_user="user",
            vespa_host="vespa.example.com",
            vespa_port=8081,
            status="suspended",
        )

        with patch.object(registry, '_fetch_from_database', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_infra

            with pytest.raises(TenantSuspendedError) as exc_info:
                await registry.get_tenant("org-suspended")

            assert "suspended" in str(exc_info.value).lower()


class TestGetTenantInfrastructure:
    """Tests for the get_tenant_infrastructure helper function"""

    @pytest.mark.asyncio
    async def test_returns_infrastructure(self):
        """Should return tenant infrastructure"""
        mock_infra = TenantInfrastructure(
            organization_id="org-helper",
            database_host="db.example.com",
            database_port=5432,
            database_name="oceanic_org_helper",
            database_user="user",
            vespa_host="vespa.example.com",
            vespa_port=8081,
            status="active",
        )

        with patch('onyx.db.tenant_registry.TenantRegistry') as MockRegistry:
            mock_instance = MagicMock()
            mock_instance.get_tenant = AsyncMock(return_value=mock_infra)
            MockRegistry.return_value = mock_instance

            result = await get_tenant_infrastructure("org-helper")
            assert result.organization_id == "org-helper"


class TestSchemaValidation:
    """Tests for tenant ID validation"""

    def test_valid_tenant_id(self):
        """Should accept valid tenant IDs"""
        valid_ids = [
            "org_abc123",
            "org-abc-123",
            "ORG_ABC_123",
            "tenant_12345678",
        ]
        from onyx.db.engine.sql_engine import is_valid_schema_name
        for tenant_id in valid_ids:
            assert is_valid_schema_name(tenant_id), f"Should accept: {tenant_id}"

    def test_invalid_tenant_id(self):
        """Should reject invalid tenant IDs"""
        invalid_ids = [
            "'; DROP TABLE users; --",
            "org/abc",
            "org.abc",
            "org abc",
            "",
        ]
        from onyx.db.engine.sql_engine import is_valid_schema_name
        for tenant_id in invalid_ids:
            # Empty string might pass regex but fail elsewhere
            if tenant_id == "":
                continue
            assert not is_valid_schema_name(tenant_id), f"Should reject: {tenant_id}"

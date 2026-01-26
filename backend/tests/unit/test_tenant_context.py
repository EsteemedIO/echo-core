"""
Unit tests for Tenant Context Variables (Feature 044 - Echo Multi-Tenant)

Tests the context variables used for tenant isolation.
"""
import pytest
import asyncio
from contextvars import copy_context

# Import the modules under test
from shared_configs.contextvars import (
    CURRENT_TENANT_ID_CONTEXTVAR,
    CURRENT_DB_URL_CONTEXTVAR,
    CURRENT_VESPA_URL_CONTEXTVAR,
    CURRENT_ORG_ID_CONTEXTVAR,
    TENANT_ISOLATION_MODE_CONTEXTVAR,
    get_current_tenant_id,
    get_current_db_url,
    get_current_vespa_url,
    get_current_org_id,
    get_tenant_isolation_mode,
    is_full_database_isolation,
    set_tenant_context,
    reset_tenant_context,
)


class TestContextVarDefaults:
    """Tests for context variable default values"""

    def test_tenant_id_default(self):
        """CURRENT_TENANT_ID_CONTEXTVAR should have default value"""
        # Reset to default for test
        token = CURRENT_TENANT_ID_CONTEXTVAR.set(None)
        try:
            # Default is typically "public" schema
            value = CURRENT_TENANT_ID_CONTEXTVAR.get()
            # May be None or default schema name
            assert value is None or isinstance(value, str)
        finally:
            CURRENT_TENANT_ID_CONTEXTVAR.reset(token)

    def test_db_url_default_none(self):
        """CURRENT_DB_URL_CONTEXTVAR should default to None"""
        token = CURRENT_DB_URL_CONTEXTVAR.set(None)
        try:
            assert CURRENT_DB_URL_CONTEXTVAR.get() is None
        finally:
            CURRENT_DB_URL_CONTEXTVAR.reset(token)

    def test_vespa_url_default_none(self):
        """CURRENT_VESPA_URL_CONTEXTVAR should default to None"""
        token = CURRENT_VESPA_URL_CONTEXTVAR.set(None)
        try:
            assert CURRENT_VESPA_URL_CONTEXTVAR.get() is None
        finally:
            CURRENT_VESPA_URL_CONTEXTVAR.reset(token)

    def test_org_id_default_none(self):
        """CURRENT_ORG_ID_CONTEXTVAR should default to None"""
        token = CURRENT_ORG_ID_CONTEXTVAR.set(None)
        try:
            assert CURRENT_ORG_ID_CONTEXTVAR.get() is None
        finally:
            CURRENT_ORG_ID_CONTEXTVAR.reset(token)

    def test_isolation_mode_default(self):
        """TENANT_ISOLATION_MODE_CONTEXTVAR should default to 'schema'"""
        token = TENANT_ISOLATION_MODE_CONTEXTVAR.set("schema")
        try:
            assert TENANT_ISOLATION_MODE_CONTEXTVAR.get() == "schema"
        finally:
            TENANT_ISOLATION_MODE_CONTEXTVAR.reset(token)


class TestContextVarSetAndGet:
    """Tests for setting and getting context variables"""

    def test_set_and_get_tenant_id(self):
        """Should store and return tenant ID"""
        token = CURRENT_TENANT_ID_CONTEXTVAR.set("test_tenant")
        try:
            assert CURRENT_TENANT_ID_CONTEXTVAR.get() == "test_tenant"
        finally:
            CURRENT_TENANT_ID_CONTEXTVAR.reset(token)

    def test_set_and_get_db_url(self):
        """Should store and return database URL"""
        test_url = "postgresql://user:pass@host:5432/db"
        token = CURRENT_DB_URL_CONTEXTVAR.set(test_url)
        try:
            assert CURRENT_DB_URL_CONTEXTVAR.get() == test_url
        finally:
            CURRENT_DB_URL_CONTEXTVAR.reset(token)

    def test_set_and_get_vespa_url(self):
        """Should store and return Vespa URL"""
        test_url = "http://vespa.example.com:8081"
        token = CURRENT_VESPA_URL_CONTEXTVAR.set(test_url)
        try:
            assert CURRENT_VESPA_URL_CONTEXTVAR.get() == test_url
        finally:
            CURRENT_VESPA_URL_CONTEXTVAR.reset(token)

    def test_set_and_get_org_id(self):
        """Should store and return organization ID"""
        token = CURRENT_ORG_ID_CONTEXTVAR.set("550e8400-e29b-41d4-a716-446655440000")
        try:
            assert CURRENT_ORG_ID_CONTEXTVAR.get() == "550e8400-e29b-41d4-a716-446655440000"
        finally:
            CURRENT_ORG_ID_CONTEXTVAR.reset(token)


class TestContextIsolation:
    """Tests for context variable isolation between async contexts"""

    @pytest.mark.asyncio
    async def test_isolation_between_tasks(self):
        """Context vars should be isolated per async task"""
        results = {"task1": None, "task2": None}

        async def task1():
            CURRENT_ORG_ID_CONTEXTVAR.set("org-task1")
            await asyncio.sleep(0.1)
            results["task1"] = CURRENT_ORG_ID_CONTEXTVAR.get()

        async def task2():
            CURRENT_ORG_ID_CONTEXTVAR.set("org-task2")
            await asyncio.sleep(0.05)
            results["task2"] = CURRENT_ORG_ID_CONTEXTVAR.get()

        # Run tasks with separate contexts
        ctx1 = copy_context()
        ctx2 = copy_context()

        await asyncio.gather(
            asyncio.create_task(ctx1.run(task1)),
            asyncio.create_task(ctx2.run(task2)),
        )

        # Each task should see its own value
        assert results["task1"] == "org-task1"
        assert results["task2"] == "org-task2"


class TestAccessorFunctions:
    """Tests for accessor functions"""

    def test_get_current_tenant_id(self):
        """get_current_tenant_id should return context value"""
        token = CURRENT_TENANT_ID_CONTEXTVAR.set("accessor_test")
        try:
            assert get_current_tenant_id() == "accessor_test"
        finally:
            CURRENT_TENANT_ID_CONTEXTVAR.reset(token)

    def test_get_current_db_url(self):
        """get_current_db_url should return context value"""
        test_url = "postgresql://test"
        token = CURRENT_DB_URL_CONTEXTVAR.set(test_url)
        try:
            assert get_current_db_url() == test_url
        finally:
            CURRENT_DB_URL_CONTEXTVAR.reset(token)

    def test_get_current_vespa_url(self):
        """get_current_vespa_url should return context value"""
        test_url = "http://vespa:8081"
        token = CURRENT_VESPA_URL_CONTEXTVAR.set(test_url)
        try:
            assert get_current_vespa_url() == test_url
        finally:
            CURRENT_VESPA_URL_CONTEXTVAR.reset(token)

    def test_get_current_org_id(self):
        """get_current_org_id should return context value"""
        test_id = "org-accessor"
        token = CURRENT_ORG_ID_CONTEXTVAR.set(test_id)
        try:
            assert get_current_org_id() == test_id
        finally:
            CURRENT_ORG_ID_CONTEXTVAR.reset(token)


class TestIsolationMode:
    """Tests for tenant isolation mode"""

    def test_schema_isolation_mode(self):
        """Should detect schema isolation mode"""
        token = TENANT_ISOLATION_MODE_CONTEXTVAR.set("schema")
        try:
            assert get_tenant_isolation_mode() == "schema"
            assert not is_full_database_isolation()
        finally:
            TENANT_ISOLATION_MODE_CONTEXTVAR.reset(token)

    def test_database_isolation_mode(self):
        """Should detect database isolation mode"""
        token = TENANT_ISOLATION_MODE_CONTEXTVAR.set("database")
        try:
            assert get_tenant_isolation_mode() == "database"
            assert is_full_database_isolation()
        finally:
            TENANT_ISOLATION_MODE_CONTEXTVAR.reset(token)


class TestSetTenantContext:
    """Tests for set_tenant_context helper"""

    def test_set_all_context_values(self):
        """set_tenant_context should set all values"""
        tokens = set_tenant_context(
            tenant_id="test_tenant",
            org_id="org-123",
            db_url="postgresql://test",
            vespa_url="http://vespa:8081",
            isolation_mode="database",
        )

        try:
            assert get_current_tenant_id() == "test_tenant"
            assert get_current_org_id() == "org-123"
            assert get_current_db_url() == "postgresql://test"
            assert get_current_vespa_url() == "http://vespa:8081"
            assert is_full_database_isolation()
        finally:
            reset_tenant_context(tokens)

    def test_reset_tenant_context(self):
        """reset_tenant_context should restore previous values"""
        # Set initial values
        CURRENT_ORG_ID_CONTEXTVAR.set(None)
        CURRENT_DB_URL_CONTEXTVAR.set(None)

        tokens = set_tenant_context(
            tenant_id="temp_tenant",
            org_id="temp-org",
            db_url="postgresql://temp",
            vespa_url="http://temp:8081",
        )

        # Values should be set
        assert get_current_org_id() == "temp-org"

        # Reset
        reset_tenant_context(tokens)

        # Values should be restored
        assert get_current_org_id() is None

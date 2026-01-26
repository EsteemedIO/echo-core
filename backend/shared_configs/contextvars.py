import contextvars

from shared_configs.configs import MULTI_TENANT
from shared_configs.configs import POSTGRES_DEFAULT_SCHEMA


# Context variable for the current tenant id
CURRENT_TENANT_ID_CONTEXTVAR: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar(
        "current_tenant_id", default=None if MULTI_TENANT else POSTGRES_DEFAULT_SCHEMA
    )
)

# set by every route in the API server
INDEXING_REQUEST_ID_CONTEXTVAR: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("indexing_request_id", default=None)
)

# set by every route in the API server
ONYX_REQUEST_ID_CONTEXTVAR: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "onyx_request_id", default=None
)

# Used to store cc pair id and index attempt id in multithreaded environments
INDEX_ATTEMPT_INFO_CONTEXTVAR: contextvars.ContextVar[tuple[int, int] | None] = (
    contextvars.ContextVar("index_attempt_info", default=None)
)


# =============================================================================
# Multi-Tenant Full Database Isolation Context Variables
# Feature: ECHO-044 Multi-Tenant Architecture
# =============================================================================

# Context variable for the current tenant's database URL
# Set by tenant routing middleware when Organization ID is resolved
CURRENT_DB_URL_CONTEXTVAR: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_db_url", default=None
)

# Context variable for the current tenant's Vespa URL
# Set by tenant routing middleware when Organization ID is resolved
CURRENT_VESPA_URL_CONTEXTVAR: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("current_vespa_url", default=None)
)

# Context variable for the current tenant's Organization ID
# Higher-level than tenant_id, represents the actual Organization UUID
CURRENT_ORG_ID_CONTEXTVAR: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_org_id", default=None
)

# Context variable for tenant isolation mode
# Values: "schema" (default Onyx behavior) or "database" (full DB isolation)
TENANT_ISOLATION_MODE_CONTEXTVAR: contextvars.ContextVar[str] = contextvars.ContextVar(
    "tenant_isolation_mode", default="schema"
)


"""Utils related to contextvars"""


def get_current_tenant_id() -> str:
    tenant_id = CURRENT_TENANT_ID_CONTEXTVAR.get()
    if tenant_id is None:
        import traceback

        if not MULTI_TENANT:
            return POSTGRES_DEFAULT_SCHEMA

        stack_trace = traceback.format_stack()
        error_message = (
            "Tenant ID is not set. This should never happen.\nStack trace:\n"
            + "".join(stack_trace)
        )
        raise RuntimeError(error_message)
    return tenant_id


def get_current_db_url() -> str | None:
    """
    Get the current tenant's database URL from context.

    Returns:
        The database URL if set (full database isolation mode),
        or None if using schema-based isolation.
    """
    return CURRENT_DB_URL_CONTEXTVAR.get()


def get_current_vespa_url() -> str | None:
    """
    Get the current tenant's Vespa URL from context.

    Returns:
        The Vespa URL if set (full database isolation mode),
        or None if using shared Vespa instance.
    """
    return CURRENT_VESPA_URL_CONTEXTVAR.get()


def get_current_org_id() -> str | None:
    """
    Get the current Organization ID from context.

    Returns:
        The Organization UUID if set, or None.
    """
    return CURRENT_ORG_ID_CONTEXTVAR.get()


def get_tenant_isolation_mode() -> str:
    """
    Get the current tenant isolation mode.

    Returns:
        "schema" for schema-based isolation (default),
        "database" for full database isolation.
    """
    return TENANT_ISOLATION_MODE_CONTEXTVAR.get()


def is_full_database_isolation() -> bool:
    """
    Check if the current context is using full database isolation.

    Returns:
        True if using dedicated tenant database, False for schema isolation.
    """
    return get_tenant_isolation_mode() == "database"


def set_tenant_context(
    tenant_id: str | None = None,
    org_id: str | None = None,
    db_url: str | None = None,
    vespa_url: str | None = None,
    isolation_mode: str = "schema",
) -> dict[str, contextvars.Token]:
    """
    Set all tenant context variables at once.

    This is a convenience function for the middleware to set all
    tenant-related context variables in one call.

    Args:
        tenant_id: The tenant schema name (for schema isolation)
        org_id: The Organization UUID
        db_url: The tenant-specific database URL (for full isolation)
        vespa_url: The tenant-specific Vespa URL (for full isolation)
        isolation_mode: Either "schema" or "database"

    Returns:
        Dictionary of context tokens for later reset
    """
    tokens = {}

    if tenant_id is not None:
        tokens["tenant_id"] = CURRENT_TENANT_ID_CONTEXTVAR.set(tenant_id)

    if org_id is not None:
        tokens["org_id"] = CURRENT_ORG_ID_CONTEXTVAR.set(org_id)

    if db_url is not None:
        tokens["db_url"] = CURRENT_DB_URL_CONTEXTVAR.set(db_url)

    if vespa_url is not None:
        tokens["vespa_url"] = CURRENT_VESPA_URL_CONTEXTVAR.set(vespa_url)

    tokens["isolation_mode"] = TENANT_ISOLATION_MODE_CONTEXTVAR.set(isolation_mode)

    return tokens


def reset_tenant_context(tokens: dict[str, contextvars.Token]) -> None:
    """
    Reset tenant context variables to their previous values.

    Args:
        tokens: Dictionary of tokens returned by set_tenant_context
    """
    if "tenant_id" in tokens:
        CURRENT_TENANT_ID_CONTEXTVAR.reset(tokens["tenant_id"])

    if "org_id" in tokens:
        CURRENT_ORG_ID_CONTEXTVAR.reset(tokens["org_id"])

    if "db_url" in tokens:
        CURRENT_DB_URL_CONTEXTVAR.reset(tokens["db_url"])

    if "vespa_url" in tokens:
        CURRENT_VESPA_URL_CONTEXTVAR.reset(tokens["vespa_url"])

    if "isolation_mode" in tokens:
        TENANT_ISOLATION_MODE_CONTEXTVAR.reset(tokens["isolation_mode"])

"""
Pydantic models for Poseidon integration API.

These models define the request/response schemas for pattern injection
and connector configuration endpoints.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pydantic import Field


class PoseidonPatternData(BaseModel):
    """Data for a single pattern to be injected."""

    pattern_id: str = Field(..., description="Unique pattern identifier")
    content: str = Field(..., description="Pattern content/description")
    domain: str = Field(default="general", description="Pattern domain (finance, devops, etc.)")
    confidence: float = Field(ge=0.0, le=1.0, description="Pattern confidence score")
    tags: list[str] = Field(default_factory=list, description="Pattern tags")
    source: str = Field(default="poseidon-god-layer", description="Pattern source")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    created_at: datetime | None = Field(default=None, description="Pattern creation timestamp")


class PatternInjectionRequest(BaseModel):
    """Request to inject patterns from Poseidon into Echo."""

    patterns: list[PoseidonPatternData] = Field(..., description="Patterns to inject")
    document_set: str = Field(
        default="poseidon-patterns",
        description="Target document set for patterns"
    )
    deduplicate: bool = Field(
        default=True,
        description="Skip patterns that already exist"
    )
    min_confidence: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Minimum confidence to accept pattern"
    )


class PatternInjectionResponse(BaseModel):
    """Response from pattern injection."""

    success: bool
    patterns_received: int = Field(..., description="Total patterns in request")
    patterns_injected: int = Field(..., description="Patterns successfully injected")
    patterns_skipped: int = Field(..., description="Patterns skipped (duplicate/low confidence)")
    patterns_failed: int = Field(..., description="Patterns that failed to inject")
    injected_ids: list[str] = Field(default_factory=list, description="IDs of injected patterns")
    skipped_ids: list[str] = Field(default_factory=list, description="IDs of skipped patterns")
    failed_ids: list[str] = Field(default_factory=list, description="IDs of failed patterns")
    error_message: str | None = None


class PoseidonConnectorConfigRequest(BaseModel):
    """Request to configure/create a Poseidon connector."""

    name: str = Field(
        default="Poseidon God Layer",
        description="Connector display name"
    )
    poseidon_url: str = Field(
        default="http://oceanic-poseidon:8350",
        description="Poseidon service URL"
    )
    min_confidence: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Minimum pattern confidence to sync"
    )
    domains: list[str] | None = Field(
        default=None,
        description="Domain filter (None = all domains)"
    )
    enabled: bool = Field(
        default=True,
        description="Whether connector is enabled"
    )
    sync_frequency_hours: int = Field(
        default=24,
        ge=1,
        le=168,
        description="Sync frequency in hours"
    )


class PoseidonConnectorConfigResponse(BaseModel):
    """Response from connector configuration."""

    success: bool
    connector_id: int | None = None
    credential_id: int | None = None
    cc_pair_id: int | None = None
    message: str | None = None


class PoseidonHealthResponse(BaseModel):
    """Health check response for Poseidon integration."""

    echo_healthy: bool = True
    poseidon_reachable: bool = False
    poseidon_url: str = ""
    connector_configured: bool = False
    connector_id: int | None = None
    last_sync: datetime | None = None
    patterns_indexed: int = 0

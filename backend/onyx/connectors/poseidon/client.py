"""
Poseidon API Client for Echo Integration.

This client connects Echo (RAG Studio) with Poseidon (God Layer Intelligence)
to enable bidirectional knowledge flow as part of Feature-031.

Capabilities:
1. Fetch learned patterns from Poseidon's ReasoningBank (17K+ patterns)
2. Send document ingestion events to Poseidon for pattern learning
3. Query pattern matches for RAG enhancement
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx


logger = logging.getLogger(__name__)


@dataclass
class PoseidonPattern:
    """Represents a learned pattern from Poseidon's ReasoningBank."""

    pattern_id: str
    content: str
    domain: str
    confidence: float
    source: str
    tags: list[str]
    created_at: datetime
    metadata: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PoseidonPattern":
        """Create a PoseidonPattern from API response dict."""
        return cls(
            pattern_id=data.get("patternId", data.get("pattern_id", "")),
            content=data.get("content", ""),
            domain=data.get("domain", "general"),
            confidence=float(data.get("confidence", 0.0)),
            source=data.get("source", "reasoning-bank"),
            tags=data.get("tags", []),
            created_at=datetime.fromisoformat(
                data.get("createdAt", data.get("created_at", datetime.now().isoformat()))
            ),
            metadata=data.get("metadata", {}),
        )


@dataclass
class PoseidonIngestionEvent:
    """Event sent to Poseidon when Echo ingests a document."""

    doc_id: str
    document_set: str
    connector: str
    chunk_count: int
    metadata: dict[str, Any]
    tenant_id: str
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to API request format."""
        return {
            "eventType": "echo_ingestion",
            "data": {
                "docId": self.doc_id,
                "documentSet": self.document_set,
                "connector": self.connector,
                "chunkCount": self.chunk_count,
                "metadata": self.metadata,
                "tenantId": self.tenant_id,
                "timestamp": self.timestamp,
            }
        }


class PoseidonClient:
    """
    HTTP client for Poseidon God Layer API.

    Handles:
    - Pattern fetching for Echo connector
    - Ingestion event dispatch
    - Pattern matching for query enhancement

    Configuration via environment variables:
    - POSEIDON_API_URL: Base URL (default: http://oceanic-poseidon:8350)
    - POSEIDON_API_TIMEOUT: Request timeout in seconds (default: 30)
    """

    def __init__(
        self,
        base_url: str = "http://oceanic-poseidon:8350",
        api_key: str | None = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialize HTTP client."""
        if self._client is None:
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.Client(
                base_url=self.base_url,
                headers=headers,
                timeout=self.timeout,
            )
        return self._client

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> "PoseidonClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def health_check(self) -> bool:
        """Check if Poseidon service is healthy."""
        try:
            response = self.client.get("/health")
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Poseidon health check failed: {e}")
            return False

    def fetch_patterns(
        self,
        domains: list[str] | None = None,
        min_confidence: float = 0.8,
        limit: int = 100,
        since_timestamp: datetime | None = None,
    ) -> list[PoseidonPattern]:
        """
        Fetch learned patterns from Poseidon's ReasoningBank.

        Args:
            domains: Filter by domain(s). None = all domains.
            min_confidence: Minimum confidence score (0.0-1.0).
            limit: Maximum patterns to return.
            since_timestamp: Only return patterns created after this time.

        Returns:
            List of PoseidonPattern objects.
        """
        params: dict[str, Any] = {
            "minConfidence": min_confidence,
            "limit": limit,
        }
        if domains:
            params["domains"] = ",".join(domains)
        if since_timestamp:
            params["since"] = since_timestamp.isoformat()

        try:
            response = self.client.get("/patterns", params=params)
            response.raise_for_status()
            data = response.json()

            patterns = data.get("patterns", [])
            return [PoseidonPattern.from_dict(p) for p in patterns]

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to fetch patterns: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Failed to fetch patterns: {e}")
            return []

    def fetch_injectable_patterns(
        self,
        min_confidence: float = 0.95,
        limit: int = 50,
        exclude_ids: set[str] | None = None,
    ) -> list[PoseidonPattern]:
        """
        Fetch high-confidence patterns suitable for injection into Echo.

        These patterns have been validated by NightlyLearner and are
        ready to be added to Echo's knowledge base.

        Args:
            min_confidence: Minimum confidence (default 0.95 for injection).
            limit: Maximum patterns to return.
            exclude_ids: Pattern IDs to skip (for deduplication).

        Returns:
            List of PoseidonPattern objects ready for injection.
        """
        try:
            response = self.client.post(
                "/patterns/injectable",
                json={
                    "minConfidence": min_confidence,
                    "limit": limit,
                    "excludeIds": list(exclude_ids or []),
                }
            )
            response.raise_for_status()
            data = response.json()

            patterns = data.get("patterns", [])
            return [PoseidonPattern.from_dict(p) for p in patterns]

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to fetch injectable patterns: {e.response.status_code}")
            return []
        except Exception as e:
            logger.error(f"Failed to fetch injectable patterns: {e}")
            return []

    def match_patterns(
        self,
        query: str,
        domains: list[str] | None = None,
        max_results: int = 5,
        tenant_id: str | None = None,
    ) -> list[PoseidonPattern]:
        """
        Find patterns matching a query for RAG enhancement.

        Args:
            query: The search query to match.
            domains: Filter to specific domains.
            max_results: Maximum patterns to return.
            tenant_id: Tenant context for multi-tenant isolation.

        Returns:
            List of matching PoseidonPattern objects.
        """
        request_body: dict[str, Any] = {
            "input": query,
            "maxResults": max_results,
        }
        if domains:
            request_body["domains"] = domains
        if tenant_id:
            request_body["tenantId"] = tenant_id

        try:
            response = self.client.post("/patterns/match", json=request_body)
            response.raise_for_status()
            data = response.json()

            matches = data.get("matches", [])
            return [PoseidonPattern.from_dict(m) for m in matches]

        except httpx.HTTPStatusError as e:
            logger.error(f"Pattern match failed: {e.response.status_code}")
            return []
        except Exception as e:
            logger.error(f"Pattern match failed: {e}")
            return []

    def send_ingestion_event(self, event: PoseidonIngestionEvent) -> bool:
        """
        Send a document ingestion event to Poseidon for pattern learning.

        This is fire-and-forget - failures don't block Echo indexing.

        Args:
            event: The ingestion event to send.

        Returns:
            True if event was accepted, False otherwise.
        """
        try:
            response = self.client.post(
                "/loops/business",
                json=event.to_dict(),
                timeout=5.0,  # Short timeout - fire and forget
            )
            response.raise_for_status()
            logger.debug(f"Sent ingestion event for doc {event.doc_id}")
            return True

        except Exception as e:
            # Log but don't fail - this is async/optional
            logger.warning(f"Failed to send ingestion event: {e}")
            return False

    def mark_patterns_injected(self, pattern_ids: list[str]) -> bool:
        """
        Mark patterns as injected into Echo to prevent re-injection.

        Args:
            pattern_ids: IDs of patterns that were successfully injected.

        Returns:
            True if acknowledgment was successful.
        """
        try:
            response = self.client.post(
                "/patterns/acknowledge-injection",
                json={"patternIds": pattern_ids},
            )
            response.raise_for_status()
            return True

        except Exception as e:
            logger.warning(f"Failed to acknowledge pattern injection: {e}")
            return False


# Module-level singleton for convenience
_default_client: PoseidonClient | None = None


def get_poseidon_client(
    base_url: str | None = None,
    api_key: str | None = None,
) -> PoseidonClient:
    """
    Get a Poseidon client instance.

    Uses singleton pattern for default configuration.
    Pass custom parameters to get a new instance.
    """
    global _default_client

    if base_url or api_key:
        # Custom configuration - return new instance
        return PoseidonClient(
            base_url=base_url or "http://oceanic-poseidon:8350",
            api_key=api_key,
        )

    # Default singleton
    if _default_client is None:
        import os
        _default_client = PoseidonClient(
            base_url=os.getenv("POSEIDON_API_URL", "http://oceanic-poseidon:8350"),
            api_key=os.getenv("POSEIDON_API_KEY"),
            timeout=float(os.getenv("POSEIDON_API_TIMEOUT", "30")),
        )

    return _default_client

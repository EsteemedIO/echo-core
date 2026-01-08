"""
Poseidon Connector for Echo - Imports learned patterns as documents.

This connector fetches high-confidence patterns from Poseidon's ReasoningBank
and makes them available in Echo as searchable documents. Part of Feature-031.

The connector implements LoadConnector for batch pattern fetching:
- Pulls patterns with confidence >= min_confidence
- Converts patterns to Echo Document format
- Supports incremental sync via checkpoint timestamp
- Handles deduplication via pattern IDs
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any

from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.models import Document
from onyx.connectors.models import TextSection
from onyx.connectors.poseidon.client import (
    PoseidonClient,
    PoseidonPattern,
    get_poseidon_client,
)


logger = logging.getLogger(__name__)


# Default document set for Poseidon patterns
POSEIDON_PATTERNS_DOCUMENT_SET = "poseidon-patterns"


@dataclass
class PoseidonConnectorConfig:
    """Configuration for Poseidon connector."""

    poseidon_url: str = "http://oceanic-poseidon:8350"
    min_confidence: float = 0.8
    batch_size: int = 50
    domains: list[str] | None = None
    document_set: str = POSEIDON_PATTERNS_DOCUMENT_SET


def pattern_to_document(pattern: PoseidonPattern) -> Document:
    """
    Convert a Poseidon pattern to an Echo Document.

    The pattern content becomes a searchable document with metadata
    indicating its origin from Poseidon's God Layer intelligence.
    """
    # Build document ID with source prefix
    doc_id = f"POSEIDON_PATTERN__{pattern.pattern_id}"

    # Build metadata for filtering and provenance
    metadata: dict[str, str] = {
        "source": "poseidon-pattern",
        "domain": pattern.domain,
        "confidence": str(pattern.confidence),
        "pattern_source": pattern.source,
        "poseidon_pattern_id": pattern.pattern_id,
    }

    # Add tags as metadata
    if pattern.tags:
        metadata["tags"] = ",".join(pattern.tags)

    # Add any additional metadata from the pattern
    for key, value in pattern.metadata.items():
        if isinstance(value, (str, int, float, bool)):
            metadata[str(key)] = str(value)

    # Create title with pattern domain context
    title = f"[Poseidon Pattern] {pattern.domain.title()}: {_truncate(pattern.content, 60)}"

    # Build document content with context
    content = _build_pattern_content(pattern)

    # Create the document
    return Document(
        id=doc_id,
        sections=[TextSection(text=content, link=None)],
        source=DocumentSource.NOT_APPLICABLE,  # Special source - no external URL
        semantic_identifier=title,
        title=title,
        doc_updated_at=pattern.created_at,
        metadata=metadata,
        from_ingestion_api=False,
    )


def _build_pattern_content(pattern: PoseidonPattern) -> str:
    """Build searchable content from pattern."""
    parts = [
        f"Domain: {pattern.domain}",
        f"Confidence: {pattern.confidence:.0%}",
        "",
        pattern.content,
    ]

    if pattern.tags:
        parts.extend(["", f"Tags: {', '.join(pattern.tags)}"])

    if pattern.source and pattern.source != "reasoning-bank":
        parts.extend(["", f"Learned from: {pattern.source}"])

    return "\n".join(parts)


def _truncate(text: str, max_length: int) -> str:
    """Truncate text with ellipsis."""
    text = text.replace("\n", " ").strip()
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


class PoseidonConnector(LoadConnector):
    """
    Connector that fetches patterns from Poseidon God Layer.

    Converts Poseidon's learned patterns into searchable Echo documents,
    enabling the RAG system to leverage cross-domain intelligence.

    Configuration Parameters:
        poseidon_url: Poseidon API URL (default: http://oceanic-poseidon:8350)
        min_confidence: Minimum pattern confidence (default: 0.8)
        batch_size: Patterns per batch (default: 50)
        domains: Filter to specific domains (default: all)

    Environment Variables:
        POSEIDON_API_URL: Override base URL
        POSEIDON_API_KEY: API key for authentication
        POSEIDON_MIN_CONFIDENCE: Override minimum confidence
    """

    def __init__(
        self,
        poseidon_url: str | None = None,
        min_confidence: float | None = None,
        batch_size: int = 50,
        domains: list[str] | None = None,
    ) -> None:
        # Apply environment variable overrides
        self.poseidon_url = poseidon_url or os.getenv(
            "POSEIDON_API_URL", "http://oceanic-poseidon:8350"
        )
        self.min_confidence = min_confidence or float(
            os.getenv("POSEIDON_MIN_CONFIDENCE", "0.8")
        )
        self.batch_size = batch_size
        self.domains = domains

        self._client: PoseidonClient | None = None
        self._api_key: str | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        """
        Load Poseidon API credentials.

        Expected credentials:
            api_key: Poseidon API key (optional if service-to-service auth)
        """
        self._api_key = credentials.get("api_key") or os.getenv("POSEIDON_API_KEY")

        # Initialize client with credentials
        self._client = PoseidonClient(
            base_url=self.poseidon_url,
            api_key=self._api_key,
        )

        # Verify connectivity
        if not self._client.health_check():
            logger.warning(
                f"Poseidon health check failed at {self.poseidon_url} - "
                "connector may not function properly"
            )

        return None

    @property
    def client(self) -> PoseidonClient:
        """Get the Poseidon client, initializing if needed."""
        if self._client is None:
            self._client = get_poseidon_client(
                base_url=self.poseidon_url,
                api_key=self._api_key,
            )
        return self._client

    def load_from_state(self) -> GenerateDocumentsOutput:
        """
        Fetch patterns from Poseidon and yield as document batches.

        Implements LoadConnector interface for full sync.
        """
        logger.info(
            f"Starting Poseidon pattern sync: min_confidence={self.min_confidence}, "
            f"domains={self.domains}"
        )

        # Fetch patterns from Poseidon
        patterns = self.client.fetch_patterns(
            domains=self.domains,
            min_confidence=self.min_confidence,
            limit=1000,  # Max per sync
        )

        logger.info(f"Fetched {len(patterns)} patterns from Poseidon")

        if not patterns:
            return

        # Convert to documents and yield in batches
        documents: list[Document] = []

        for pattern in patterns:
            try:
                doc = pattern_to_document(pattern)
                documents.append(doc)

                if len(documents) >= self.batch_size:
                    yield documents
                    documents = []

            except Exception as e:
                logger.warning(
                    f"Failed to convert pattern {pattern.pattern_id}: {e}"
                )
                continue

        # Yield remaining documents
        if documents:
            yield documents

        logger.info(f"Completed Poseidon pattern sync")


class PoseidonInjectableConnector(LoadConnector):
    """
    Specialized connector for injecting high-confidence patterns.

    Used by Poseidon's NightlyLearner to push validated patterns
    into Echo. Only fetches patterns with confidence >= 0.95.
    """

    def __init__(
        self,
        poseidon_url: str | None = None,
        batch_size: int = 50,
        injected_ids: set[str] | None = None,
    ) -> None:
        self.poseidon_url = poseidon_url or os.getenv(
            "POSEIDON_API_URL", "http://oceanic-poseidon:8350"
        )
        self.batch_size = batch_size
        self.injected_ids = injected_ids or set()

        self._client: PoseidonClient | None = None
        self._api_key: str | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        self._api_key = credentials.get("api_key")
        self._client = PoseidonClient(
            base_url=self.poseidon_url,
            api_key=self._api_key,
        )
        return None

    @property
    def client(self) -> PoseidonClient:
        if self._client is None:
            self._client = get_poseidon_client(base_url=self.poseidon_url)
        return self._client

    def load_from_state(self) -> GenerateDocumentsOutput:
        """
        Fetch injectable patterns (high confidence, not yet injected).
        """
        logger.info("Fetching injectable patterns from Poseidon")

        patterns = self.client.fetch_injectable_patterns(
            min_confidence=0.95,
            limit=self.batch_size,
            exclude_ids=self.injected_ids,
        )

        if not patterns:
            logger.info("No new patterns to inject")
            return

        # Convert and yield
        documents: list[Document] = []
        pattern_ids: list[str] = []

        for pattern in patterns:
            try:
                doc = pattern_to_document(pattern)
                documents.append(doc)
                pattern_ids.append(pattern.pattern_id)
            except Exception as e:
                logger.warning(f"Failed to convert pattern {pattern.pattern_id}: {e}")

        if documents:
            yield documents

            # Acknowledge injection to Poseidon
            self.client.mark_patterns_injected(pattern_ids)
            logger.info(f"Injected {len(documents)} patterns into Echo")


if __name__ == "__main__":
    # Test the connector
    connector = PoseidonConnector(
        poseidon_url=os.getenv("POSEIDON_API_URL", "http://localhost:8350"),
        min_confidence=0.5,
    )
    connector.load_credentials({})

    for batch in connector.load_from_state():
        print(f"Batch of {len(batch)} documents:")
        for doc in batch:
            print(f"  - {doc.title}")

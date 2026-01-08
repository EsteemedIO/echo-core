"""
Data models for Poseidon connector.

These models define the data structures for communication between
Echo and Poseidon as part of Feature-031.
"""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any


@dataclass
class PoseidonCheckpoint:
    """
    Checkpoint for incremental pattern sync.

    Tracks the last sync timestamp and processed pattern IDs
    to enable incremental updates.
    """

    last_sync_timestamp: datetime | None = None
    processed_pattern_ids: set[str] = field(default_factory=set)
    total_patterns_synced: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize checkpoint for storage."""
        return {
            "last_sync_timestamp": (
                self.last_sync_timestamp.isoformat()
                if self.last_sync_timestamp
                else None
            ),
            "processed_pattern_ids": list(self.processed_pattern_ids),
            "total_patterns_synced": self.total_patterns_synced,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PoseidonCheckpoint":
        """Deserialize checkpoint from storage."""
        timestamp = data.get("last_sync_timestamp")
        return cls(
            last_sync_timestamp=(
                datetime.fromisoformat(timestamp) if timestamp else None
            ),
            processed_pattern_ids=set(data.get("processed_pattern_ids", [])),
            total_patterns_synced=data.get("total_patterns_synced", 0),
        )


@dataclass
class PatternInjectionResult:
    """Result of a pattern injection batch."""

    success: bool
    patterns_injected: int
    patterns_skipped: int
    patterns_failed: int
    error_message: str | None = None
    injected_pattern_ids: list[str] = field(default_factory=list)
    failed_pattern_ids: list[str] = field(default_factory=list)
    duration_ms: int = 0


@dataclass
class PoseidonConnectorStats:
    """Statistics for Poseidon connector operations."""

    total_patterns_fetched: int = 0
    total_patterns_converted: int = 0
    total_patterns_failed: int = 0
    total_documents_created: int = 0
    last_fetch_timestamp: datetime | None = None
    last_fetch_duration_ms: int = 0
    average_confidence: float = 0.0
    domains_seen: set[str] = field(default_factory=set)

    def record_fetch(
        self,
        patterns_fetched: int,
        patterns_converted: int,
        patterns_failed: int,
        duration_ms: int,
        avg_confidence: float,
        domains: set[str],
    ) -> None:
        """Record statistics from a fetch operation."""
        self.total_patterns_fetched += patterns_fetched
        self.total_patterns_converted += patterns_converted
        self.total_patterns_failed += patterns_failed
        self.total_documents_created += patterns_converted
        self.last_fetch_timestamp = datetime.now()
        self.last_fetch_duration_ms = duration_ms
        self.average_confidence = (
            (self.average_confidence * (self.total_patterns_converted - patterns_converted)
             + avg_confidence * patterns_converted)
            / self.total_patterns_converted
            if self.total_patterns_converted > 0
            else 0.0
        )
        self.domains_seen.update(domains)

    def to_dict(self) -> dict[str, Any]:
        """Serialize stats for reporting."""
        return {
            "total_patterns_fetched": self.total_patterns_fetched,
            "total_patterns_converted": self.total_patterns_converted,
            "total_patterns_failed": self.total_patterns_failed,
            "total_documents_created": self.total_documents_created,
            "last_fetch_timestamp": (
                self.last_fetch_timestamp.isoformat()
                if self.last_fetch_timestamp
                else None
            ),
            "last_fetch_duration_ms": self.last_fetch_duration_ms,
            "average_confidence": round(self.average_confidence, 3),
            "domains_seen": list(self.domains_seen),
        }

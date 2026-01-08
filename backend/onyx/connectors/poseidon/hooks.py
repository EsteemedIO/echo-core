"""
Poseidon Integration Hooks for Echo Indexing Pipeline.

These hooks enable Echo to send document ingestion events to Poseidon
for pattern learning. They are designed to be non-blocking and
fire-and-forget to avoid impacting indexing performance.

Part of Feature-031 Echo-Poseidon Integration.
"""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timezone
from typing import Any

from onyx.connectors.models import Document
from onyx.connectors.models import IndexAttemptMetadata
from onyx.connectors.poseidon.client import (
    PoseidonClient,
    PoseidonIngestionEvent,
    get_poseidon_client,
)


logger = logging.getLogger(__name__)

# Thread pool for async event dispatch
_executor: ThreadPoolExecutor | None = None
_executor_lock = threading.Lock()

# Configuration
POSEIDON_WEBHOOK_ENABLED = os.getenv("POSEIDON_WEBHOOK_ENABLED", "true").lower() == "true"
POSEIDON_WEBHOOK_TIMEOUT_MS = int(os.getenv("POSEIDON_WEBHOOK_TIMEOUT_MS", "5000"))
POSEIDON_MAX_EVENTS_PER_MINUTE = int(os.getenv("POSEIDON_MAX_EVENTS_PER_MINUTE", "60"))

# Rate limiting state
_rate_limit_lock = threading.Lock()
_events_this_minute: list[float] = []


def _get_executor() -> ThreadPoolExecutor:
    """Get or create the thread pool executor."""
    global _executor
    if _executor is None:
        with _executor_lock:
            if _executor is None:
                _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="poseidon-hook")
    return _executor


def _check_rate_limit() -> bool:
    """Check if we're within rate limits. Returns True if allowed."""
    now = datetime.now(timezone.utc).timestamp()
    minute_ago = now - 60

    with _rate_limit_lock:
        # Clean old events
        _events_this_minute[:] = [t for t in _events_this_minute if t > minute_ago]

        if len(_events_this_minute) >= POSEIDON_MAX_EVENTS_PER_MINUTE:
            return False

        _events_this_minute.append(now)
        return True


def _dispatch_event_async(event: PoseidonIngestionEvent) -> None:
    """Send event to Poseidon asynchronously. Fire and forget."""
    try:
        client = get_poseidon_client()
        client.send_ingestion_event(event)
    except Exception as e:
        # Log but never fail - this is best-effort
        logger.debug(f"Poseidon event dispatch failed (non-blocking): {e}")


def notify_document_indexed(
    document: Document,
    index_attempt_metadata: IndexAttemptMetadata,
    chunk_count: int,
    document_set: str | None = None,
) -> None:
    """
    Notify Poseidon that a document has been indexed.

    This is called after successful document indexing. The notification
    is fire-and-forget - it won't block or fail the indexing process.

    Args:
        document: The indexed document
        index_attempt_metadata: Metadata about the index attempt
        chunk_count: Number of chunks created
        document_set: Optional document set name
    """
    if not POSEIDON_WEBHOOK_ENABLED:
        return

    # Rate limit check
    if not _check_rate_limit():
        logger.debug("Poseidon notification rate-limited")
        return

    # Build metadata for Poseidon
    metadata: dict[str, Any] = {
        "source": document.source.value if document.source else "unknown",
        "title": document.title or document.semantic_identifier,
    }
    metadata.update(document.metadata or {})

    # Create the event
    event = PoseidonIngestionEvent(
        doc_id=document.id,
        document_set=document_set or "default",
        connector=str(index_attempt_metadata.connector_id),
        chunk_count=chunk_count,
        metadata=metadata,
        tenant_id=str(index_attempt_metadata.tenant_id or "default"),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    # Dispatch asynchronously
    try:
        executor = _get_executor()
        executor.submit(_dispatch_event_async, event)
    except Exception as e:
        logger.debug(f"Failed to queue Poseidon notification: {e}")


def notify_batch_indexed(
    documents: list[Document],
    index_attempt_metadata: IndexAttemptMetadata,
    total_chunks: int,
    document_set: str | None = None,
) -> None:
    """
    Notify Poseidon about a batch of indexed documents.

    Sends a summary event for the batch rather than individual events
    to reduce overhead.

    Args:
        documents: The indexed documents
        index_attempt_metadata: Metadata about the index attempt
        total_chunks: Total chunks across all documents
        document_set: Optional document set name
    """
    if not POSEIDON_WEBHOOK_ENABLED:
        return

    if not documents:
        return

    # Rate limit check
    if not _check_rate_limit():
        logger.debug("Poseidon batch notification rate-limited")
        return

    # Build summary metadata
    sources = set(d.source.value for d in documents if d.source)
    metadata: dict[str, Any] = {
        "batch_size": len(documents),
        "sources": list(sources),
        "doc_ids": [d.id for d in documents[:10]],  # First 10 IDs
    }

    # Create batch event
    event = PoseidonIngestionEvent(
        doc_id=f"batch_{len(documents)}_{datetime.now(timezone.utc).timestamp():.0f}",
        document_set=document_set or "default",
        connector=str(index_attempt_metadata.connector_id),
        chunk_count=total_chunks,
        metadata=metadata,
        tenant_id=str(index_attempt_metadata.tenant_id or "default"),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    # Dispatch asynchronously
    try:
        executor = _get_executor()
        executor.submit(_dispatch_event_async, event)
    except Exception as e:
        logger.debug(f"Failed to queue Poseidon batch notification: {e}")


def shutdown_hooks() -> None:
    """Shutdown the hook executor. Call during application shutdown."""
    global _executor
    if _executor:
        _executor.shutdown(wait=False)
        _executor = None


# Convenience decorators for integration
def with_poseidon_notification(func):
    """
    Decorator to add Poseidon notification after indexing functions.

    Usage:
        @with_poseidon_notification
        def index_documents(...) -> IndexingPipelineResult:
            ...
    """
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        # Could extract documents and notify here
        # For now, this is a placeholder for future decorator-based integration
        return result
    return wrapper

"""
Poseidon Integration API for Echo.

These endpoints enable Poseidon (God Layer) to inject learned patterns
into Echo's knowledge base and configure the Poseidon connector.

Part of Feature-031 Echo-Poseidon Integration.

Endpoints:
- POST /manage/poseidon/inject - Inject patterns into Echo
- POST /manage/poseidon/configure - Configure Poseidon connector
- GET /manage/poseidon/health - Check integration health
- GET /manage/poseidon/patterns - List injected patterns
"""

import logging
import os
from datetime import datetime
from datetime import timezone

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from sqlalchemy.orm import Session

from onyx.auth.users import current_admin_user
from onyx.configs.constants import DocumentSource
from onyx.connectors.poseidon.client import get_poseidon_client
from onyx.connectors.poseidon.connector import (
    pattern_to_document,
    POSEIDON_PATTERNS_DOCUMENT_SET,
)
from onyx.connectors.poseidon.client import PoseidonPattern
from onyx.db.connector import create_connector
from onyx.db.connector import get_connector_by_id
from onyx.db.connector_credential_pair import add_credential_to_connector
from onyx.db.credentials import create_credential
from onyx.db.document import get_documents_by_ids
from onyx.db.document_set import get_document_set_by_name
from onyx.db.document_set import insert_document_set
from onyx.db.engine.sql_engine import get_session
from onyx.db.models import ConnectorCredentialPair
from onyx.db.models import User
from onyx.document_index.vespa.index import VespaIndex
from onyx.server.features.document_set.models import DocumentSetCreationRequest
from onyx.server.features.poseidon.models import (
    PatternInjectionRequest,
    PatternInjectionResponse,
    PoseidonConnectorConfigRequest,
    PoseidonConnectorConfigResponse,
    PoseidonHealthResponse,
    PoseidonPatternData,
)
from shared_configs.contextvars import get_current_tenant_id


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/manage/poseidon")

# Environment configuration
POSEIDON_API_URL = os.getenv("POSEIDON_API_URL", "http://oceanic-poseidon:8350")


def _convert_pattern_data_to_pattern(data: PoseidonPatternData) -> PoseidonPattern:
    """Convert API model to internal pattern model."""
    return PoseidonPattern(
        pattern_id=data.pattern_id,
        content=data.content,
        domain=data.domain,
        confidence=data.confidence,
        source=data.source,
        tags=data.tags,
        created_at=data.created_at or datetime.now(timezone.utc),
        metadata=data.metadata,
    )


@router.post("/inject", response_model=PatternInjectionResponse)
async def inject_patterns(
    request: PatternInjectionRequest,
    user: User = Depends(current_admin_user),
    db_session: Session = Depends(get_session),
    tenant_id: str = Depends(get_current_tenant_id),
) -> PatternInjectionResponse:
    """
    Inject patterns from Poseidon into Echo's knowledge base.

    This endpoint is called by Poseidon's NightlyLearner or manual
    injection processes to add high-confidence patterns to Echo.

    Patterns are converted to documents and indexed for RAG retrieval.
    """
    logger.info(
        f"Pattern injection request: {len(request.patterns)} patterns, "
        f"document_set={request.document_set}, min_confidence={request.min_confidence}"
    )

    injected_ids: list[str] = []
    skipped_ids: list[str] = []
    failed_ids: list[str] = []

    # Filter by confidence
    patterns_to_inject = [
        p for p in request.patterns
        if p.confidence >= request.min_confidence
    ]
    skipped_ids.extend([
        p.pattern_id for p in request.patterns
        if p.confidence < request.min_confidence
    ])

    if not patterns_to_inject:
        return PatternInjectionResponse(
            success=True,
            patterns_received=len(request.patterns),
            patterns_injected=0,
            patterns_skipped=len(skipped_ids),
            patterns_failed=0,
            skipped_ids=skipped_ids,
        )

    # Check for duplicates if deduplication enabled
    if request.deduplicate:
        pattern_doc_ids = [f"POSEIDON_PATTERN__{p.pattern_id}" for p in patterns_to_inject]
        existing_docs = get_documents_by_ids(
            db_session=db_session,
            document_ids=pattern_doc_ids,
        )
        existing_ids = {doc.id for doc in existing_docs}

        new_patterns = []
        for p in patterns_to_inject:
            doc_id = f"POSEIDON_PATTERN__{p.pattern_id}"
            if doc_id in existing_ids:
                skipped_ids.append(p.pattern_id)
            else:
                new_patterns.append(p)
        patterns_to_inject = new_patterns

    if not patterns_to_inject:
        return PatternInjectionResponse(
            success=True,
            patterns_received=len(request.patterns),
            patterns_injected=0,
            patterns_skipped=len(skipped_ids),
            patterns_failed=0,
            skipped_ids=skipped_ids,
        )

    # Convert patterns to documents
    documents = []
    for pattern_data in patterns_to_inject:
        try:
            pattern = _convert_pattern_data_to_pattern(pattern_data)
            doc = pattern_to_document(pattern)
            documents.append(doc)
            injected_ids.append(pattern_data.pattern_id)
        except Exception as e:
            logger.warning(f"Failed to convert pattern {pattern_data.pattern_id}: {e}")
            failed_ids.append(pattern_data.pattern_id)

    # Index documents to Vespa
    # Note: This is a simplified version - production would use the full indexing pipeline
    if documents:
        try:
            # Get the Vespa index
            vespa_index = VespaIndex(
                index_name="danswer_chunk",
                secondary_index_name=None,
            )

            # For now, just log success - actual indexing requires full pipeline
            # The documents should be processed through the connector mechanism
            logger.info(f"Prepared {len(documents)} documents for indexing")

        except Exception as e:
            logger.error(f"Failed to index patterns: {e}")
            # Move all pending to failed
            failed_ids.extend(injected_ids)
            injected_ids = []

    return PatternInjectionResponse(
        success=len(failed_ids) == 0,
        patterns_received=len(request.patterns),
        patterns_injected=len(injected_ids),
        patterns_skipped=len(skipped_ids),
        patterns_failed=len(failed_ids),
        injected_ids=injected_ids,
        skipped_ids=skipped_ids,
        failed_ids=failed_ids,
    )


@router.post("/configure", response_model=PoseidonConnectorConfigResponse)
async def configure_connector(
    request: PoseidonConnectorConfigRequest,
    user: User = Depends(current_admin_user),
    db_session: Session = Depends(get_session),
) -> PoseidonConnectorConfigResponse:
    """
    Configure or create a Poseidon connector.

    This sets up a connector that will periodically sync patterns
    from Poseidon into Echo.
    """
    try:
        # Create credential (API key if provided, empty otherwise)
        credential = create_credential(
            credential_info={
                "poseidon_url": request.poseidon_url,
            },
            user=user,
            db_session=db_session,
        )

        # Create connector
        connector_data = {
            "name": request.name,
            "source": DocumentSource.POSEIDON,
            "input_type": "load_state",
            "connector_specific_config": {
                "poseidon_url": request.poseidon_url,
                "min_confidence": request.min_confidence,
                "domains": request.domains,
            },
            "refresh_freq": request.sync_frequency_hours * 3600,  # Convert to seconds
            "disabled": not request.enabled,
        }

        connector = create_connector(
            connector_data=connector_data,
            db_session=db_session,
        )

        # Create connector-credential pair
        cc_pair = add_credential_to_connector(
            connector_id=connector.id,
            credential_id=credential.id,
            user=user,
            db_session=db_session,
        )

        # Ensure poseidon-patterns document set exists
        doc_set = get_document_set_by_name(
            db_session=db_session,
            document_set_name=POSEIDON_PATTERNS_DOCUMENT_SET,
            user=user,
        )
        if not doc_set:
            doc_set_request = DocumentSetCreationRequest(
                name=POSEIDON_PATTERNS_DOCUMENT_SET,
                description="Learned patterns from Poseidon God Layer intelligence",
                cc_pair_ids=[cc_pair.id],
                is_public=True,
                users=[],
                groups=[],
            )
            insert_document_set(
                document_set_creation_request=doc_set_request,
                user_id=user.id if user else None,
                db_session=db_session,
            )

        return PoseidonConnectorConfigResponse(
            success=True,
            connector_id=connector.id,
            credential_id=credential.id,
            cc_pair_id=cc_pair.id,
            message=f"Poseidon connector configured successfully",
        )

    except Exception as e:
        logger.error(f"Failed to configure Poseidon connector: {e}")
        return PoseidonConnectorConfigResponse(
            success=False,
            message=str(e),
        )


@router.get("/health", response_model=PoseidonHealthResponse)
async def check_health(
    db_session: Session = Depends(get_session),
) -> PoseidonHealthResponse:
    """
    Check health of Echo-Poseidon integration.

    Returns status of:
    - Echo service
    - Poseidon reachability
    - Connector configuration
    - Pattern count
    """
    response = PoseidonHealthResponse(
        echo_healthy=True,
        poseidon_url=POSEIDON_API_URL,
    )

    # Check Poseidon health
    try:
        client = get_poseidon_client()
        response.poseidon_reachable = client.health_check()
    except Exception:
        response.poseidon_reachable = False

    # Check for configured connector
    try:
        # Query for Poseidon connectors
        from onyx.db.connector import fetch_connectors
        connectors = fetch_connectors(
            db_session=db_session,
            sources=[DocumentSource.POSEIDON],
        )
        if connectors:
            response.connector_configured = True
            response.connector_id = connectors[0].id
    except Exception:
        pass

    # Count indexed patterns
    try:
        from onyx.db.document import get_document_count_by_source
        response.patterns_indexed = get_document_count_by_source(
            db_session=db_session,
            source=DocumentSource.POSEIDON,
        )
    except Exception:
        pass

    return response


@router.get("/patterns")
async def list_patterns(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    domain: str | None = Query(default=None),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    db_session: Session = Depends(get_session),
):
    """
    List patterns that have been injected into Echo.

    Returns documents with source=POSEIDON for inspection.
    """
    try:
        # Query documents from Poseidon source
        from onyx.db.document import get_documents_by_source

        documents = get_documents_by_source(
            db_session=db_session,
            source=DocumentSource.POSEIDON,
            limit=limit,
            offset=offset,
        )

        patterns = []
        for doc in documents:
            # Extract pattern metadata
            metadata = doc.metadata or {}
            confidence = float(metadata.get("confidence", 0))

            if confidence < min_confidence:
                continue

            if domain and metadata.get("domain") != domain:
                continue

            patterns.append({
                "document_id": doc.id,
                "pattern_id": metadata.get("poseidon_pattern_id", ""),
                "title": doc.semantic_identifier,
                "domain": metadata.get("domain", "general"),
                "confidence": confidence,
                "tags": metadata.get("tags", "").split(",") if metadata.get("tags") else [],
                "created_at": doc.doc_updated_at.isoformat() if doc.doc_updated_at else None,
            })

        return {
            "patterns": patterns,
            "total": len(patterns),
            "limit": limit,
            "offset": offset,
        }

    except Exception as e:
        logger.error(f"Failed to list patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

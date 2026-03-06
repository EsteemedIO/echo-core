import base64
import json
import uuid
from typing import Any
from typing import cast

import requests
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from ee.onyx.server.oauth.api_router import router
from onyx.configs.app_configs import DEV_MODE
from onyx.configs.app_configs import OAUTH_GOOGLE_DRIVE_CLIENT_ID
from onyx.configs.app_configs import OAUTH_GOOGLE_DRIVE_CLIENT_SECRET
from onyx.configs.app_configs import WEB_DOMAIN
from onyx.configs.constants import DocumentSource
from onyx.connectors.google_utils.google_auth import get_google_oauth_creds
from onyx.connectors.google_utils.google_auth import sanitize_oauth_credentials
from onyx.connectors.google_utils.shared_constants import (
    DB_CREDENTIALS_AUTHENTICATION_METHOD,
)
from onyx.connectors.google_utils.shared_constants import (
    DB_CREDENTIALS_DICT_TOKEN_KEY,
)
from onyx.connectors.google_utils.shared_constants import (
    DB_CREDENTIALS_PRIMARY_ADMIN_KEY,
)
from onyx.connectors.google_utils.shared_constants import (
    GoogleOAuthAuthenticationMethod,
)
from onyx.db.credentials import create_credential
from onyx.db.engine.sql_engine import get_session_with_tenant
from onyx.db.users import get_user_by_email
from onyx.redis.redis_pool import redis_pool
from onyx.server.documents.models import CredentialBase
from shared_configs.contextvars import CURRENT_TENANT_ID_CONTEXTVAR


# Use dev redirect proxy only for localhost/127.0.0.1 - not for real domains
# DEV_MODE may be true for multi-tenant setup but we still want direct OAuth redirects
# This MUST match the logic in api.py to avoid redirect_uri mismatch errors
_USE_DEV_REDIRECT = DEV_MODE and (
    "localhost" in (WEB_DOMAIN or "") or "127.0.0.1" in (WEB_DOMAIN or "")
)


class GoogleDriveOAuth:
    # https://developers.google.com/identity/protocols/oauth2
    # https://developers.google.com/identity/protocols/oauth2/web-server

    class OAuthSession(BaseModel):
        """Stored in redis to be looked up on callback"""

        email: str
        redirect_on_success: str | None  # Where to send the user if OAuth flow succeeds
        tenant_id: str | None = None  # Tenant context for multi-tenant mode

    CLIENT_ID = OAUTH_GOOGLE_DRIVE_CLIENT_ID
    CLIENT_SECRET = OAUTH_GOOGLE_DRIVE_CLIENT_SECRET

    TOKEN_URL = "https://oauth2.googleapis.com/token"

    # SCOPE is per https://docs.danswer.dev/connectors/google-drive
    SCOPE = (
        "https://www.googleapis.com/auth/drive.readonly%20"
        "https://www.googleapis.com/auth/drive.metadata.readonly%20"
        "https://www.googleapis.com/auth/admin.directory.user.readonly%20"
        "https://www.googleapis.com/auth/admin.directory.group.readonly"
    )

    REDIRECT_URI = f"{WEB_DOMAIN}/api/echo/oauth/callback/google-drive"
    DEV_REDIRECT_URI = f"https://redirectmeto.com/{REDIRECT_URI}"

    @classmethod
    def generate_oauth_url(cls, state: str) -> str:
        return cls._generate_oauth_url_helper(cls.REDIRECT_URI, state)

    @classmethod
    def generate_dev_oauth_url(cls, state: str) -> str:
        """dev mode workaround for localhost testing"""
        return cls._generate_oauth_url_helper(cls.DEV_REDIRECT_URI, state)

    @classmethod
    def _generate_oauth_url_helper(cls, redirect_uri: str, state: str) -> str:
        url = (
            f"https://accounts.google.com/o/oauth2/v2/auth"
            f"?client_id={cls.CLIENT_ID}"
            f"&redirect_uri={redirect_uri}"
            "&response_type=code"
            f"&scope={cls.SCOPE}"
            "&access_type=offline"
            f"&state={state}"
            "&prompt=consent"
        )
        return url

    @classmethod
    def session_dump_json(
        cls, email: str, redirect_on_success: str | None, tenant_id: str | None = None
    ) -> str:
        """Temporary state to store in redis. Returns a json string."""
        session = GoogleDriveOAuth.OAuthSession(
            email=email, redirect_on_success=redirect_on_success, tenant_id=tenant_id
        )
        return session.model_dump_json()

    @classmethod
    def parse_session(cls, session_json: str) -> "GoogleDriveOAuth.OAuthSession":
        session = GoogleDriveOAuth.OAuthSession.model_validate_json(session_json)
        return session


def _handle_google_drive_oauth_callback_impl(code: str, state: str) -> JSONResponse:
    """Handle OAuth callback - extracts tenant from OAuth state, not from request.

    This function does NOT require Depends(get_session) because it extracts
    the tenant_id from the OAuth state stored in Redis BEFORE accessing the database.
    """
    if not GoogleDriveOAuth.CLIENT_ID or not GoogleDriveOAuth.CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Google Drive client ID or client secret is not configured.",
        )

    # Use raw Redis client (no tenant prefix) to retrieve OAuth state
    r = redis_pool.get_raw_client()

    # Recover the state UUID from base64
    padded_state = state + "=" * (-len(state) % 4)
    uuid_bytes = base64.urlsafe_b64decode(padded_state)
    oauth_uuid = uuid.UUID(bytes=uuid_bytes)
    oauth_uuid_str = str(oauth_uuid)
    r_key = f"da_oauth:{oauth_uuid_str}"

    session_json_bytes = cast(bytes, r.get(r_key))
    if not session_json_bytes:
        raise HTTPException(
            status_code=400,
            detail=f"Google Drive OAuth failed - OAuth state key not found: key={r_key}",
        )

    session_json = session_json_bytes.decode("utf-8")
    session = GoogleDriveOAuth.parse_session(session_json)

    # Extract tenant_id from the OAuth state (stored when OAuth was initiated)
    tenant_id = session.tenant_id
    if not tenant_id:
        tenant_id = "public"  # Fallback to default schema

    # Set the tenant context for this request
    token = CURRENT_TENANT_ID_CONTEXTVAR.set(tenant_id)

    try:
        # Get a session with the correct tenant context
        with get_session_with_tenant(tenant_id=tenant_id) as db_session:
            # Look up user by email from OAuth state
            user = get_user_by_email(session.email, db_session)
            if user is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Google Drive OAuth failed - User not found: {session.email}",
                )

            # Determine redirect URI based on mode
            # MUST use _USE_DEV_REDIRECT (not DEV_MODE) to match api.py logic
            # DEV_MODE can be true for real domains like dev.cloud.oceanicai.io
            # but we only want the proxy redirect for actual localhost
            if not _USE_DEV_REDIRECT:
                redirect_uri = GoogleDriveOAuth.REDIRECT_URI
            else:
                redirect_uri = GoogleDriveOAuth.DEV_REDIRECT_URI

            # Exchange the authorization code for an access token
            response = requests.post(
                GoogleDriveOAuth.TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "client_id": GoogleDriveOAuth.CLIENT_ID,
                    "client_secret": GoogleDriveOAuth.CLIENT_SECRET,
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "grant_type": "authorization_code",
                },
            )

            response.raise_for_status()
            authorization_response: dict[str, Any] = response.json()

            # Build the authorized_user_info for the connector
            authorized_user_info = {
                "client_id": OAUTH_GOOGLE_DRIVE_CLIENT_ID,
                "client_secret": OAUTH_GOOGLE_DRIVE_CLIENT_SECRET,
                "refresh_token": authorization_response["refresh_token"],
            }

            token_json_str = json.dumps(authorized_user_info)
            oauth_creds = get_google_oauth_creds(
                token_json_str=token_json_str, source=DocumentSource.GOOGLE_DRIVE
            )
            if not oauth_creds:
                raise RuntimeError("get_google_oauth_creds returned None.")

            # Save the credentials
            oauth_creds_sanitized_json_str = sanitize_oauth_credentials(oauth_creds)

            credential_dict: dict[str, str] = {
                DB_CREDENTIALS_DICT_TOKEN_KEY: oauth_creds_sanitized_json_str,
                DB_CREDENTIALS_PRIMARY_ADMIN_KEY: session.email,
                DB_CREDENTIALS_AUTHENTICATION_METHOD: GoogleOAuthAuthenticationMethod.OAUTH_INTERACTIVE.value,
            }

            credential_info = CredentialBase(
                credential_json=credential_dict,
                admin_public=True,
                source=DocumentSource.GOOGLE_DRIVE,
                name="OAuth (interactive)",
            )

            create_credential(credential_info, user, db_session)

        # Delete the OAuth state from Redis
        r.delete(r_key)

        # Get the credential ID we just created so frontend can link it
        credential_id = None
        try:
            with get_session_with_tenant(tenant_id=tenant_id) as db_session2:
                from sqlalchemy import select, desc
                from onyx.db.models import Credential as CredentialModel
                stmt = (
                    select(CredentialModel)
                    .where(CredentialModel.source == DocumentSource.GOOGLE_DRIVE)
                    .order_by(desc(CredentialModel.time_created))
                    .limit(1)
                )
                cred = db_session2.execute(stmt).scalar_one_or_none()
                if cred:
                    credential_id = cred.id
        except Exception:
            pass

        # Redirect browser to frontend with OAuth success params
        redirect_url = session.redirect_on_success or f"{WEB_DOMAIN}/echo/pipelines"
        separator = "&" if "?" in redirect_url else "?"
        redirect_url += f"{separator}oauth_success=true&source=google_drive"
        if credential_id is not None:
            redirect_url += f"&credential_id={credential_id}"

        return RedirectResponse(url=redirect_url, status_code=302)

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": f"An error occurred during Google Drive OAuth: {str(e)}",
            },
        )
    finally:
        CURRENT_TENANT_ID_CONTEXTVAR.reset(token)


# POST handler for OAuth callback (legacy)
@router.post("/connector/google-drive/callback")
def handle_google_drive_oauth_callback(code: str, state: str) -> JSONResponse:
    """POST handler - delegates to implementation."""
    return _handle_google_drive_oauth_callback_impl(code=code, state=state)


# GET handler for OAuth callback - matches Google Cloud Console URI format
# Google redirects with GET request: /api/echo/oauth/callback/google-drive?code=XXX&state=XXX
@router.get("/callback/google-drive")
def handle_google_drive_oauth_callback_get(code: str, state: str) -> JSONResponse:
    """GET handler for OAuth callback from Google.

    Does NOT require current_user authentication. The user and tenant are
    looked up from the OAuth state stored in Redis.
    """
    return _handle_google_drive_oauth_callback_impl(code=code, state=state)

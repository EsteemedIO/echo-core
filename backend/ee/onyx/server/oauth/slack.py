import base64
import uuid
from typing import cast

import requests
from fastapi import Depends
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ee.onyx.server.oauth.api_router import router
from onyx.auth.users import current_user
from onyx.configs.app_configs import DEV_MODE
from onyx.configs.app_configs import OAUTH_SLACK_CLIENT_ID
from onyx.configs.app_configs import OAUTH_SLACK_CLIENT_SECRET
from onyx.configs.app_configs import WEB_DOMAIN
from onyx.configs.constants import DocumentSource
from onyx.db.credentials import create_credential
from onyx.db.engine.sql_engine import get_session
from onyx.db.engine.sql_engine import get_session_with_tenant
from onyx.db.models import User
from onyx.db.users import get_user_by_email
from onyx.redis.redis_pool import get_redis_client
from onyx.redis.redis_pool import redis_pool
from onyx.server.documents.models import CredentialBase
from shared_configs.contextvars import get_current_tenant_id
from shared_configs.contextvars import CURRENT_TENANT_ID_CONTEXTVAR


class SlackOAuth:
    # https://knock.app/blog/how-to-authenticate-users-in-slack-using-oauth
    # Example: https://api.slack.com/authentication/oauth-v2#exchanging

    class OAuthSession(BaseModel):
        """Stored in redis to be looked up on callback"""

        email: str
        redirect_on_success: str | None  # Where to send the user if OAuth flow succeeds

    CLIENT_ID = OAUTH_SLACK_CLIENT_ID
    CLIENT_SECRET = OAUTH_SLACK_CLIENT_SECRET

    TOKEN_URL = "https://slack.com/api/oauth.v2.access"

    # SCOPE is per https://docs.danswer.dev/connectors/slack
    BOT_SCOPE = (
        "channels:history,"
        "channels:read,"
        "groups:history,"
        "groups:read,"
        "channels:join,"
        "im:history,"
        "users:read,"
        "users:read.email,"
        "usergroups:read"
    )

    REDIRECT_URI = f"{WEB_DOMAIN}/admin/connectors/slack/oauth/callback"
    DEV_REDIRECT_URI = f"https://redirectmeto.com/{REDIRECT_URI}"

    @classmethod
    def generate_oauth_url(cls, state: str) -> str:
        return cls._generate_oauth_url_helper(cls.REDIRECT_URI, state)

    @classmethod
    def generate_dev_oauth_url(cls, state: str) -> str:
        """dev mode workaround for localhost testing
        - https://www.nango.dev/blog/oauth-redirects-on-localhost-with-https
        """

        return cls._generate_oauth_url_helper(cls.DEV_REDIRECT_URI, state)

    @classmethod
    def _generate_oauth_url_helper(cls, redirect_uri: str, state: str) -> str:
        url = (
            f"https://slack.com/oauth/v2/authorize"
            f"?client_id={cls.CLIENT_ID}"
            f"&redirect_uri={redirect_uri}"
            f"&scope={cls.BOT_SCOPE}"
            f"&state={state}"
        )
        return url

    @classmethod
    def session_dump_json(cls, email: str, redirect_on_success: str | None) -> str:
        """Temporary state to store in redis. to be looked up on auth response.
        Returns a json string.
        """
        session = SlackOAuth.OAuthSession(
            email=email, redirect_on_success=redirect_on_success
        )
        return session.model_dump_json()

    @classmethod
    def parse_session(cls, session_json: str) -> OAuthSession:
        session = SlackOAuth.OAuthSession.model_validate_json(session_json)
        return session


def _handle_slack_oauth_callback_impl(code: str, state: str) -> JSONResponse:
    """Handle Slack OAuth callback - extracts tenant from OAuth state, not from request.

    This function does NOT require Depends(get_session) because it extracts
    the tenant_id from the OAuth state stored in Redis BEFORE accessing the database.
    """
    if not SlackOAuth.CLIENT_ID or not SlackOAuth.CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Slack client ID or client secret is not configured.",
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
            detail=f"Slack OAuth failed - OAuth state key not found: key={r_key}",
        )

    session_json = session_json_bytes.decode("utf-8")
    session = SlackOAuth.parse_session(session_json)

    # Extract tenant_id from session email lookup
    # Use the default tenant for now (single-tenant deployment)
    tenant_id = "tenant_oceanic"
    token = CURRENT_TENANT_ID_CONTEXTVAR.set(tenant_id)

    try:
        with get_session_with_tenant(tenant_id=tenant_id) as db_session:
            user = get_user_by_email(session.email, db_session)
            if user is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Slack OAuth failed - User not found: {session.email}",
                )

            # Use direct redirect URI (not dev proxy) for real domains
            _use_dev = DEV_MODE and ("localhost" in (WEB_DOMAIN or "") or "127.0.0.1" in (WEB_DOMAIN or ""))
            if not _use_dev:
                redirect_uri = SlackOAuth.REDIRECT_URI
            else:
                redirect_uri = SlackOAuth.DEV_REDIRECT_URI

            # Exchange the authorization code for an access token
            response = requests.post(
                SlackOAuth.TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "client_id": SlackOAuth.CLIENT_ID,
                    "client_secret": SlackOAuth.CLIENT_SECRET,
                    "code": code,
                    "redirect_uri": redirect_uri,
                },
            )

            response_data = response.json()

            if not response_data.get("ok"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Slack OAuth failed: {response_data.get('error')}",
                )

            # Extract token and team information
            access_token: str = response_data.get("access_token")
            team_id: str = response_data.get("team", {}).get("id")
            authed_user_id: str = response_data.get("authed_user", {}).get("id")

            credential_info = CredentialBase(
                credential_json={"slack_bot_token": access_token},
                admin_public=True,
                source=DocumentSource.SLACK,
                name="Slack OAuth",
            )

            create_credential(credential_info, user, db_session)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": f"An error occurred during Slack OAuth: {str(e)}",
            },
        )
    finally:
        r.delete(r_key)
        CURRENT_TENANT_ID_CONTEXTVAR.reset(token)

    # Redirect browser back to the success URL
    if session.redirect_on_success:
        return RedirectResponse(url=session.redirect_on_success, status_code=302)

    return JSONResponse(
        content={
            "success": True,
            "message": "Slack OAuth completed successfully.",
            "finalize_url": None,
            "redirect_on_success": session.redirect_on_success,
            "team_id": team_id,
            "authed_user_id": authed_user_id,
        }
    )


# POST handler (legacy - used by frontend POST flows)
@router.post("/connector/slack/callback")
def handle_slack_oauth_callback(
    code: str,
    state: str,
    user: User = Depends(current_user),
    db_session: Session = Depends(get_session),
    tenant_id: str | None = Depends(get_current_tenant_id),
) -> JSONResponse:
    return _handle_slack_oauth_callback_impl(code=code, state=state)


# GET handler - Slack redirects browser here after OAuth authorization
# Path matches the REDIRECT_URI: /admin/connectors/slack/oauth/callback?code=XXX&state=XXX
# Mounted at /oauth prefix, so full path is /oauth/callback/slack
@router.get("/callback/slack")
def handle_slack_oauth_callback_get(code: str, state: str) -> JSONResponse:
    """GET handler for OAuth callback from Slack.
    Does NOT require current_user authentication.
    User and tenant are looked up from the OAuth state stored in Redis.
    """
    return _handle_slack_oauth_callback_impl(code=code, state=state)

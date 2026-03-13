"""
NOMOLO Shared Google OAuth Authentication

Reusable OAuth2 logic for any Google API collector.
Handles token loading, refresh, and new authentication flows.
"""

import json
import logging
import os
import sys

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger("nomolo.auth")


def get_google_credentials(credentials_file, token_file, scopes):
    """Authenticate with Google OAuth2. Opens browser on first run.

    Args:
        credentials_file: Path to the OAuth client secrets JSON file
            (downloaded from Google Cloud Console).
        token_file: Path to store/load the user's access token.
        scopes: List of Google API scopes to request.

    Returns:
        google.oauth2.credentials.Credentials object ready to use.

    Raises:
        FileNotFoundError: If credentials_file does not exist.
    """
    creds = None
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.warning(
                "Existing token file is corrupted (%s) — re-authenticating.", e
            )
            os.remove(token_file)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.warning(
                    "Token refresh failed (%s) — re-authenticating. "
                    "You may need to sign in again.",
                    e,
                )
                creds = None

        if not creds:
            if not os.path.exists(credentials_file):
                raise FileNotFoundError(
                    f"OAuth credentials not found: {credentials_file}\n\n"
                    "To get credentials.json:\n"
                    "  1. Go to https://console.cloud.google.com\n"
                    "  2. Create a project (or select existing)\n"
                    "  3. Enable the API you need (Library > search for it)\n"
                    "  4. Go to APIs & Services > Credentials\n"
                    "  5. Create Credentials > OAuth Client ID > Desktop App\n"
                    "  6. Download the JSON and save as: credentials.json\n\n"
                    "For detailed instructions, see: docs/GOOGLE_SETUP.md"
                )

            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, scopes
                )
            except (json.JSONDecodeError, ValueError) as e:
                print(f"\nError: {credentials_file} is not valid JSON: {e}")
                print(
                    "Re-download it from Google Cloud Console > APIs & Services > Credentials."
                )
                sys.exit(1)

            try:
                creds = flow.run_local_server(port=0)
            except OSError as e:
                print(f"\nError: Could not start local OAuth server: {e}")
                print(
                    "This usually means another process is blocking the port."
                )
                print("Close other running NOMOLO instances and try again.")
                sys.exit(1)

        fd = os.open(token_file, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(creds.to_json())

    return creds


def check_token_scopes(token_file, required_scopes):
    """Check if an existing token has all the required scopes.

    Useful for detecting when a user needs to re-auth because a new
    scope was added to a collector.

    Args:
        token_file: Path to the stored token JSON file.
        required_scopes: List of scope strings that must all be present.

    Returns:
        True if the token exists and contains all required scopes,
        False otherwise (missing file, corrupted token, or missing scopes).
    """
    if not os.path.exists(token_file):
        return False

    try:
        creds = Credentials.from_authorized_user_file(token_file)
    except (json.JSONDecodeError, ValueError, KeyError):
        return False

    if not creds.scopes:
        return False

    return all(scope in creds.scopes for scope in required_scopes)

"""
Mock Gmail API for testing the full WHID pipeline without credentials.

Simulates:
- OAuth credentials
- Message listing with pagination
- Batch message fetching
- Rate limiting (configurable)
- Realistic email payloads
"""

import base64
import json
import random
import string
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from googleapiclient.errors import HttpError


def _random_id(length=16):
    return "".join(random.choices(string.hexdigits[:16], k=length))


def _make_email(
    msg_id=None,
    subject="Test Email",
    sender="alice@example.com",
    to="bob@example.com",
    body="This is a test email body.",
    date=None,
    labels=None,
):
    """Create a realistic Gmail API message response."""
    msg_id = msg_id or _random_id()
    if date is None:
        date = datetime.now(timezone.utc)

    date_str = date.strftime("%a, %d %b %Y %H:%M:%S %z")
    body_encoded = base64.urlsafe_b64encode(body.encode()).decode()

    return {
        "id": msg_id,
        "threadId": f"thread_{msg_id}",
        "labelIds": labels or ["INBOX"],
        "payload": {
            "headers": [
                {"name": "Date", "value": date_str},
                {"name": "Subject", "value": subject},
                {"name": "From", "value": sender},
                {"name": "To", "value": to},
            ],
            "mimeType": "text/plain",
            "body": {"data": body_encoded},
        },
    }


def _make_html_email(msg_id=None, subject="HTML Email", body_html="<p>Hello <b>World</b></p>"):
    """Create an email with HTML body."""
    msg_id = msg_id or _random_id()
    html_encoded = base64.urlsafe_b64encode(body_html.encode()).decode()
    date = datetime.now(timezone.utc)
    date_str = date.strftime("%a, %d %b %Y %H:%M:%S %z")

    return {
        "id": msg_id,
        "threadId": f"thread_{msg_id}",
        "labelIds": ["INBOX"],
        "payload": {
            "headers": [
                {"name": "Date", "value": date_str},
                {"name": "Subject", "value": subject},
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "receiver@example.com"},
            ],
            "mimeType": "multipart/alternative",
            "parts": [
                {
                    "mimeType": "text/html",
                    "body": {"data": html_encoded},
                }
            ],
        },
    }


class MockGmailInbox:
    """
    Simulates a Gmail inbox with configurable size and behavior.

    Usage:
        inbox = MockGmailInbox(num_messages=100, rate_limit_after=50)
        service = inbox.build_service()
        # Use service exactly like googleapiclient.discovery.build() result
    """

    def __init__(
        self,
        num_messages=50,
        rate_limit_after=None,
        rate_limit_count=5,
        page_size=10,
        include_html=True,
        include_bad_dates=True,
    ):
        self.messages = {}
        self.rate_limit_after = rate_limit_after
        self.rate_limit_count = rate_limit_count
        self.rate_limited_so_far = 0
        self.fetch_count = 0
        self.page_size = page_size

        # Generate realistic emails spanning multiple years
        base_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        senders = [
            "alice@example.com",
            "bob@company.com",
            "newsletter@shop.com",
            "support@service.io",
            "friend@gmail.com",
        ]
        subjects = [
            "Meeting tomorrow",
            "Invoice #{}",
            "Re: Project update",
            "Your order has shipped",
            "Welcome to our platform",
            "Quick question",
            "Reminder: deadline approaching",
            "Photos from last weekend",
            "Newsletter: Weekly digest",
            "Account security alert",
        ]

        for i in range(num_messages):
            msg_id = f"msg_{i:06d}"
            days_offset = int(i * (365 * 4 / num_messages))  # spread over 4 years
            date = base_date + timedelta(days=days_offset, hours=random.randint(0, 23))
            subject = random.choice(subjects).format(i)
            sender = random.choice(senders)

            if include_html and i % 5 == 0:
                self.messages[msg_id] = _make_html_email(
                    msg_id=msg_id,
                    subject=subject,
                    body_html=f"<html><body><h1>{subject}</h1><p>Email body {i}</p><script>track()</script></body></html>",
                )
                # Fix date in HTML emails
                self.messages[msg_id]["payload"]["headers"][0]["value"] = date.strftime(
                    "%a, %d %b %Y %H:%M:%S %z"
                )
            elif include_bad_dates and i % 20 == 0:
                msg = _make_email(
                    msg_id=msg_id,
                    subject=subject,
                    sender=sender,
                    body=f"Email body number {i}",
                    date=date,
                )
                msg["payload"]["headers"][0]["value"] = "INVALID DATE"
                self.messages[msg_id] = msg
            else:
                self.messages[msg_id] = _make_email(
                    msg_id=msg_id,
                    subject=subject,
                    sender=sender,
                    body=f"Email body number {i}. " * 10,
                    date=date,
                )

    def build_service(self):
        """Build a mock Gmail API service object."""
        service = MagicMock()
        users = MagicMock()
        messages = MagicMock()

        service.users.return_value = users
        users.messages.return_value = messages

        # Mock messages().list()
        all_ids = list(self.messages.keys())

        def mock_list(userId="me", maxResults=500, pageToken=None):
            start = 0
            if pageToken:
                start = int(pageToken)

            end = min(start + self.page_size, len(all_ids))
            chunk = [{"id": mid} for mid in all_ids[start:end]]

            result = {"messages": chunk}
            if end < len(all_ids):
                result["nextPageToken"] = str(end)

            mock_request = MagicMock()
            mock_request.execute.return_value = result
            return mock_request

        messages.list = mock_list

        # Mock messages().get()
        def mock_get(userId="me", id=None):
            self.fetch_count += 1

            # Simulate rate limiting
            if (
                self.rate_limit_after is not None
                and self.fetch_count > self.rate_limit_after
                and self.rate_limited_so_far < self.rate_limit_count
            ):
                self.rate_limited_so_far += 1
                mock_request = MagicMock()
                resp = MagicMock()
                resp.status = 429
                mock_request.execute.side_effect = HttpError(
                    resp, b'{"error": {"message": "Rate limit exceeded"}}'
                )
                return mock_request

            if id not in self.messages:
                mock_request = MagicMock()
                resp = MagicMock()
                resp.status = 404
                mock_request.execute.side_effect = HttpError(
                    resp, b'{"error": {"message": "Not found"}}'
                )
                return mock_request

            mock_request = MagicMock()
            mock_request.execute.return_value = self.messages[id]
            return mock_request

        messages.get = mock_get

        # Mock new_batch_http_request
        def mock_batch_http_request(callback=None):
            batch = MockBatchRequest(callback, messages)
            return batch

        service.new_batch_http_request = mock_batch_http_request

        return service

    def build_credentials(self):
        """Build mock credentials that look valid."""
        creds = MagicMock()
        creds.valid = True
        creds.expired = False
        return creds


class MockBatchRequest:
    """Simulates Gmail batch HTTP request."""

    def __init__(self, callback, messages_resource):
        self.callback = callback
        self.messages_resource = messages_resource
        self.requests = []

    def add(self, request, request_id=None):
        self.requests.append((request, request_id))

    def execute(self):
        for request, request_id in self.requests:
            try:
                response = request.execute()
                self.callback(request_id, response, None)
            except HttpError as e:
                self.callback(request_id, None, e)
            except Exception as e:
                self.callback(request_id, None, e)

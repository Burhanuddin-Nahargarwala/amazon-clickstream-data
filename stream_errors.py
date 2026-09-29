import asyncio
import re

from azure.eventhub import exceptions as eventhub_exceptions
from azure.eventhub.aio import EventHubProducerClient
from google.api_core import exceptions as google_exceptions
from google.auth import exceptions as google_auth_exceptions
from google.auth.transport.requests import Request
from google.oauth2 import service_account

# Who has to act on an error: the user (bad details / permissions on their cloud) or us (bug, outage)
USER = "user"
SERVER = "server"

PREFLIGHT_TIMEOUT_SECONDS = 30


class StreamError(Exception):
    def __init__(self, message, source, detail=""):
        super().__init__(message)
        self.message = message
        self.source = source
        self.detail = detail


def _detail(err):
    # First line of the raw error for troubleshooting, with any access key masked
    text = f"{type(err).__name__}: {err}".strip().splitlines()[0]
    text = re.sub(r"(SharedAccessKey=)[^;\s]+", r"\1***", text, flags=re.IGNORECASE)
    return text[:300]


def describe_azure_error(err, hub_name):
    text = str(err).lower()
    detail = _detail(err)

    if isinstance(err, ValueError):
        return StreamError(
            "The connection string is not in the expected format. Copy it from your Event Hubs namespace: "
            "Shared access policies > your policy > Connection string-primary key.",
            USER, detail,
        )
    if "name or service not known" in text or "getaddrinfo" in text or "nodename nor servname" in text:
        return StreamError(
            "Could not find the Event Hubs namespace. Check the Endpoint (sb://<namespace>.servicebus.windows.net/) "
            "in your connection string.",
            USER, detail,
        )
    if "not-found" in text or "could not be found" in text or "notfound" in text:
        return StreamError(
            f"Event hub '{hub_name}' was not found in this namespace. Check the hub name (it is case sensitive).",
            USER, detail,
        )
    if (
        isinstance(err, eventhub_exceptions.AuthenticationError)
        or "unauthorized" in text
        or "put-token" in text
        or "put token" in text
    ):
        return StreamError(
            "Azure rejected the credentials. Check the SharedAccessKeyName/SharedAccessKey and that the policy "
            "has the Send permission on this event hub.",
            USER, detail,
        )
    if isinstance(err, (eventhub_exceptions.ConnectError, eventhub_exceptions.OperationTimeoutError, asyncio.TimeoutError)):
        return StreamError(
            "Could not connect to Azure Event Hubs. If your namespace has a firewall or private endpoint enabled, "
            "it must allow public network access.",
            USER, detail,
        )
    return StreamError("Unexpected error while sending to Azure Event Hubs.", SERVER, detail)


def describe_gcp_error(err, project_id, topic_id):
    detail = _detail(err)

    if isinstance(err, (ValueError, KeyError, TypeError)) or type(err).__name__ == "MalformedError":
        return StreamError(
            "The service account key file is not valid. Upload the JSON key downloaded from "
            "IAM & Admin > Service accounts > Keys.",
            USER, detail,
        )
    if isinstance(err, google_auth_exceptions.RefreshError) or isinstance(err, google_exceptions.Unauthenticated):
        return StreamError(
            "Google rejected the service account key. It may have been deleted or disabled - create a new key.",
            USER, detail,
        )
    if isinstance(err, google_exceptions.NotFound):
        return StreamError(
            f"Topic '{topic_id}' was not found in project '{project_id}'. Check the project ID and topic ID.",
            USER, detail,
        )
    if isinstance(err, google_exceptions.PermissionDenied):
        return StreamError(
            "The service account is not allowed to publish to this topic. Grant it the Pub/Sub Publisher role.",
            USER, detail,
        )
    if isinstance(err, google_exceptions.InvalidArgument):
        return StreamError("The project ID or topic ID is not valid.", USER, detail)
    return StreamError("Unexpected error while publishing to Google Pub/Sub.", SERVER, detail)


async def check_azure_connection(connection_string, hub_name):
    """Opens a sender link to the event hub without sending anything, so bad details fail before the job starts."""
    try:
        producer = EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=hub_name, retry_total=1
        )
        async with producer:
            # create_batch() opens the sender link, which validates namespace, key, permission and hub name
            await asyncio.wait_for(producer.create_batch(), timeout=PREFLIGHT_TIMEOUT_SECONDS)
    except Exception as err:
        raise describe_azure_error(err, hub_name) from err


def _refresh_gcp_credentials(credentials_info):
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=["https://www.googleapis.com/auth/pubsub"]
    )
    credentials.refresh(Request())


async def check_gcp_connection(credentials_info, project_id, topic_id):
    """Checks the key file is valid and accepted by Google. Topic and permission problems show up on the
    first publish, because the Pub/Sub Publisher role cannot read topic metadata."""
    try:
        await asyncio.wait_for(
            asyncio.to_thread(_refresh_gcp_credentials, credentials_info), timeout=PREFLIGHT_TIMEOUT_SECONDS
        )
    except Exception as err:
        raise describe_gcp_error(err, project_id, topic_id) from err

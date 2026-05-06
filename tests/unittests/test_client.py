import unittest
from unittest.mock import MagicMock, patch, call

import requests
from parameterized import parameterized

from tap_pardot.client import (
    Client,
    Pardot5xxError,
    Pardot401Error,
    Pardot89Error,
    AuthCredsMissingError,
    PardotException,
    is_not_retryable_pardot_exception,
)


class MockResponse:
    """Mock HTTP response for testing."""

    def __init__(self, status_code, json_data=None, raise_for_status_error=False):
        self.status_code = status_code
        self.json_data = json_data or {}
        self.raise_for_status_error = raise_for_status_error

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.raise_for_status_error:
            raise requests.HTTPError(f"HTTP Error {self.status_code}")


class TestClientInitialization(unittest.TestCase):
    """Test Client initialization with various credential types."""

    @patch("tap_pardot.client.Client.refresh_credentials")
    def test_init_with_oauth_credentials(self, mock_refresh):
        """Test client initializes with OAuth credentials."""
        creds = {
            "refresh_token": "test_refresh_token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "pardot_business_unit_id": "test_bu_id",
        }
        client = Client(creds)

        self.assertEqual(client.api_version, "4")
        mock_refresh.assert_called_once()

    @patch("tap_pardot.client.Client.login")
    def test_init_with_api_key_credentials(self, mock_login):
        """Test client initializes with API key credentials."""
        creds = {
            "email": "test@example.com",
            "password": "test_password",
            "user_key": "test_user_key",
        }
        client = Client(creds)

        self.assertEqual(client.api_version, "4")
        mock_login.assert_called_once()

    def test_init_missing_credentials_raises_error(self):
        """Test client raises error when credentials are missing."""
        creds = {"start_date": "2020-01-01T00:00:00Z"}

        with self.assertRaises(AuthCredsMissingError):
            Client(creds)

    @patch("tap_pardot.client.Client.refresh_credentials")
    def test_has_oauth_values_true(self, mock_refresh):
        """Test has_oauth_values returns True with complete OAuth config."""
        creds = {
            "refresh_token": "rt",
            "client_id": "cid",
            "client_secret": "cs",
            "pardot_business_unit_id": "buid",
        }
        client = Client(creds)
        self.assertTrue(client.has_oauth_values())

    @patch("tap_pardot.client.Client.refresh_credentials")
    def test_has_oauth_values_false_missing_field(self, mock_refresh):
        """Test has_oauth_values returns False with incomplete OAuth config."""
        creds = {
            "refresh_token": "rt",
            "client_id": "cid",
            "client_secret": "cs",
            "pardot_business_unit_id": "buid",
        }
        # Need to construct without triggering init logic
        client = Client(creds)
        client.creds = {"refresh_token": "rt", "client_id": "cid"}
        self.assertFalse(client.has_oauth_values())

    @patch("tap_pardot.client.Client.login")
    def test_has_api_key_auth_values_true(self, mock_login):
        """Test has_api_key_auth_values returns True with API key config."""
        creds = {
            "email": "test@example.com",
            "password": "pwd",
            "user_key": "uk",
        }
        client = Client(creds)
        self.assertTrue(client.has_api_key_auth_values())

    @patch("tap_pardot.client.Client.login")
    def test_has_api_key_auth_values_false(self, mock_login):
        """Test has_api_key_auth_values returns False with missing fields."""
        creds = {
            "email": "test@example.com",
            "password": "pwd",
            "user_key": "uk",
        }
        client = Client(creds)
        client.creds = {"email": "test@example.com"}
        self.assertFalse(client.has_api_key_auth_values())


class TestClientLogin(unittest.TestCase):
    """Test Client login method."""

    @patch("tap_pardot.client.requests.post")
    def test_login_success(self, mock_post):
        """Test successful login returns api_key."""
        mock_post.return_value = MockResponse(
            200, json_data={"api_key": "test_api_key"}
        )

        creds = {
            "email": "test@example.com",
            "password": "test_password",
            "user_key": "test_user_key",
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = creds
            client.api_version = "4"
            client.api_key = None
            client.login()

        self.assertEqual(client.api_key, "test_api_key")

    @patch("tap_pardot.client.requests.post")
    def test_login_pardot_error(self, mock_post):
        """Test login raises PardotException on error response."""
        mock_post.return_value = MockResponse(
            200,
            json_data={
                "err": "Invalid credentials",
                "@attributes": {"err_code": 15},
            },
        )

        creds = {
            "email": "test@example.com",
            "password": "wrong_password",
            "user_key": "test_user_key",
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = creds
            client.api_version = "4"
            client.api_key = None

            with self.assertRaises(PardotException):
                client.login()

    @patch("tap_pardot.client.requests.post")
    def test_login_http_error(self, mock_post):
        """Test login raises HTTPError on non-200 response."""
        mock_post.return_value = MockResponse(
            500, raise_for_status_error=True
        )

        creds = {
            "email": "test@example.com",
            "password": "test_password",
            "user_key": "test_user_key",
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = creds
            client.api_version = "4"
            client.api_key = None

            with self.assertRaises(requests.HTTPError):
                client.login()


class TestClientRefreshCredentials(unittest.TestCase):
    """Test Client refresh_credentials method."""

    @patch("tap_pardot.client.requests.request")
    def test_refresh_credentials_success(self, mock_request):
        """Test successful credential refresh stores access_token."""
        mock_request.return_value = MockResponse(
            200, json_data={"access_token": "new_access_token"}
        )

        creds = {
            "refresh_token": "test_refresh_token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "pardot_business_unit_id": "test_bu_id",
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = creds
            client.api_version = "4"
            client.refresh_credentials()

        self.assertEqual(client.creds["access_token"], "new_access_token")

    @patch("tap_pardot.client.requests.request")
    def test_refresh_credentials_http_error(self, mock_request):
        """Test refresh_credentials raises on HTTP error."""
        mock_request.return_value = MockResponse(
            401, raise_for_status_error=True
        )

        creds = {
            "refresh_token": "test_refresh_token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "pardot_business_unit_id": "test_bu_id",
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = creds
            client.api_version = "4"

            with self.assertRaises(requests.HTTPError):
                client.refresh_credentials()


class TestClientMakeRequest(unittest.TestCase):
    """Test Client _make_request method."""

    def _create_client_with_oauth(self):
        """Helper to create a client with OAuth creds without triggering init."""
        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {
                "refresh_token": "rt",
                "client_id": "cid",
                "client_secret": "cs",
                "pardot_business_unit_id": "buid",
                "access_token": "test_access_token",
            }
            client.api_version = "4"
            client.api_key = None
            return client

    def _create_client_with_api_key(self):
        """Helper to create a client with API key creds without triggering init."""
        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {
                "email": "test@example.com",
                "password": "pwd",
                "user_key": "uk",
            }
            client.api_version = "4"
            client.api_key = "test_api_key"
            return client

    @patch("tap_pardot.client.requests.request")
    def test_make_request_success(self, mock_request):
        """Test successful API request returns content."""
        mock_request.return_value = MockResponse(
            200, json_data={"result": {"total_results": 1, "prospect": [{"id": 1}]}}
        )
        client = self._create_client_with_oauth()

        result = client._make_request("get", "https://pi.pardot.com/api/prospect/version/{}/do/query")
        self.assertEqual(result["result"]["total_results"], 1)

    @patch("tap_pardot.client.requests.request")
    def test_make_request_401_triggers_refresh(self, mock_request):
        """Test 401 response triggers credential refresh for OAuth."""
        mock_request.return_value = MockResponse(401, json_data={})
        client = self._create_client_with_oauth()

        with patch.object(client, "refresh_credentials") as mock_refresh:
            with self.assertRaises(Pardot401Error):
                client._make_request("get", "https://pi.pardot.com/api/prospect/version/{}/do/query")

            # Called on each retry attempt (backoff retries 3 times)
            self.assertEqual(mock_refresh.call_count, 3)

    @patch("tap_pardot.client.requests.request")
    def test_make_request_5xx_raises_error(self, mock_request):
        """Test 5xx response raises Pardot5xxError."""
        mock_request.return_value = MockResponse(500, json_data={})
        client = self._create_client_with_oauth()

        with self.assertRaises(Pardot5xxError):
            client._make_request("get", "https://pi.pardot.com/api/prospect/version/{}/do/query")

    @patch("tap_pardot.client.Client.login")
    @patch("tap_pardot.client.requests.request")
    def test_make_request_error_code_1_reauths(self, mock_request, mock_login):
        """Test error code 1 triggers re-authentication."""
        # First call returns error code 1, second call after re-auth succeeds
        error_response = MockResponse(
            200,
            json_data={
                "err": "Invalid API key or user key",
                "@attributes": {"err_code": 1},
            },
        )
        success_response = MockResponse(
            200, json_data={"result": {"total_results": 1}}
        )
        mock_request.side_effect = [error_response, success_response]
        client = self._create_client_with_api_key()

        result = client._make_request("get", "https://pi.pardot.com/api/prospect/version/{}/do/query")
        mock_login.assert_called_once()

    @patch("tap_pardot.client.requests.request")
    def test_make_request_error_code_89_switches_version(self, mock_request):
        """Test error code 89 switches API version to 3."""
        mock_request.return_value = MockResponse(
            200,
            json_data={
                "err": "Use API version 3",
                "@attributes": {"err_code": 89},
            },
        )
        client = self._create_client_with_api_key()

        with self.assertRaises(Pardot89Error):
            client._make_request("get", "https://pi.pardot.com/api/prospect/version/{}/do/query")

        self.assertEqual(client.api_version, "3")


class TestClientDescribe(unittest.TestCase):
    """Test Client describe method."""

    @patch("tap_pardot.client.Client._make_request")
    def test_describe_success(self, mock_make_request):
        """Test describe returns content on success."""
        mock_make_request.return_value = {
            "result": {"field": [{"@attributes": {"id": "field1"}}]}
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {"email": "e", "password": "p", "user_key": "u"}
            client.api_version = "4"
            client.api_key = "key"

            result = client.describe("prospect")
            self.assertIn("result", result)

    @patch("tap_pardot.client.Client._make_request")
    def test_describe_error_raises_exception(self, mock_make_request):
        """Test describe raises PardotException on error."""
        mock_make_request.return_value = {
            "err": "Endpoint not found",
            "@attributes": {"err_code": 71},
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {"email": "e", "password": "p", "user_key": "u"}
            client.api_version = "4"
            client.api_key = "key"

            with self.assertRaises(PardotException):
                client.describe("invalid_endpoint")


class TestClientGetPost(unittest.TestCase):
    """Test Client get and post methods."""

    @patch("tap_pardot.client.Client._make_request")
    def test_get_success(self, mock_make_request):
        """Test get method returns content."""
        mock_make_request.return_value = {
            "result": {"total_results": 2, "prospect": [{"id": 1}, {"id": 2}]}
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {"email": "e", "password": "p", "user_key": "u"}
            client.api_version = "4"
            client.api_key = "key"

            result = client.get("prospect", sort_by="id")
            self.assertEqual(result["result"]["total_results"], 2)

    @patch("tap_pardot.client.Client._make_request")
    def test_post_success(self, mock_make_request):
        """Test post method returns content."""
        mock_make_request.return_value = {
            "result": {"total_results": 1, "visit": [{"id": 1}]}
        }

        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {"email": "e", "password": "p", "user_key": "u"}
            client.api_version = "4"
            client.api_key = "key"

            result = client.post("visit", visitor_ids="1,2,3")
            self.assertEqual(result["result"]["total_results"], 1)


class TestClientAuthHeaders(unittest.TestCase):
    """Test Client _get_auth_header method."""

    def test_oauth_auth_header(self):
        """Test auth header with OAuth credentials."""
        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {
                "refresh_token": "rt",
                "client_id": "cid",
                "client_secret": "cs",
                "pardot_business_unit_id": "buid",
                "access_token": "my_token",
            }
            client.api_version = "4"
            client.api_key = None

            headers = client._get_auth_header()
            self.assertEqual(headers["Authorization"], "Bearer my_token")
            self.assertEqual(headers["Pardot-Business-Unit-Id"], "buid")

    def test_api_key_auth_header(self):
        """Test auth header with API key credentials."""
        with patch.object(Client, "__init__", lambda self, c: None):
            client = Client(None)
            client.creds = {
                "email": "test@example.com",
                "password": "pwd",
                "user_key": "my_user_key",
            }
            client.api_version = "4"
            client.api_key = "my_api_key"

            headers = client._get_auth_header()
            self.assertIn("my_api_key", headers["Authorization"])
            self.assertIn("my_user_key", headers["Authorization"])


class TestIsNotRetryablePardotException(unittest.TestCase):
    """Test is_not_retryable_pardot_exception function."""

    def test_pardot_401_is_retryable(self):
        """Test Pardot401Error is retryable (returns False)."""
        exc = Pardot401Error()
        self.assertFalse(is_not_retryable_pardot_exception(exc))

    def test_pardot_89_is_retryable(self):
        """Test Pardot89Error is retryable (returns False)."""
        exc = Pardot89Error()
        self.assertFalse(is_not_retryable_pardot_exception(exc))

    def test_pardot_5xx_is_retryable(self):
        """Test Pardot5xxError is retryable (returns False)."""
        exc = Pardot5xxError()
        self.assertFalse(is_not_retryable_pardot_exception(exc))

    def test_pardot_exception_code_66_is_retryable(self):
        """Test PardotException with code 66 is retryable."""
        exc = PardotException(
            "Rate limit", {"@attributes": {"err_code": 66}}
        )
        self.assertFalse(is_not_retryable_pardot_exception(exc))

    def test_pardot_exception_other_code_is_not_retryable(self):
        """Test PardotException with other codes are not retryable."""
        exc = PardotException(
            "Other error", {"@attributes": {"err_code": 99}}
        )
        self.assertTrue(is_not_retryable_pardot_exception(exc))


if __name__ == "__main__":
    unittest.main()

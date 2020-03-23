import backoff
import requests
import singer

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

LOGGER = singer.get_logger()

AUTH_URL = "https://pi.pardot.com/api/login/version/3"
ENDPOINT_BASE = "https://pi.pardot.com/api/"


class PardotException(Exception):
    def __init__(self, message, response_content):
        self.code = response_content.get("@attributes", {}).get("err_code")
        self.response = response_content
        super().__init__(message)


class Client:
    """Lightweight Client wrapper to allow switching between version 3 and 4 API based
    on availability, if desired."""

    api_version = None
    api_key = None
    creds = None

    get_url = "{}/version/{}/do/query"
    describe_url = "{}/version/{}/do/describe"

    def __init__(self, creds):
        self.creds = creds
        self.requests_session = requests.Session()
        self.login()

    def login(self):
        content = self._make_request(
            "post",
            AUTH_URL,
            data={
                "email": self.creds["email"],
                "password": self.creds["password"],
                "user_key": self.creds["user_key"],
            },
            params={"format": "json"},
        )

        self._check_error(content, "authenticating")

        self.api_version = content.get("version") or "3"
        self.api_key = content["api_key"]

    def _check_error(self, content, activity):
        error_message = content.get("err")
        if error_message:
            error_code = content["@attributes"]["err_code"]
            raise PardotException(
                "Pardot returned error code {} while {}. Message: {}".format(
                    error_code, activity, error_message
                ),
                content,
            )

    def _get_auth_header(self):
        return {
            "Authorization": "Pardot api_key={}, user_key={}".format(
                self.api_key, self.creds["user_key"]
            )
        }

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError, PardotException),
        jitter=None,
        max_tries=10,
    )
    def _make_request(self, method, url, params=None, data=None):
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            url,
            method.upper(),
            url,
            params,
        )

        response = self.requests_session.request(
            method, url, headers=self._get_auth_header(), params=params, data=data
        )
        response.raise_for_status()
        content = response.json()
        error_message = content.get("err")

        if error_message:
            error_code = content["@attributes"]["err_code"]

            # Error code 1 indicates a bad api_key or user_key
            # If we get error code 1 then re-authenticate login
            # http://developer.pardot.com/kb/error-codes-messages/#error-code-1

            if error_code == 1:
                LOGGER.info("API key or user key expired -- Reauthenticating once")
                self.login()
                response = self.requests_session.request(
                    method, url, headers=self._get_auth_header(), params=params
                )
                content = response.json()

        return content

    def describe(self, endpoint, **kwargs):
        url = (ENDPOINT_BASE + self.describe_url).format(endpoint, self.api_version)

        params = {"format": "json", "output": "bulk", **kwargs}

        content = self._make_request("get", url, params)

        self._check_error(content, "describing endpoint")

        return content

    def _fetch(self, method, endpoint, format_params, **kwargs):
        base_formatting = [endpoint, self.api_version]
        if format_params:
            base_formatting.extend(format_params)
        url = (ENDPOINT_BASE + self.get_url).format(*base_formatting)

        params = {"format": "json", "output": "bulk", **kwargs}

        content = self._make_request(method, url, params)

        self._check_error(content, "retrieving endpoint")

        return content

    def get(self, endpoint, format_params=None, **kwargs):
        return self._fetch("get", endpoint, format_params, **kwargs)

    def post(self, endpoint, format_params=None, **kwargs):
        return self._fetch("post", endpoint, format_params, **kwargs)

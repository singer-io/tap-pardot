import backoff
import requests
import singer

from base64 import b64encode

LOGGER = singer.get_logger()

AUTH_URL = "https://pi.pardot.com/api/login/version/3"
ENDPOINT_BASE = "https://pi.pardot.com/api/"
REFRESH_URL = "https://login.salesforce.com/services/oauth2/token"


class Pardot5xxError(Exception):
    pass

class Pardot401Error(Exception):
    pass

class Pardot89Error(Exception):
    pass

class AuthCredsMissingError(Exception):
    def __init__(self, message):
        super().__init__(message)

class PardotException(Exception):
    def __init__(self, message, response_content):
        self.code = response_content.get("@attributes", {}).get("err_code")
        self.response = response_content
        super().__init__(message)


def is_not_retryable_pardot_exception(exc):
    if isinstance(exc, Pardot401Error):
        return False
    if isinstance(exc, Pardot89Error):
        return False
    if isinstance(exc, Pardot5xxError):
        return False
    if exc.code == 66:
        LOGGER.warn("Exceeded concurrent request limit, backing off exponentially.")
        return False
    return True


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
        self.api_version = "4"
        if self.has_oauth_values():
            self.refresh_credentials()
        elif self.has_api_key_auth_values():
            self.login()
        else:
            raise AuthCredsMissingError("Requires OAuth credentials refresh token, client id, client secret, or Pardot Business Unit Id.")


    def has_oauth_values(self):
        return self.creds.get('refresh_token') and self.creds.get('client_id') and self.creds.get('client_secret') and self.creds.get('pardot_business_unit_id')

    def has_api_key_auth_values(self):
        return self.creds.get('email') and self.creds.get('password') and self.creds.get('user_key')

    def login(self):
        response = requests.post(
            AUTH_URL,
            data={
                "email": self.creds["email"],
                "password": self.creds["password"],
                "user_key": self.creds["user_key"],
            },
            params={"format": "json"},
        )

        # This will only work if they use HTTP codes. Handling Pardot
        # errors below.
        response.raise_for_status()

        content = response.json()

        self._check_error(content, "authenticating")


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
        if self.has_oauth_values():
            headers = {
                "Authorization": "Bearer {}".format(self.creds["access_token"]),
                "Pardot-Business-Unit-Id": self.creds["pardot_business_unit_id"]
            }
        # This is the case where the tap has api_key auth config set up
        else:
            headers = {
                "Authorization": "Pardot api_key={}, user_key={}".format(
                    self.api_key, self.creds["user_key"]
                )
            }

        return headers

    def refresh_credentials(self):
        header_token = b64encode((self.creds["client_id"] + ":" + self.creds["client_secret"]).encode('utf-8'))

        headers = {
            "Authorization": "Basic " + header_token.decode('utf-8'),
            "Content-Type": "application/x-www-form-urlencoded"
        }

        params = {
            "grant_type": "refresh_token",
            "refresh_token": self.creds["refresh_token"],
        }
        method = "POST"

        response = requests.request(
            method,
            REFRESH_URL,
            headers=headers,
            params=params
        )

        response.raise_for_status()
        response = response.json()

        self.creds['access_token'] = response["access_token"]


    @backoff.on_exception(
        backoff.expo,
        (Pardot401Error,Pardot89Error),
        max_tries=3,
        giveup=is_not_retryable_pardot_exception,
    )
    def _make_request(self, method, url, params=None):
        full_url = url.format(self.api_version)
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            full_url,
            params,
        )

        response = requests.request(
            method, full_url, headers=self._get_auth_header(), params=params
        )

        if response.status_code == 401:
            if self.has_oauth_values():
                LOGGER.warning("Received a 401 unauthenticated error from Pardot. Reauthing and retrying the request.")
                self.refresh_credentials()
                raise Pardot401Error

        # 5xx errors should be retried
        if response.status_code >= 500:
            raise Pardot5xxError()

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
                response = requests.request(
                    method, full_url, headers=self._get_auth_header(), params=params
                )
                content = response.json()
            if error_code == 89:
                # 89 specifically means you are using api version 4 and should use 3
                # https://developer.pardot.com/kb/error-codes-messages/#error-code-89
                LOGGER.info("Pardot returned error code 89, switching to api version 3")
                self.api_version = "3"
                raise Pardot89Error

        return content

    @backoff.on_exception(
        backoff.expo,
        (PardotException,Pardot5xxError),
        giveup=is_not_retryable_pardot_exception,
    )
    def describe(self, endpoint, **kwargs):
        url = (ENDPOINT_BASE + self.describe_url).format(endpoint, '{}')

        params = {"format": "json", "output": "bulk", **kwargs}

        content = self._make_request("get", url, params)

        self._check_error(content, "describing endpoint")

        return content

    @backoff.on_exception(
        backoff.expo,
        (PardotException,Pardot5xxError),
        giveup=is_not_retryable_pardot_exception,
        jitter=None,
    )
    def _fetch(self, method, endpoint, format_params, **kwargs):
        base_formatting = [endpoint, '{}']
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

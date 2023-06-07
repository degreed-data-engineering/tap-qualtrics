"""Stream class for tap-qualtrics."""

import base64
import json
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing
from functools import cached_property
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
import requests


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class TapQualtricsStream(RESTStream):
    """Qualtrics stream class."""
    
    _LOG_REQUEST_METRIC_URLS: bool = True
    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return f"https://api.qualtricshq.com"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
        return headers

    @property
    def authenticator(self):
        http_headers = {}

        # If only api_token is provided, use "Basic 123456789abcdefghijklmnopqrstuv" authentication
        if self.config.get("api_token") and not self.config.get("api_key") and not self.config.get("app_key"):
            http_headers["Authorization"] = "Basic " +  self.config.get("api_token")


        # If api and app keys are provided, and POST required:
        if self.config.get("api_key") and self.config.get("app_key"):
            http_headers["DD-API-KEY"] = self.config.get("api_key")
            http_headers["DD-APPLICATION-KEY"] = self.config.get("app_key")

        return SimpleAuthenticator(stream=self, auth_headers=http_headers)

class Events(TapQualtricsStream):
    name = "events" # Stream name 
    path = "/api/v2/logs/events/search" # API endpoint after base_url 
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]" # https://jsonpath.com Use requests response json to identify the json path 
    replication_key = None
    #schema_filepath = SCHEMAS_DIR / "events.json"  # Optional: use schema_filepath with .json inside schemas/ 

    # Optional: If using schema_filepath, remove the propertyList schema method below
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
    ).to_dict()
    # Overwrite GET here by updating rest_method
    rest_method = "POST"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Define request parameters to return"""
        payload = {"filter": {"query": "source:degreed.api env:production","from": self.config.get("start_date")},"page": {"limit": 4}}
        return payload

    # For passing url parameters: 
    # def get_url_params(
    #     self, context: Optional[dict], next_page_token: Optional[Any]
    # ) -> Dict[str, Any]:






### Qualtrics to use for new stream 
# class QualtricsStream(RESTStream):
#     """Qualtrics stream class."""

#     # TODO: Set the API's base URL here:
#     url_base = "https://api.mysample.com"

#     # OR use a dynamic url_base:
#     # @property
#     # def url_base(self) -> str:
#     #     """Return the API URL root, configurable via tap settings."""
#     #     return self.config["api_url"]

#     records_jsonpath = "$[*]"  # Or override `parse_response`.
#     next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.

#     @property
#     def authenticator(self) -> BasicAuthenticator:
#         """Return a new authenticator object."""
#         return BasicAuthenticator.create_for_stream(
#             self,
#             username=self.config.get("username"),
#             password=self.config.get("password"),
#         )

#     @property
#     def http_headers(self) -> dict:
#         """Return the http headers needed."""
#         headers = {}
#         if "user_agent" in self.config:
#             headers["User-Agent"] = self.config.get("user_agent")
#         # If not using an authenticator, you may also provide inline auth headers:
#         # headers["Private-Token"] = self.config.get("auth_token")
#         return headers

#     def get_next_page_token(
#         self, response: requests.Response, previous_token: Optional[Any]
#     ) -> Optional[Any]:
#         """Return a token for identifying next page or None if no more pages."""
#         # TODO: If pagination is required, return a token which can be used to get the
#         #       next page. If this is the final page, return "None" to end the
#         #       pagination loop.
#         if self.next_page_token_jsonpath:
#             all_matches = extract_jsonpath(
#                 self.next_page_token_jsonpath, response.json()
#             )
#             first_match = next(iter(all_matches), None)
#             next_page_token = first_match
#         else:
#             next_page_token = response.headers.get("X-Next-Page", None)

#         return next_page_token

#     def get_url_params(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> Dict[str, Any]:
#         """Return a dictionary of values to be used in URL parameterization."""
#         params: dict = {}
#         if next_page_token:
#             params["page"] = next_page_token
#         if self.replication_key:
#             params["sort"] = "asc"
#             params["order_by"] = self.replication_key
#         return params

#     def prepare_request_payload(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> Optional[dict]:
#         """Prepare the data payload for the REST API request.

#         By default, no payload will be sent (return None).
#         """
#         # TODO: Delete this method if no payload is required. (Most REST APIs.)
#         return None

#     def parse_response(self, response: requests.Response) -> Iterable[dict]:
#         """Parse the response and return an iterator of result records."""
#         # TODO: Parse response body and return a set of records.
#         yield from extract_jsonpath(self.records_jsonpath, input=response.json())

#     def post_process(self, row: dict, context: Optional[dict]) -> dict:
#         """As needed, append or transform raw data to match expected structure."""
#         # TODO: Delete this method if not needed.
#         return row

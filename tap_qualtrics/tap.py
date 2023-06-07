"""qualtrics tap class."""

from pathlib import Path
from typing import List
import logging
import click
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_qualtrics.streams import (
    Events,
)

PLUGIN_NAME = "tap-qualtrics"

STREAM_TYPES = [ 
    Events,
]

class TapQualtrics(Tap):
    """qualtrics tap class."""

    name = "tap-qualtrics"
    config_jsonschema = th.PropertiesList(
        th.Property("url_base", th.StringType, required=False, description="Url base for the source endpoint"),
        th.Property("api_key", th.StringType, required=False, description="API key"),
        th.Property("app_key", th.StringType, required=False, description="Application key"),
        th.Property("api_token", th.StringType, required=False, description="api token for Basic auth"),
        th.Property("start_date", th.StringType, required=False, description="start date for sync"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams =  [stream_class(tap=self) for stream_class in STREAM_TYPES]

        return streams


# CLI Execution:
cli = TapQualtrics.cli
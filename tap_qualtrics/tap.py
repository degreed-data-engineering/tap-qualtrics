"""qualtrics tap class."""

from pathlib import Path
from typing import List
import logging
import click
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_qualtrics.streams import (
    SurveyResponses,
)

PLUGIN_NAME = "tap-qualtrics"

STREAM_TYPES = [ 
    SurveyResponses,
]

class TapQualtrics(Tap):
    """qualtrics tap class."""

    name = "tap-qualtrics"
    config_jsonschema = th.PropertiesList(
        th.Property("api_token", th.StringType, required=False, description="Qualtrics API token"),
        th.Property("survey", th.StringType, required=False, description="Name of the survey to extract responses from"),
        th.Property("survey_id", th.StringType, required=False, description="Survey ID"),
        th.Property("datacenter", th.StringType, required=False, description="Data center"),
        th.Property("start_date", th.StringType, required=False, description="Date for response extraction to start from"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams =  [stream_class(tap=self) for stream_class in STREAM_TYPES]

        return streams


# CLI Execution:
cli = TapQualtrics.cli
"""Stream class for tap-qualtrics."""

import logging
import sys
import zipfile
import pandas as pd
import io

import base64
import json
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing
from functools import cached_property
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
import requests

from singer_sdk import Tap, Stream

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class TapQualtricsStream(RESTStream):
    """Qualtrics stream class."""
    
    _LOG_REQUEST_METRIC_URLS: bool = True
    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return "https://{0}.qualtrics.com".format(self.config.get("datacenter"))

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["content-type"] = "application/json"

        return headers

    @property
    def authenticator(self):
        http_headers = {}
        if self.config.get("api_token"):
            http_headers["x-api-token"] = self.config.get("api_token")
        
        return SimpleAuthenticator(stream=self, auth_headers=http_headers)

class CXPartnershipSurvey(TapQualtricsStream):
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

    @property
    def path(self) -> str:
        path = "/API/v3/surveys/{}/export-responses/".format(self.config["cx_partnership_survey"])
        return path
    
    name = "cxpartnershipsurvey" # Stream name 
    primary_keys = ["ResponseID"]
    records_jsonpath = "$[*]" # https://jsonpath.com Use requests response json to identify the json path 
    replication_key = None
    rest_method = "POST"
    #schema_filepath = SCHEMAS_DIR / "events.json"  # Optional: use schema_filepath with .json inside schemas/ 

    # Optional: If using schema_filepath, remove the propertyList schema method below
    schema = th.PropertiesList(
        th.Property("StartDate", th.StringType),
        th.Property("EndDate", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("IPAddress", th.StringType),
        th.Property("Progress", th.StringType),
        th.Property("Duration_in_seconds", th.StringType),
        th.Property("Finished", th.StringType),
        th.Property("RecordedDate", th.StringType),
        th.Property("ResponseId", th.StringType),
        th.Property("RecipientLastName", th.StringType),
        th.Property("RecipientFirstName", th.StringType),
        th.Property("RecipientEmail", th.StringType),
        th.Property("ExternalReference", th.StringType),
        th.Property("LocationLatitude", th.StringType),
        th.Property("LocationLongitude", th.StringType),
        th.Property("DistributionChannel", th.StringType),
        th.Property("UserLanguage", th.StringType),
        th.Property("sfContactId", th.StringType),
        th.Property("sfAccountId", th.StringType),
        th.Property("RecipientEmail", th.StringType),
        th.Property("RecipientFirstName", th.StringType),
        th.Property("RecipientLastName", th.StringType),
        th.Property("SurveyID", th.StringType),
        th.Property("ResponseID", th.StringType),
        th.Property("Country", th.StringType),
        th.Property("Survey_Language", th.StringType),
        th.Property("Questions", th.StringType),        
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        payload = {
            "format": "csv",
            # "startDate": "2023-04-20T21:46:30Z", # TODO: add replication value
            "useLabels": True,
        }
        return payload

    def _check_progress(self, row, url):
        row = json.loads(row)
        progressId = row["result"]["progressId"]

        isFile = None

        requestCheckProgress = 0.0
        progressStatus = "inProgress"
        
        headers = {
            "content-type": "application/json",
            "x-api-token": self.config.get("api_token"),
        }
        while progressStatus != "complete" and progressStatus != "failed" and isFile is None:
            if isFile is None:
                print("file not ready")
            else:
                print("progressStatus=", progressStatus)
            requestCheckUrl = url + progressId
            requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers)
            try:
                isFile = requestCheckResponse.json()["result"]["fileId"]
            except KeyError:
                1==1
            print(requestCheckResponse.json())
            requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
            print("Download is " + str(requestCheckProgress) + " complete")
            progressStatus = requestCheckResponse.json()["result"]["status"]

        #step 2.1: Check for error
        if progressStatus is "failed":
            raise Exception("export failed")

        fileId = requestCheckResponse.json()["result"]["fileId"]
        
        return fileId

    def _nest_question_cols(self, row):
        # Initialize two empty dictionaries
        data = {}
        questions = {}
        
        # Loop through each item in the row
        for col, value in row.iteritems():
            # Check if the value is NaN, and if it is, assign None
            if pd.isnull(value):
                value = None

            # If the column starts with 'Q', add it to the 'questions' dictionary
            if str(col).startswith('Q'):
                questions[col] = value
            # Otherwise, add it to the 'data' dictionary
            else:
                data[col] = value
        
        # Add the 'questions' dictionary to the 'data' dictionary
        data['Questions'] = questions
        
        # Return the 'data' dictionary (not converted to JSON)
        return data
    

    def _get_survey_results(self, fileId, url):
        headers = {
            "content-type": "application/json",
            "x-api-token": self.config.get("api_token"),
        }
        requestDownloadUrl = url + fileId + '/file'
        requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)

        # Step 4: Unzipping the file
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall("MyQualtricsDownload")
        print('Complete')

        # Step 4: Load the file into a pandas dataframe
        with zipfile.ZipFile(io.BytesIO(requestDownload.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f, skiprows=[1, 2])

        # Replace spaces in column names with underscores
        df.columns = df.columns.str.replace(r'\(|\)', '', regex=True)
        df.columns = df.columns.str.replace(' ', '_')
        data_dicts = df.apply(self._nest_question_cols, axis=1).tolist()

        return data_dicts

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""

        url = self.url_base + self.path
 
        # Check on Data Export Progress and wait until export is ready
        fileId = self._check_progress(response.text, url)

        # Get the results after report has completed and convert formatted results
        results = self._get_survey_results(fileId, url)

        yield from extract_jsonpath(self.records_jsonpath, input=results)

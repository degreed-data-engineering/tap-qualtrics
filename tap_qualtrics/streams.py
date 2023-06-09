"""Stream class for tap-qualtrics."""
import logging
import os
import sys
import zipfile
import pandas as pd
import io
import time 
from datetime import datetime

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

def chunker(seq, size):
        """Yield chunks of a sequence as lists."""
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
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

class SurveyResponses(TapQualtricsStream):
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

    @property
    def path(self) -> str:
        path = "/API/v3/surveys/{}/export-responses/".format(self.config["survey_id"])
        return path
    
    @property
    def start_time(self) -> str:
        # Get the current date and time
        now = datetime.utcnow()
        # Format the date and time
        formatted_now = now.strftime('%Y-%m-%dT%H:%M:%SZ')
        return formatted_now

    name = "surveyresponses" # Stream name 
    primary_keys = ["ResponseId"]
    records_jsonpath = "$[*]" # https://jsonpath.com Use requests response json to identify the json path 
    replication_key = "survey_export_date"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("SurveyName", th.StringType),
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
        th.Property("Country", th.StringType),
        th.Property("Survey_Language", th.StringType),
        th.Property("Questions", th.StringType),  
        th.Property("survey_export_date", th.StringType),  
        th.Property("OrganizationID", th.StringType),        
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        if "replication_key_value" not in self.stream_state:
            logging.info("Using start_date from config")
            # Convert the date string to a datetime object
            date_obj = datetime.strptime(self.config.get("start_date"), '%Y-%m-%d')

            # Convert the datetime object to a string in the desired format
            formatted_date = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')

            payload = {
                "format": "csv",
                "startDate": formatted_date,
                "useLabels": True,
            }
        else:
            logging.info("Using start date from state")
            payload = {
                "format": "csv",
                "startDate": self.stream_state['replication_key_value'],  
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
                logging.info("file not ready. Checking again in 30 seconds")
            else:
                logging.info("progressStatus=", progressStatus)
            requestCheckUrl = url + progressId
            requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers)
            try:
                isFile = requestCheckResponse.json()["result"]["fileId"]
            except KeyError:
                1==1
            logging.info(requestCheckResponse.json())
            requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
            logging.info("Download is " + str(requestCheckProgress) + " complete")
            progressStatus = requestCheckResponse.json()["result"]["status"]

            # Wait for 30 seconds before the next check 
            time.sleep(30)

        #step 2.1: Check for error
        if progressStatus == "failed":
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
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""

        url = self.url_base + self.path
 
        # Check on Data Export Progress and wait until export is ready
        fileId = self._check_progress(response.text, url)

        # Get the results after report has completed and convert formatted results
        headers = {
            "content-type": "application/json",
            "x-api-token": self.config.get("api_token"),
        }
        requestDownloadUrl = url + fileId + '/file'
        requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)

        # Step 4: Unzipping the file
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall()

        # Create a list to hold all the chunks
        total_chunks = 0
        # Step 4: Load the file into a pandas dataframe in chunks
        with zipfile.ZipFile(io.BytesIO(requestDownload.content)) as z:
            filename = z.namelist()[0]
            with z.open(filename) as f:
                # Set chunksize parameter to specify the number of rows per chunk
                chunksize = 10000
                for chunk in pd.read_csv(f, skiprows=[1, 2], chunksize=chunksize, low_memory=False, dtype={"ColumnName": str}):
                    # Replace spaces in column names with underscores
                    chunk.columns = chunk.columns.str.replace(r'\(|\)', '', regex=True)
                    chunk.columns = chunk.columns.str.replace(' ', '_')
                    chunk.columns = chunk.columns.str.replace(r"[\'\.?!]", '_', regex=True)

                    # Find duplicate columns
                    duplicate_columns = chunk.columns[chunk.columns.duplicated(keep='first')]

                    # Drop the second instance of duplicate columns
                    chunk = chunk.drop(columns=duplicate_columns)

                    # Convert the chunk to a list of dictionaries and add it to the list
                    data_dicts = chunk.apply(self._nest_question_cols, axis=1).tolist()
                    #chunks_list.append(data_dicts)

                    logging.info("Extracting batch of 10000 responses")
                    yield from extract_jsonpath(self.records_jsonpath, input=data_dicts)
                    total_chunks = total_chunks + 10000
                    logging.info(f"Total responses extracted: {total_chunks}")
        
        # delete the file at the end
        try:
            os.remove(filename)
            logging.info(f"Deleted file: {filename}")
        except FileNotFoundError:
            logging.error(f"File not found: {filename}")
        except PermissionError:
            logging.error(f"Permission error while deleting file: {filename}")
        except Exception as e:
            logging.error(f"Unable to delete file {filename}. Reason: {e}")

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["survey_export_date"] = self.start_time     
        row["SurveyName"] = self.config.get("survey")         
        return row
    
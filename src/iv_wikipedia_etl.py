import os
import requests
import duckdb
import sys  # for log deletion
import pandas as pd
import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging
from contextlib import closing


# ____implementing logging for whole process____
class PipelineLogger:
    def __init__(self, log_path='../logs/py_exec.log'):
        self.log_path = log_path
        self.logger = self.setup_logger()

    def setup_logger(self):
        # Delete log file if it exists
        if os.path.exists(self.log_path):
            try:
                for handler in logging.root.handlers[:]:
                    with closing(handler):
                        logging.root.removeHandler(handler)
                os.remove(self.log_path)
            except Exception as e:
                sys.stdout.write(f"Error deleting log file: {str(e)}\n")
                sys.stdout.flush()

        # Initiate logging process - standard templated code
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_path),
                logging.StreamHandler()
            ]
        )

        return logging.getLogger(__name__)


# ____initial variables to be shared through dataclass____
@dataclass
class ApiConfig:
    """
    Wikipedia API parameters: @dataclass is used to pass values to other classes
    refer https://en.wikipedia.org/w/api.php?action=help&modules=query
    in the section of List > recentchanges
    https://en.wikipedia.org/w/api.php?action=help&modules=query%2Brecentchanges

    All default values are based on the specification
    """
    base_url: str = "https://en.wikipedia.org/w/api.php"
    limit: int = 500  # maximum allowed value is 500 per API call
    tumbling_window_size: int = 30  # API tumbling window in seconds


# ____main pipeline class____
class IvWikipediaData:
    """
    Class to handle Wikipedia analytics including API data fetching and processing.

    Attributes:
        config: ApiConfig object containing API configuration
        df: pandas DataFrame to store the collected data
        config: Optional[ApiConfig] = None
    """

    def __init__(self, config):

        # if no configuration is supplied, default values of @dataclass is used
        # highly likely to have smaller tumbling window size
        self.config = config or ApiConfig()

        # empty dataframe to append dataframe of each API call.
        self.df = self._create_empty_dataframe()

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """
        Create empty DataFrame with the schema for Wikipedia edit data.
        This dataframe serves as a base to append into this dataframe,
        with data comes from tumbling window.
        """
        return (pd.DataFrame(columns=[
            'type', 'ns', 'title', 'user', 'userid', 'bot',
            'oldlen', 'newlen', 'timestamp', 'comment',
            'minor', 'anon', 'new'
        ])
        .astype({
            'type': 'string',
            'title': 'string',
            'user': 'string',
            'comment': 'string',
            'ns': 'int64',
            'userid': 'int64',
            'bot': 'bool',
            'oldlen': 'int64',
            'newlen': 'int64',
            'timestamp': 'datetime64[ns]',
            'minor': 'bool',
            'anon': 'bool',
            'new': 'bool'
        })
        )

    def _get_api_params(self, rcstart: str, rcend: str) -> Dict:
        """
        Create a dictionary of an API parameters:
        https://en.wikipedia.org/w/api.php?action=help&modules=query%2Brecentchanges
        rcstart and rcend are passed as argument in each tumbling window
        !!! note rcstart = ending time in business sense, vice verca

        Args:
            rcstart: Start timestamp (upper limit of timeframe - ending timestamp)
            rcend: End timestamp (lower limit of timeframe - starting timestamp)

        Returns:
            Dictionary of API parameters for each API call
        """
        return {
            "action": "query",
            "format": "json",
            "list": "recentchanges",
            "rcstart": rcstart,
            "rcend": rcend,
            "rclimit": self.config.limit,
            "rcprop": "title|timestamp|userid|user|comment|flags|sizes"
        }

    def _fetch_tumbling_window_data(self, start_time: str, end_time: str) -> pd.DataFrame:
        """
        Fetch data for a specific time window.
        Follow the API specification of .json()['query']['recentchanges']

        Args:
            start_time: yyyy-MM-ddTHH:mm:ssZ (confusingly, means ending time)
            end_time: yyyy-MM-ddTHH:mm:ssZ (confusingly means starting time)

        Returns:
            DataFrame containing the fetched data within the tumbling window

        Raises:
            SystemExit: If API returns max records
            or request fails by unspecific reasons
        """
        params = self._get_api_params(end_time, start_time)
        # for verification in log-file, param is recorded
        logger.info(f"Current param is defined as: {params}")

        response = requests.get(self.config.base_url, params=params)

        if response.status_code != 200:
            logger.error(f"API request failed with status code {response.status_code}")
            logger.error(f"Response text: {response.text}")
            raise SystemExit(f'API request failed: {response.text}')

        else:
            edit_records = response.json()['query']['recentchanges']
            window_df = pd.DataFrame(edit_records)
            logger.info(f"Current tumbling window records: {len(window_df)}")

            # if it reached 500, abend the pipeline - tumbling window must be adjusted to smaller frame
            if len(window_df) == self.config.limit:
                raise SystemExit('Error: reached max records for tumbling window')

        return window_df

    def _get_api_data(self, input_date: str) -> pd.DataFrame:
        """
        Collect Wikipedia edit data for a specific day (24 hours) using tumbling windows.

        Args:
            input_date: Date string in format 'YYYY-MM-DD'

        Returns:
            DataFrame containing all data
        """
        target_date = datetime.datetime.strptime(input_date, "%Y-%m-%d").date()
        start_time = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
        end_time = start_time + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)

        current_window_start_time = start_time

        while current_window_start_time < end_time:
            rcend_str = current_window_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(f"Current window start time: {rcend_str}")

            # start time used in calculating ending time as well as next loop
            current_window_start_time += datetime.timedelta(seconds=self.config.tumbling_window_size)

            # the calculated window may overshoot the ending timestamp
            current_window_end_time = min(
                current_window_start_time - datetime.timedelta(seconds=1),
                end_time
            )

            rcstart_str = current_window_end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(f"Current window end time: {rcstart_str}")

            # get dataframe of the tumbling window - note the position of arguments
            window_df = self._fetch_tumbling_window_data(rcend_str, rcstart_str)

            # append dataframe into df
            self.df = pd.concat([self.df, window_df], ignore_index=True)
            logger.info(f"Current total records: {len(self.df)}")

            # delete the temporal df for minimizing memory impact
            del window_df

        return self.df

    def _save_to_duckdb(self, df: pd.DataFrame, database_path: str, table_name: str, schema: str = 'iv') -> None:
        """
        Save collected data to DuckDB - here dropping and re-creating table itself.
        The truncate option will increase code base.

        Args:
            database_path: e.g. data directory
            table_name: e.g. wiki_edit
            schema: e.g. iv
        """
        with duckdb.connect(database_path) as conn:
            # Create schema
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            # full db table name
            full_table_name = f"{schema}.{table_name}"

            # drop table if exists
            conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")

            # create table
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {full_table_name} (
                            type VARCHAR,
                            title VARCHAR,
                            user VARCHAR,
                            userid BIGINT,
                            timestamp TIMESTAMP,
                            comment VARCHAR
                            )
                            """
                         )

            # insert into the table - CTAS was avoided for concern of type not inferred as typed
            conn.execute(f"""
                            INSERT INTO {full_table_name}
                            SELECT type, title, user, userid, timestamp, comment FROM df
                            """)
            logger.info(f"Data saved to {full_table_name}")


if __name__ == "__main__":

    object_logger = PipelineLogger()  # instantiate logger class
    logger = object_logger.logger  # start logging

    logger.info("API python pipeline has started")

    # Initialize with custom config
    config = ApiConfig(
        tumbling_window_size=25,  # default is 30-second tumbling window
    )

    object_ivwikipediadata = IvWikipediaData(config)

    try:
        # extract data via API
        df = object_ivwikipediadata._get_api_data('2024-10-31')

        # Save to DuckDB
        object_ivwikipediadata._save_to_duckdb(
            df=df,
            database_path='..\data\wikipedia.db',
            table_name='wiki_edits'
        )
        logger.info("API python pipeline has successfully completed")

    except Exception as e:
        logger.error(f"Error: {e}")

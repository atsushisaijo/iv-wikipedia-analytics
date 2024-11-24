import os
import requests
import duckdb
import pandas as pd
import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging
from pathlib import Path

# Check if log file exists and delete it
log_file = Path('../logs/py_exec.log')

if log_file.exists():
    try:
        log_file.unlink()  # delete
    except Exception as e:
        pass

# logging is used for batch process - no logger.info() allowed
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('..\logs\py_exec.log'),  # logging to file
        logging.StreamHandler()  # logging to console
    ]
)

logger = logging.getLogger(__name__)


@dataclass
class WikiConfig:
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


class IvWikipediaAnalytics:
    """
    Class to handle Wikipedia analytics including API data fetching and processing.

    Attributes:
        config: WikiConfig object containing API configuration
        df: pandas DataFrame to store the collected data
        config: Optional[WikiConfig] = None
    """

    def __init__(self, config: Optional[WikiConfig] = None):
        # passed constant variables through a dataclass as default
        # if no config is provided, union with WikiConfig object
        self.config = config or WikiConfig()

        # created to concatenate (append) each extracted data through API call.
        self.df = self._create_empty_dataframe()

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with the schema for Wikipedia edit data."""
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
        Create a dictionary as an API parameters.
        rcstart and rcend are passed as argument in each tumbling window

        Args:
            rcstart: Start timestamp (upper limit of timeframe - ending timestamp)
            rcend: End timestamp (lower limit of timeframe - starting timestamp)

        Returns:
            Dictionary of API parameters
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

    def _fetch_window_data(self, start_time: str, end_time: str) -> pd.DataFrame:
        """
        Fetch data for a specific time window.

        Args:
            start_time: yyyy-MM-ddTHH:mm:ssZ (larger than end_time)
            end_time: yyyy-MM-ddTHH:mm:ssZ (smaller than start_time)

        Returns:
            DataFrame containing the fetched data within the tumbling window

        Raises:
            SystemExit: If API returns max records
            or request fails by unspecific reasons
        """
        params = self._get_api_params(end_time, start_time)
        response = requests.get(self.config.base_url, params=params)

        if response.status_code != 200:
            logger.error(f"API request failed with status code {response.status_code}")
            logger.error(f"Response text: {response.text}")
            raise SystemExit(f'API request failed: {response.text}')

        edit_records = response.json()['query']['recentchanges']
        window_df = pd.DataFrame(edit_records)

        if len(window_df) == self.config.limit:
            raise SystemExit('Error: reached max records')

        return window_df

    def get_api_data(self, input_date: str) -> pd.DataFrame:
        """
        Collect Wikipedia edit data for an entire day using tumbling windows.

        Args:
            input_date: Date string in format 'YYYY-MM-DD'

        Returns:
            DataFrame containing all collected data
        """
        target_date = datetime.datetime.strptime(input_date, "%Y-%m-%d").date()
        start_time = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
        end_time = start_time + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)

        current_window_start_time = start_time

        while current_window_start_time < end_time:
            rcend_str = current_window_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(f"Current window start time: {rcend_str}")

            current_window_start_time += datetime.timedelta(seconds=self.config.tumbling_window_size)
            current_window_end_time = min(
                current_window_start_time - datetime.timedelta(seconds=1),
                end_time
            )

            rcstart_str = current_window_end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(f"Current window end time: {rcstart_str}")

            window_df = self._fetch_window_data(rcend_str, rcstart_str)
            self.df = pd.concat([self.df, window_df], ignore_index=True)
            logger.info(f"Current total size: {len(self.df)}")

        return self.df

    def save_to_duckdb(self, df: pd.DataFrame, database_path: str, table_name: str, schema: str = 'iv') -> None:
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

            # Create or replace table
            full_table_name = f"{schema}.{table_name}"
            conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {full_table_name} (
                            type VARCHAR,
                            title VARCHAR,
                            user VARCHAR,
                            userid BIGINT,
                            timestamp TIMESTAMP,
                            comment VARCHAR
                            """
                         )
            conn.execute(f"""
                            INSERT INTO {full_table_name}
                            SELECT type, title, user, userid, timestamp, comment FROM df
                            """)
            logger.info(f"Data saved to {full_table_name}")


if __name__ == "__main__":
    # Initialize with custom config
    config = WikiConfig(
        tumbling_window_size=30,  # 30-second tumbling window
    )

    wiki_analytics = IvWikipediaAnalytics(config)

    try:
        # Collect data for 2024-10-31
        df = wiki_analytics.get_api_data('2024-10-31')

        # Save to DuckDB
        wiki_analytics.save_to_duckdb(
            database_path='wikipedia.db',
            table_name='wiki_edits'
        )

    except Exception as e:
        logger.info(f"Error: {e}")

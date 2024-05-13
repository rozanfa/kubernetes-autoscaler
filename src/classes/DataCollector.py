from kubernetes import client
from src.classes.DB import DB
from src.lib.query import query_all_nodes
import pandas as pd
import logging
import time
from src.lib.config_reader import config

class DataCollector:
    def __init__(self, node_names: str, db: DB):
        self.db = db
        self.node_names = node_names
        self.api_client = client.ApiClient()
        self.previous_data = None
        self.previous_timestamp = None
        self.previous_previous_data = None
        self.previous_previous_timestamp = None


    def collect_data(self, timestamp: int) -> None:
        data, colnames, error_count = query_all_nodes(self.api_client, self.node_names)
        if error_count > 0:
            logging.error(f"Error count: {error_count}")

        cumulative_df = data.to_frame(0).T

        df = cumulative_df.copy()
        print("current timestamp:", timestamp)
        print("previous timestamp:", self.previous_timestamp)
        print("previous previous timestamp:", self.previous_previous_timestamp)

        if self.previous_previous_data is not None:
            # Subtract only the cpu usage of the previous data from the current data
            cpu_cols = [col for col in df.columns if "_cpu" in col]
            df[cpu_cols] = (df[cpu_cols] - self.previous_previous_data[cpu_cols]) / (timestamp - self.previous_previous_timestamp)

            self.db.insert_actual_data(df, timestamp)
            self.db.insert_error_count_data(error_count, timestamp)

            print("Data collected")

        if self.previous_data is not None:
            self.previous_previous_data = self.previous_data
            self.previous_previous_timestamp = self.previous_timestamp

        self.previous_data = cumulative_df
        self.previous_timestamp = timestamp



    def close(self):
        self.db.close()
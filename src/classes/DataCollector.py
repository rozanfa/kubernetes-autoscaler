from kubernetes import client
from src.classes.DB import DB
from src.lib.query import query_all_nodes
import pandas as pd
import logging
import time

class DataCollector:
    def __init__(self, node_names: str, db: DB):
        self.db = db
        self.node_names = node_names
        self.api_client = client.ApiClient()


    def collect_data(self):
        data, colnames, error_count = query_all_nodes(self.api_client, self.node_names)
        if error_count > 0:
            logging.error(f"Error count: {error_count}")

        df = data.to_frame(0).T
        current_time = time.time()
        self.db.insert_data(df, current_time)
        self.db.insert_error_count_data(error_count, current_time)
        print("Data collected")

    def close(self):
        self.db.close()
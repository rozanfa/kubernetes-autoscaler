from src.classes.ConfigManager import ConfigManager
from src.lib.query import query_all_nodes
from kubernetes import client
from src.classes.DB import DB
import pandas as pd
import logging
from typing import Dict
from src.dataclasses.dataclasses import ContainerMetric
from pandas import DataFrame

class DataCollector:
    def __init__(self, node_names: str, db: DB):
        config = ConfigManager.get_config()
        self.db = db
        self.node_names = node_names
        self.api_client = client.ApiClient()
        self.api_instance = client.AppsV1Api(self.api_client)
        self.previous_data = None
        self.previous_timestamp = None
        self.previous_previous_data = None
        self.previous_previous_timestamp = None
        self.namespace = config.namespace
        self.containers = config.containers

    def __get_replicas(self):
        current_deployment_data = self.api_instance.list_namespaced_deployment(self.namespace)
        current_replicas = {
            f"{item.metadata.name}_replicas": item.spec.replicas for item in current_deployment_data.items
        }
        return current_replicas
    
    def __transform_data(self, data: Dict[str, ContainerMetric], timestamp: int) -> DataFrame:
        df = pd.DataFrame(data.values(), columns=["cpu", "memory", "container"]).fillna(0)
        df = df.loc[df["container"].isin(self.containers.keys())]
        df = df.assign(timestamp=timestamp)
        df = df.groupby("container").mean().reset_index()
        df = df.pivot(index="timestamp", columns="container", values=['cpu', 'memory'])
        df.columns = [f'{col[1]}_{col[0]}' for col in df.columns]

        current_replicas = self.__get_replicas()
        df = df.assign(**current_replicas)
        return df

    def collect_data(self, timestamp: int) -> None:
        data, error_count = query_all_nodes(self.api_client, self.node_names)
        if error_count > 0:
            logging.error(f"Error count: {error_count}")

        # print("current timestamp:", timestamp)
        # print("previous timestamp:", self.previous_timestamp)
        # print("previous previous timestamp:", self.previous_previous_timestamp)

        if self.previous_previous_data is not None:
            subtracted_data = {}
            for pod_name in data:
                try:
                    subtracted_data[pod_name] = {
                        "cpu": data[pod_name]["cpu"] - self.previous_previous_data[pod_name]["cpu"],
                        "memory": data[pod_name]["memory"],
                        "container": data[pod_name]["container"],
                    }
                except KeyError:
                    print("KeyError")
                    pass
            
            df = self.__transform_data(subtracted_data, timestamp)

            self.db.insert_actual_data(df, timestamp)
            
        
        if self.previous_data is not None:
            self.previous_previous_data = self.previous_data.copy()
            self.previous_previous_timestamp = self.previous_timestamp

        self.previous_data = data.copy()
        self.previous_timestamp = timestamp


    def close(self):
        self.db.close()
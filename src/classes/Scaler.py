from kubernetes import client
from kubernetes.client.rest import ApiException
from src.classes.ConfigManager import ConfigManager
import pandas as pd
import math
from src.classes.DB import DB
from src.classes.Color import Color

import logging
logger = logging.getLogger(__name__)

class Scaler:
    def __init__(self, db: DB):
        config = ConfigManager.get_config()
        self.db = db
        self.api_client = client.ApiClient()
        self.api_instance = client.AppsV1Api(self.api_client)
        self.containers = config.containers
        self.namespace = config.namespace

    def __adapt_name(self, name: str):
        return name.replace("-", "_")
    
    def __get_replicas(self):
        current_deployment_data = self.api_instance.list_namespaced_deployment(self.namespace)
        return {
            item.metadata.name: item.spec.replicas for item in current_deployment_data.items
        }

    def calculate_and_scale(self, prediction: pd.DataFrame | None, timestamp: int):
        current_replicas = self.__get_replicas()
        self.db.insert_replica_count_data(current_replicas, timestamp)

        if prediction is None:
            return

        for container in self.containers:
            container_key = self.__adapt_name(container)
            if self.containers[container].get("desired_metrics") == None:
                continue

            desired_cpu = self.containers[container]["desired_metrics"].get("cpu")
            desired_replicas_cpu = 1
            if desired_cpu != None:
                desired_replicas_cpu = math.ceil(current_replicas[container] * (prediction[f"{container_key}_cpu"].iloc[0] / self.containers[container]["desired_metrics"]["cpu"]))

            desired_memory = self.containers[container]["desired_metrics"].get("memory")
            desired_replicas_memory = 1
            if desired_memory != None:
                desired_replicas_memory = math.ceil(current_replicas[container] * (prediction[f"{container_key}_memory"].iloc[0] / self.containers[container]["desired_metrics"]["memory"]))

            col_1_1 = f"Desired replicas for {container}:"
            col_1_2 = f"CPU: {desired_replicas_cpu} Memory: {desired_replicas_memory}"
            col_2 = f"Desired CPU: {desired_cpu} Memory: {desired_memory}"
            col_4_1 = f"CPU: {prediction[f'{container_key}_cpu'].iloc[0]:.2f}"
            col_4_2 = f"Memory: {prediction[f'{container_key}_memory'].iloc[0]:.2f}"
            col_5 = f"Current replicas: {current_replicas[container]}"
            print(f"{col_1_1 : <48} {col_1_2 : <20} | {col_2 : <31} | Prediction: {col_4_1 : <15} {col_4_2 : <18} | {col_5}")

            desired_replicas = max(desired_replicas_cpu, desired_replicas_memory)
            desired_replicas = max(desired_replicas, self.containers[container].get("min_replicas"))
            desired_replicas = min(desired_replicas, self.containers[container].get("max_replicas"))

            if current_replicas[container] != desired_replicas:
                if current_replicas[container] < desired_replicas:
                    print(f"{Color.GREEN}=> Upcaling {container} from {current_replicas[container]} to {desired_replicas} {Color.END}")
                elif current_replicas[container] > desired_replicas:
                    print(f"{Color.RED}=> Downscaling {container} from {current_replicas[container]} to {desired_replicas} {Color.END}")
                self.__scale_a_container(container, desired_replicas)
            else:
                pass
        
        print("-" * 175)


    def __scale_a_container(self, container_name: str, desired_replicas: int):
        body = {"spec":{"replicas": desired_replicas}} 
        pretty = 'true'
        try:
            api_response = self.api_instance.patch_namespaced_deployment(container_name, self.namespace, body, pretty=pretty)
        except ApiException as e:
            logger.error("Exception when calling AppsV1Api->patch_namespaced_deployment: %s\n" % e)

    def close(self):
        self.db.close()
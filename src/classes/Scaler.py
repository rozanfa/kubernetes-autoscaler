from kubernetes import client
from kubernetes.client.rest import ApiException
from src.lib.config_reader import config
import pandas as pd
import math

class Scaler:
    def __init__(self):
        self.api_client = client.ApiClient()
        self.api_instance = client.AppsV1Api(self.api_client)
        self.containers = config["containers"]
        self.namespace = config["namespace"]

    def __adapt_name(self, name: str):
        return name.replace("-", "_")

    def calculate_and_scale(self, prediction: pd.DataFrame):
        current_deployment_data = self.api_instance.list_namespaced_deployment(self.namespace)
        current_replicas = {
            item.metadata.name: item.spec.replicas for item in current_deployment_data.items
        }

        for container in self.containers:
            container_key = self.__adapt_name(container)
            if self.containers[container].get("desired_metrics") == None:
                continue

            desired_cpu = self.containers[container]["desired_metrics"].get("cpu")
            desired_replicas_cpu = current_replicas[container]
            if desired_cpu != None:
                desired_replicas_cpu = math.ceil(current_replicas[container] * (prediction[f"{container_key}_cpu"].iloc[0] / self.containers[container]["desired_metrics"]["cpu"]))

            desired_memory = self.containers[container]["desired_metrics"].get("memory")
            desired_replicas_memory = current_replicas[container]
            if desired_memory != None:
                desired_replicas_memory = math.ceil(current_replicas[container] * (prediction[f"{container_key}_memory"].iloc[0] / self.containers[container]["desired_metrics"]["memory"]))

            desired_replicas = max(desired_replicas_cpu, desired_replicas_memory)
            desired_replicas = max(desired_replicas, 1)
            desired_replicas = min(desired_replicas, 5)

            if current_replicas[container] != desired_replicas:
                print("Scaling", container, "from", current_replicas[container], "to", desired_replicas)
                self.__scale_a_container(container, desired_replicas)
            else:
                print("Skipping", container)
        


    def __scale_a_container(self, container_name: str, desired_replicas: int):
        name = container_name 
        namespace = 'social-network'
        body = {"spec":{"replicas": desired_replicas}} 
        pretty = 'true'
        try:
            api_response = self.api_instance.patch_namespaced_deployment(name, namespace, body, pretty=pretty)
            print(api_response)
        except ApiException as e:
            print("Exception when calling AppsV1Api->patch_namespaced_deployment: %s\n" % e)

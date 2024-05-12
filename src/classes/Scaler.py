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
        print("Current replicas:", current_replicas)

        for container in self.containers:
            container_key = self.__adapt_name(container)
            if self.containers[container].get("desired_metrics") == None:
                continue

            desired_cpu = self.containers[container]["desired_metrics"].get("cpu")
            desired_replicas_cpu = 1
            if desired_cpu != None:
                desired_replicas_cpu = math.ceil(current_replicas[container] * (prediction[f"{container_key}_cpu"].iloc[0] / self.containers[container]["desired_metrics"]["cpu"]))
            else:
                print("No desired CPU for", container)

            desired_memory = self.containers[container]["desired_metrics"].get("memory")
            desired_replicas_memory = 1
            if desired_memory != None:
                desired_replicas_memory = math.ceil(current_replicas[container] * (prediction[f"{container_key}_memory"].iloc[0] / self.containers[container]["desired_metrics"]["memory"]))
            else:
                print("No desired memory for", container)

            print("Desired replicas for", container, "CPU:", desired_replicas_cpu, "Memory:", desired_replicas_memory, "|", "Desired CPU:", desired_cpu, "Memory:", desired_memory, "|", "Prediction:", prediction[f"{container_key}_cpu"].iloc[0], prediction[f"{container_key}_memory"].iloc[0], "|", "Current replicas:", current_replicas[container])
            desired_replicas = max(desired_replicas_cpu, desired_replicas_memory)
            desired_replicas = max(desired_replicas, self.containers[container].get("min_replicas"))
            desired_replicas = min(desired_replicas, self.containers[container].get("max_replicas"))

            if current_replicas[container] != desired_replicas:
                print("=> Scaling", container, "from", current_replicas[container], "to", desired_replicas)
                self.__scale_a_container(container, desired_replicas)
            else:
                print("Skipping", container)
        


    def __scale_a_container(self, container_name: str, desired_replicas: int):
        name = container_name 
        namespace = self.namespace
        body = {"spec":{"replicas": desired_replicas}} 
        pretty = 'true'
        try:
            api_response = self.api_instance.patch_namespaced_deployment(name, namespace, body, pretty=pretty)
        except ApiException as e:
            print("Exception when calling AppsV1Api->patch_namespaced_deployment: %s\n" % e)

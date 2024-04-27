from typing import Dict
from src.dataclasses.dataclasses import PodMetric, ContainerMetric, NodeMetric
import time
import pandas as pd
from src.lib.config_reader import config

def process_pod_or_container(row: list[str], pods: Dict[str, PodMetric], containers: Dict[str, ContainerMetric]):
    raw_name = row[0]
    value = row[1]
    timestamp = row[2]
    raw_type = raw_name.split("{")[0]
    pod = raw_name.split("\"")[-2].split("\"")[-1]
    namespace = raw_name.split("\"")[-4].split("\"")[-1]
    container = raw_name.split("\"")[1].split("\"")[0]
    match raw_type:
        case "container_cpu_usage_seconds_total":
            try:
                containers[pod].cpu = float(value) / 10
                containers[pod].cpu_timestamp = int(timestamp)
            except KeyError:
                containers[pod] = ContainerMetric(namespace=namespace, pod=pod, container=container)
                containers[pod].cpu = float(value) / 10
                containers[pod].cpu_timestamp = int(timestamp)
        case "container_memory_working_set_bytes":
            try:
                containers[pod].memory = float(value) / 1024 / 1024
                containers[pod].memory_timestamp = int(timestamp)
            except KeyError:
                containers[pod] = ContainerMetric(namespace=namespace, pod=pod, container=container)
                containers[pod].memory = float(value) / 1024 / 1024
                containers[pod].memory_timestamp = int(timestamp)
        case "pod_memory_working_set_bytes":
            try:
                pods[pod].memory = float(value) / 1024 / 1024
                pods[pod].memory_timestamp = int(timestamp)
            except KeyError:
                pods[pod] = PodMetric(namespace=namespace, pod=pod)
                pods[pod].memory = float(value) / 1024 / 1024
                pods[pod].memory_timestamp = int(timestamp)
        case "pod_cpu_usage_seconds_total":
            try:
                pods[pod].cpu = float(value) / 10
                pods[pod].cpu_timestamp = int(timestamp)
            except KeyError:
                pods[pod] = PodMetric(namespace=namespace, pod=pod)
                pods[pod].cpu = float(value) / 10
                pods[pod].cpu_timestamp = int(timestamp)


def process_node(row: list[str], nodes: Dict[str, NodeMetric], node_name: str):
    raw_type = row[0]
    value = row[1]
    timestamp = row[2]
    match raw_type:
        case "node_memory_working_set_bytes":
            try:
                nodes[node_name].memory = float(value) 
                nodes[node_name].memory_timestamp = int(timestamp) 

            except KeyError:
                nodes[node_name] = NodeMetric(name=node_name)
                nodes[node_name].memory = float(value)
                nodes[node_name].memory_timestamp = int(timestamp)
        case "node_cpu_usage_seconds_total":
            try:
                nodes[node_name].cpu = float(value) 
                nodes[node_name].cpu_timestamp = int(timestamp) 
            except KeyError:
                nodes[node_name] = NodeMetric(name=node_name)
                nodes[node_name].cpu = float(value)
                nodes[node_name].cpu_timestamp = int(timestamp)


def transform_data(containers: Dict[str, ContainerMetric]):
    data = pd.DataFrame(containers.values(), columns=ContainerMetric.__annotations__.keys())
    data = data.loc[data["container"].isin(config["containers"].keys())]
    data["timestamp"] = int(time.time())
    data.drop(["pod", "namespace"], axis=1, inplace=True)
    data = data.groupby("container").sum().reset_index()
    data = data.pivot(index="timestamp", columns="container", values=['cpu', 'memory'])
    data.columns = [f'{col[1]}_{col[0]}' for col in data.columns]
    return data
        
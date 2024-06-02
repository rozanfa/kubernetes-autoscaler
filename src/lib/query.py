from src.lib.process import process_pod_or_container, process_node, transform_data
import pandas as pd
from multiprocessing.pool import ThreadPool
from kubernetes import client

def query_a_node(api_client: client.ApiClient, node_name: str) -> tuple[pd.DataFrame, int]: 
    print("Doing stuff...")

    # Create a dictionary with the data
    pod_data = {}
    container_data = {}
    node_data = {}
    error_count = 0

    try:
        response = api_client.call_api(f"/api/v1/nodes/{node_name}/proxy/metrics/resource", "GET", auth_settings=["BearerToken"], response_type="str")
        data = str(response[0])
        rows = data.split("\n")
    except Exception as e:
        print(f"Error[{node_name}]: {e}")
        return pd.DataFrame(), 1

    # Remove comments and empty lines
    rows = [row for row in rows if not row.startswith("#") and row.strip() != ""]

    # Split lines from rows
    rows = [row.split() for row in rows]

    print("Processing data...")
    for row in rows:
        try:
            if row[0] == "scrape_error":
                error_count = int(row[1])
            elif row[0] == "resource_scrape_error":
                continue
            elif row[0][:28] == "container_start_time_seconds":
                continue
            elif row[0][:4] == "node":
                process_node(row, node_data, node_name)
            else:
                process_pod_or_container(row, pod_data, container_data)
                
        except IndexError:
            print("Error:",row)
    transformed_container_data = transform_data(container_data)
    return transformed_container_data, error_count


def query_all_nodes(api_client: client.ApiClient, node_names: list[str]) -> tuple[dict, int]:
    pool = ThreadPool(processes=len(node_names))
    pool_results: list[ThreadPool] = []
    result = dict()
    error_count = 0
    for node_name in node_names:
        print("Querying node:", node_name)
        pool_results.append(pool.apply_async(query_a_node, ([api_client, node_name])))

    for pool_result in pool_results:
        transformed_container_data, new_error_count = pool_result.get()
        result.update(transformed_container_data)
        error_count += new_error_count
    return result, error_count
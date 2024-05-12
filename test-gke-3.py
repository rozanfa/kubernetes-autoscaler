from kubernetes import config, client
from google.auth import compute_engine

# Connect to GKE
config.load_kube_config()
v1 = client.CoreV1Api()

# Get all nodes
ret = v1.list_node()
node_names = [node.metadata.name for node in ret.items]


api_client = client.ApiClient()
# response = api_client.list_namespaced_pod("social-network")
response = api_client.call_api(f"/api/v1/nodes/{node_names[0]}/proxy/metrics/resource", "GET", auth_settings = ['BearerToken'], response_type="str")
print(response)

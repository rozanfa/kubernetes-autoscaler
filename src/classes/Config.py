import json
import sys
from dataclasses import dataclass

@dataclass
class Metric:
    cpu: int
    memory: int

@dataclass
class ContainerConfig:
    desired_metric: Metric
    min_replicas: int
    max_replicas: int

@dataclass
class Config:
    containers: dict[str, ContainerConfig]
    namespace: str
    n_steps: int
    periode: int
    model_path: str
    scaler_path: str

def load_config(path: str) -> Config:
    with open(path, "r") as f:
        config = json.load(f)
        return Config(
            containers=config["containers"],
            namespace=config["namespace"],
            n_steps=config["n_steps"],
            periode=config["periode"],
            model_path=config["model_path"],
            scaler_path=config["scaler_path"]
        )

if __name__ == "__main__":
    config = load_config(sys.argv[1])
    print(config)
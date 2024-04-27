from tensorflow.keras.models import load_model
from src.lib.query import query_all_nodes
from kubernetes import config as kubernetes_config, client as kubernetes_client
from src.classes.DataCollector import DataCollector
from src.classes.LoggingPool import LoggingPool
from src.classes.Predictor import Predictor
from src.lib.config_reader import config
from src.classes.DB import DB
import logging
import joblib
import sched
import time

def get_node_names() -> list[str]:
    v1 = kubernetes_client.CoreV1Api()
    ret = v1.list_node()
    node_names = [node.metadata.name for node in ret.items]
    return node_names

def main_loop(scheduler: sched.scheduler, collector: DataCollector, predictor: Predictor, periode: int):
    scheduler.enter(periode, 1, main_loop, (scheduler, collector, predictor, periode))

    collector.collect_data()
    res = predictor.predict()
    print(res)

def main():
    kubernetes_config.load_kube_config()
    db = DB()
    db.create_tables()
    node_names = get_node_names()

    collector = DataCollector(node_names, db)

    model = load_model("models/autoscaler_1_60.keras")
    scaler = joblib.load("models/scaler.pkl")
    predictor = Predictor(["minikube"], model, scaler, db)

    pool = LoggingPool()
    scheduler = sched.scheduler(time.time, time.sleep)
    periode = config.get("periode", 10)
    try :
        scheduler.enter(0, 1, main_loop, (scheduler, collector, predictor, periode))
        print("Starting scheduler")
        scheduler.run()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        db.close()


if __name__ == "__main__":
    main()



from tensorflow.keras.models import load_model
from kubernetes import config as kubernetes_config, client as kubernetes_client
from src.classes.DataCollector import DataCollector
from src.classes.LoggingPool import LoggingPool
from src.classes.Predictor import Predictor
from src.classes.Scaler import Scaler
from src.lib.config_reader import config
from src.classes.DB import DB
import joblib
import sched
import time

def get_node_names() -> list[str]:
    v1 = kubernetes_client.CoreV1Api()
    ret = v1.list_node()
    node_names = [node.metadata.name for node in ret.items]
    return node_names

def main_loop(scheduler: sched.scheduler, collector: DataCollector, predictor: Predictor, scaler: Scaler, periode: int):
    scheduler.enter(periode, 1, main_loop, (scheduler, collector, predictor, scaler, periode))

    timestamp = time.time()
    collector.collect_data(timestamp)
    predicted_data = predictor.predict(timestamp)
    print("Predicted data:", predicted_data)
    scaler.calculate_and_scale(predicted_data, timestamp)

    # if predicted_data is not None:
    #     scaler.calculate_and_scale(predicted_data, timestamp)

def main():
    kubernetes_config.load_kube_config()
    db = DB()
    db.create_tables()
    node_names = get_node_names()

    collector = DataCollector(node_names, db)

    print("Using model:", config["model_path"])
    model = load_model(config["model_path"])
    minMaxScaler = joblib.load(config["scaler_path"])
    predictor = Predictor(model, minMaxScaler, db)

    scaler = Scaler(db)

    scheduler = sched.scheduler(time.time, time.sleep)
    periode = config.get("periode", 10)
    try :
        scheduler.enter(0, 1, main_loop, (scheduler, collector, predictor, scaler, periode))
        print("Starting scheduler")
        scheduler.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        db.close()


if __name__ == "__main__":
    main()



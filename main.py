from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
from src.classes.DB import connect, get_data
import pandas as pd
import numpy as np
from src.lib.query import query_all_nodes
from kubernetes import config as kubernetes_config, client as kubernetes_client
from src.lib.config_reader import config
import joblib
from src.classes.DB import DB


N_STEP = 60
scaler = joblib.load("models/scaler.pkl")

def get_node_names() -> list[str]:
    v1 = kubernetes_client.CoreV1Api()
    ret = v1.list_node()
    node_names = [node.metadata.name for node in ret.items]
    return node_names

def preprocess_data(raw_data):
    training_set_scaled = scaler.inverse_transform(raw_data)
    return training_set_scaled

def predict(model, sequence_data):
    kubernetes_config.load_kube_config()
    node_names = get_node_names()
    raw_new_data, colnames, error_count = query_all_nodes(node_names)
    if error_count > 0:
        print(f"Error count: {error_count}")

    new_data = raw_new_data.to_numpy()
    sequence_data.append(new_data)
    if len(sequence_data) < N_STEP:
        sequence_data = [sequence_data[0] for _ in range(N_STEP - len(sequence_data))] + sequence_data
    elif len(sequence_data) > N_STEP:
        sequence_data = sequence_data[-N_STEP:]
        
    sequence_data_to_predict = preprocess_data(np.array(sequence_data))
    sequence_data_to_predict = sequence_data_to_predict.reshape(1, N_STEP, len(new_data))
    prediction = model.predict(sequence_data_to_predict)
    prediction = scaler.inverse_transform(prediction)
    prediction = pd.DataFrame(prediction, columns=colnames)


def main():
    db = DB()
    db.create_tables()

    model = load_model("models/model.h5")


if __name__ == "__main__":
    main()



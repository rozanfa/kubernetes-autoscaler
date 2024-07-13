from src.classes.ConfigManager import ConfigManager
from src.classes.DB import DB
import pandas as pd
import numpy as np

class Predictor:
    def __init__(self, model, scaler, db: DB):
        config = ConfigManager.get_config()
        self.containers = config.containers
        self.model = model
        self.scaler = scaler
        self.db = db
        self.n_steps = config.n_steps
        self.periode = config.periode
        self.model_type = config.model_type

    def predict(self, timestamp: int) -> pd.DataFrame | None:
        data, colnames = self.db.get_data(limit=self.n_steps)
        # print("Data:", data)
        print("Data length:", len(data))

        if len(data) < self.n_steps:
            print("Not enough data to predict")
            return None
        
        data = pd.DataFrame(data, columns=colnames).astype(float).drop(["id", "timestamp"], axis=1)

        transformed_data = self.scaler.transform(data)
        transformed_data = transformed_data.reshape(1, self.n_steps, len(self.containers) * 3)
        prediction = self.model.predict(transformed_data)

        if self.model_type == "a":
            prediction = self.scaler.inverse_transform(prediction)
        elif self.model_type == "b":
            transformed_prediction = np.array([0 for _ in range(len(self.containers) * 3)], dtype=float)
            k = 0
            for j in range(len(transformed_prediction)):
                if (j+1) % 3 != 0:
                    transformed_prediction[j] = prediction[0][k]
                    k += 1

            prediction = self.scaler.inverse_transform(transformed_prediction.reshape(1, len(transformed_prediction)))

        prediction = pd.DataFrame(prediction, columns=data.columns)

        if self.model_type == "a":
            prediction.drop([col for col in prediction.columns if col.endswith("_replicas")], axis=1, inplace=True)

        prediction.clip(lower=0, inplace=True)
        self.__save_to_db(prediction, timestamp + self.periode)
        return prediction

    def __save_to_db(self, prediction: pd.DataFrame, timestamp: int):
        self.db.insert_predicted_data(prediction, timestamp)
    
    def close(self):
        self.db.close()
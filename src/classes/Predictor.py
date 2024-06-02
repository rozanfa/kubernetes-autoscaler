from src.classes.ConfigManager import ConfigManager
from src.classes.DB import DB
import pandas as pd

class Predictor:
    def __init__(self, model, scaler, db: DB):
        config = ConfigManager.get_config()
        self.container = config.containers
        self.model = model
        self.scaler = scaler
        self.db = db
        self.n_steps = config.n_steps
        self.periode = config.periode

    def predict(self, timestamp: float) -> pd.DataFrame | None:
        data, colnames = self.db.get_data(limit=self.n_steps)
        # print("Data:", data)
        print("Data length:", len(data))

        if len(data) < self.n_steps:
            print("Not enough data to predict")
            return None
        
        data = pd.DataFrame(data, columns=colnames)
        data = data.astype(float)
        data.drop(["id", "timestamp"], axis=1, inplace=True)

        transformed_data = self.scaler.transform(data)
        transformed_data = transformed_data.reshape(1, self.n_steps, len(self.containers) * 3)
        prediction = self.model.predict(transformed_data)
        prediction = self.scaler.inverse_transform(prediction)
        prediction = pd.DataFrame(prediction, columns=data.columns)
        prediction = prediction.clip(lower=0)
        self.__save_to_db(prediction, timestamp + self.periode)
        return prediction

    def __save_to_db(self, prediction: pd.DataFrame, timestamp: float):
        self.db.insert_predicted_data(prediction, timestamp)
    
    def close(self):
        self.db.close()
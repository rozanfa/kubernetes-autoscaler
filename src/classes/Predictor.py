from src.lib.config_reader import config
from src.classes.DB import DB
import pandas as pd
import logging

class Predictor:
    def __init__(self, node_names: list[str], model, scaler, db: DB):
        self.node_namess = node_names
        self.model = model
        self.scaler = scaler
        self.db = db
        self.n_steps = config.get("n_steps")

    def predict(self):
        data, colnames = self.db.get_data(limit=self.n_steps)

        if len(data) < 60:
            logging.info("Not enough data to predict")
            return
        
        data = pd.DataFrame(data, columns=colnames)
        data = data.astype(float)
        data.drop(["id", "timestamp"], axis=1, inplace=True)

        transformed_data = self.scaler.transform(data)
        transformed_data = transformed_data.reshape(1, self.n_steps, len(config["containers"]) * 2)
        prediction = self.model.predict(transformed_data)
        prediction = self.scaler.inverse_transform(prediction)
        prediction = pd.DataFrame(prediction, columns=data.columns)
        return prediction

        
    # def scale
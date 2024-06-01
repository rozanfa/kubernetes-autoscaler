from src.classes.Config import Config, load_config

class ConfigManager:
    __instance: Config = None

    @staticmethod
    def get_config() -> Config:
        return ConfigManager.__instance

    @staticmethod
    def load_config(file_path: str) -> None:
        ConfigManager.__instance = load_config(file_path)

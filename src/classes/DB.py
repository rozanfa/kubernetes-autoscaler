import psycopg2
from dotenv import load_dotenv
import os
from typing import Dict
from src.dataclasses.dataclasses import PodMetric, ContainerMetric, NodeMetric
from psycopg2.extensions import connection
from src.classes.ConfigManager import ConfigManager
from pandas import DataFrame
from typing import Tuple, List

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

class DB:
    def __init__(self, conn: connection = None):
        if conn is not None:
            self.__conn = conn
        else:
            self.__conn = self.__connect()
        
    def __connect(self):
        return psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host=localhost")

    def __to_metric_table_creation_command(self, container: str):
        return f"""
            {self.__adapt_name(container)}_cpu DOUBLE PRECISION,
            {self.__adapt_name(container)}_memory DOUBLE PRECISION,
            {self.__adapt_name(container)}_replicas INTEGER,
        """
    
    def __to_replica_table_creation_command(self, container: str):
        return f"""
            {self.__adapt_name(container)} INTEGER,
        """
    

    def __adapt_name(self, name: str):
        return name.replace("-", "_")

    def create_tables(self):
        config = ConfigManager.get_config()
        cur = self.__conn.cursor()
        metric_table_commands = "\n".join(list(map(self.__to_metric_table_creation_command, config.containers.keys())))
        replica_table_commands = "\n".join(list(map(self.__to_replica_table_creation_command, config.containers.keys())))

        create_actual_data_table_command = f"""
        CREATE TABLE IF NOT EXISTS {self.__adapt_name(config.namespace)} (
            id SERIAL PRIMARY KEY,
            {metric_table_commands}
            timestamp BIGINT
        );
        """

        cur.execute(create_actual_data_table_command)

        create_predicted_data_table_command = f"""
        CREATE TABLE IF NOT EXISTS {self.__adapt_name(config.namespace)}_predicted (
            id SERIAL PRIMARY KEY,
            {metric_table_commands}
            timestamp BIGINT
        );
        """

        cur.execute(create_predicted_data_table_command)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.__adapt_name(config.namespace)}_error_count (
                id SERIAL PRIMARY KEY,
                error_count INT,
                insert_timestamp BIGINT
            )
        """)

        metric_table_commands = "\n".join(list(map(self.__to_metric_table_creation_command, config.containers.keys())))
        create_replica_table_command = f"""
        CREATE TABLE IF NOT EXISTS {self.__adapt_name(config.namespace)}_replicas (
            id SERIAL PRIMARY KEY,
            {replica_table_commands}
            timestamp BIGINT
        );
        """
        cur.execute(create_replica_table_command)


        self.__conn.commit()
        cur.close()

    def insert_actual_data(self, data: DataFrame, timestamp: float):
        config = ConfigManager.get_config()
        cur = self.__conn.cursor()
        try:
            command = f"""
            INSERT INTO {self.__adapt_name(config.namespace)} ({", ".join(list(map(self.__adapt_name, data.columns)))}, timestamp) VALUES ({", ".join(data.values[0].astype(str))}, {timestamp});
            """
            cur.execute(command)
            self.__conn.commit()
        except Exception as e:
            print("Error:", e)
            print("df:", data)
            print("Columns:", list(map(self.__adapt_name, data.columns)))
            print("Data:", data.values[0].astype(str))
            print("Command:", command)
            raise e
        cur.close()

    def insert_predicted_data(self, data: DataFrame, timestamp: float):
        config = ConfigManager.get_config()
        cur = self.__conn.cursor()
        command = f"""
        INSERT INTO {self.__adapt_name(config.namespace)}_predicted ({", ".join(list(map(self.__adapt_name, data.columns)))}, timestamp) VALUES ({", ".join(data.values[0].astype(str))}, {timestamp});
        """
        cur.execute(command)
        self.__conn.commit()
        cur.close()


    def insert_error_count_data(self, error_count: int, timestamp: float):
        config = ConfigManager.get_config()
        cur = self.__conn.cursor()

        cur.execute(f"INSERT INTO {self.__adapt_name(config.namespace)}_error_count (error_count, insert_timestamp) VALUES (%s, %s)", (error_count, timestamp))

        self.__conn.commit()
        cur.close()

    def insert_replica_count_data(self, data: Dict[str, int], timestamp: float):
        config = ConfigManager.get_config()
        cur = self.__conn.cursor()
        command = f"""
        INSERT INTO {self.__adapt_name(config.namespace)}_replicas ({", ".join(list(map(self.__adapt_name, data.keys())))}, timestamp) VALUES ({", ".join(map(str, list(data.values())))}, {timestamp});
        """
        cur.execute(command)
        self.__conn.commit()
        cur.close()
        
    def get_data(self, limit: int = None):
        config = ConfigManager.get_config()
        table = self.__adapt_name(config.namespace)
        cur = self.__conn.cursor()
        if limit is not None:
            cur.execute(f"SELECT * FROM (SELECT * FROM {table} ORDER BY timestamp DESC LIMIT {limit}) a ORDER BY timestamp ASC")
        else:
            cur.execute(f"SELECT * FROM {table}")
        data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        cur.close()
        return data, column_names

    def get_error_count_data(self, limit: int = None) -> Tuple[Tuple, List[str]]:
        cur = self.__conn.cursor()
        if limit is not None:
            cur.execute(f"SELECT * FROM (SELECT * FROM error_count ORDER BY timestamp DESC LIMIT {limit}) a ORDER BY timestamp ASC")
        else:
            cur.execute("SELECT * FROM error_count")
        data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        cur.close()
        return data, column_names
    
    def close(self):
        self.__conn.close()


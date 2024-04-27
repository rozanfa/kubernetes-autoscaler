import psycopg2
from dotenv import load_dotenv
import os
from typing import Dict
from src.dataclasses.dataclasses import PodMetric, ContainerMetric, NodeMetric
from psycopg2.extensions import connection
from src.lib.config_reader import config
from pandas import DataFrame
from typing import Tuple, List

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

class DB:
    def __init__(self, conn: connection = None):
        if conn is not None:
            self.conn = conn
        else:
            self.__conn = self.__connect()
        
    def __connect(self):
        return psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host=localhost")

    def __to_table_creation_command(self, container: str):
        return f"""
            {self.__adapt_name(container)}_cpu DOUBLE PRECISION,
            {self.__adapt_name(container)}_memory DOUBLE PRECISION,
        """

    def __adapt_name(self, name: str):
        return name.replace("-", "_")

    def create_tables(self):
        cur = self.__conn.cursor()
        table_commands = "\n".join(list(map(self.__to_table_creation_command, config["containers"].keys())))

        command = f"""
        CREATE TABLE IF NOT EXISTS {self.__adapt_name(config["namespace"])} (
            id SERIAL PRIMARY KEY,
            {table_commands}
            timestamp BIGINT
        );
        """

        cur.execute(command)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS error_count (
                id SERIAL PRIMARY KEY,
                error_count INT,
                insert_timestamp BIGINT
            )
        """)


        self.__conn.commit()
        cur.close()

    def insert_data(self, data: DataFrame, timestamp: float):
        cur = self.__conn.cursor()
        command = f"""
        INSERT INTO {self.__adapt_name(config["namespace"])} ({", ".join(list(map(self.__adapt_name, data.columns)))}, timestamp) VALUES ({", ".join(data.values[0].astype(str))}, {timestamp});
        """
        cur.execute(command)
        self.__conn.commit()
        cur.close()


    def insert_error_count_data(self, error_count: int, timestamp: float):
        cur = self.__conn.cursor()

        cur.execute("INSERT INTO error_count (error_count, insert_timestamp) VALUES (%s, %s)", (error_count, timestamp))

        self.__conn.commit()
        cur.close()

    def get_data(self, limit: int = None):
        table = self.__adapt_name(config["namespace"])
        cur = self.__conn.cursor()
        if limit is not None:
            cur.execute(f"SELECT * FROM {table} LIMIT {limit}")
        else:
            cur.execute(f"SELECT * FROM {table}")
        data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        cur.close()
        return data, column_names

    def get_error_count_data(self, limit: int = None) -> Tuple[Tuple, List[str]]:
        cur = self.__conn.cursor()
        if limit is not None:
            cur.execute(f"SELECT * FROM error_count LIMIT {limit}")
        else:
            cur.execute("SELECT * FROM error_count")
        data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        cur.close()
        return data, column_names
    
    def close(self):
        self.__conn.close()

if __name__ == "__main__":
    db = DB()
    db.create_tables()
    
    db.insert_batch_pod_data({"pod1": PodMetric("pod1", "namespace1", 1.0, 1.0, 1.0, 1.0)})
    db.insert_batch_container_data({"container1": ContainerMetric("container1", "namespace1", 1.0, 1.0, 1.0, 1.0)})
    db.insert_batch_node_data({"node1": NodeMetric("node1", 1.0, 1.0, 1.0, 1.0)})
    db.insert_error_count_data(1, 1.0)

    db.close

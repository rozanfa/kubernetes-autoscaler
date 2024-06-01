import psycopg2
from dotenv import load_dotenv
import os
from typing import Dict
from dataclass import PodMetric, ContainerMetric, NodeMetric
from psycopg2.extensions import connection
from config_reader import config
from pandas import DataFrame
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def connect():
    return psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host=localhost")

def to_table_creation_command(container: str):
    return f"""
        {adapt_name(container)}_cpu DOUBLE PRECISION,
        {adapt_name(container)}_memory DOUBLE PRECISION,
    """

def adapt_name(namespace: str):
    return namespace.replace("-", "_")

def create_tables():
    conn = connect()
    cur = conn.cursor()

    table_commands = "\n".join(list(map(to_table_creation_command, config["containers"].keys())))

    command = f"""
    CREATE TABLE IF NOT EXISTS {adapt_name(config["namespace"])} (
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


    conn.commit()
    cur.close()
    conn.close()

def insert_data(conn: connection, data: DataFrame, timestamp: float):
    cur = conn.cursor()
    command = f"""
    INSERT INTO {adapt_name(config["namespace"])} ({", ".join(list(map(adapt_name, data.columns)))}, timestamp) VALUES ({", ".join(data.values[0])}, {timestamp});
    """
    cur.execute(command)
    conn.commit()
    cur.close()


def insert_batch_pod_data(conn: connection, pod_data: Dict[str, PodMetric], timestamp: float):
    cur = conn.cursor()
    for pod in pod_data.values():
        cur.execute("INSERT INTO pods (pod_name, namespace, cpu_usage, memory_usage, cpu_timestamp, memory_timestamp, insert_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)", (pod.pod, pod.namespace, pod.cpu, pod.memory, pod.cpu_timestamp, pod.memory_timestamp, timestamp))
    conn.commit()
    cur.close()


def insert_batch_container_data(conn: connection, container_data: Dict[str, ContainerMetric], timestamp: float):
    cur = conn.cursor()
    for container in container_data.values():
        cur.execute("INSERT INTO containers (pod_name, container_name, namespace, cpu_usage, memory_usage, cpu_timestamp, memory_timestamp, insert_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (container.pod, container.container, container.namespace, container.cpu, container.memory, container.cpu_timestamp, container.memory_timestamp, timestamp))

    conn.commit()
    cur.close()

def insert_batch_node_data(conn: connection, node_data: Dict[str, NodeMetric], timestamp: float):
    cur = conn.cursor()

    for node in node_data.values():
        cur.execute("INSERT INTO nodes (name, cpu_usage, memory_usage, cpu_timestamp, memory_timestamp, insert_timestamp) VALUES (%s, %s, %s, %s, %s, %s)", (node.name, node.cpu, node.memory, node.cpu_timestamp, node.memory_timestamp, timestamp))

    conn.commit()
    cur.close()


def insert_error_count_data(conn: connection, error_count: int, timestamp: float):
    cur = conn.cursor()

    cur.execute("INSERT INTO error_count (error_count, insert_timestamp) VALUES (%s, %s)", (error_count, timestamp))

    conn.commit()
    cur.close()

def get_data(conn: connection, limit: int = None):
    table = adapt_name(config["namespace"])
    cur = conn.cursor()
    if limit is not None:
        cur.execute(f"SELECT * FROM {table} LIMIT {limit}")
    else:
        cur.execute(f"SELECT * FROM {table}")
    data = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    cur.close()
    return data, column_names

def get_error_count_data(conn: connection, limit: int = None):
    cur = conn.cursor()
    if limit is not None:
        cur.execute(f"SELECT * FROM error_count LIMIT {limit}")
    else:
        cur.execute("SELECT * FROM error_count")
    data = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    cur.close()
    return data, column_names

if __name__ == "__main__":
    create_tables()
    conn = connect()
    
    insert_batch_pod_data(conn, {"pod1": PodMetric("pod1", "namespace1", 1.0, 1.0, 1.0, 1.0)})
    insert_batch_container_data(conn, {"container1": ContainerMetric("container1", "namespace1", 1.0, 1.0, 1.0, 1.0)})
    insert_batch_node_data(conn, {"node1": NodeMetric("node1", 1.0, 1.0, 1.0, 1.0)})
    insert_error_count_data(1, 1.0)

    conn.close()

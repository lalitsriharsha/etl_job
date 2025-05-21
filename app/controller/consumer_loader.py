import asyncio
import json
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from app.utils.kafka_helper import get_kafka_consumer
from app.config.config import settings

TOPIC = settings.transformed_topic

def create_cassandra_table():
    cluster = Cluster()
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {settings.cassandra_keyspace}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    """)
    session.set_keyspace(settings.cassandra_keyspace)

    session.execute("""
        CREATE TABLE IF NOT EXISTS etl_transformed (
            pid int PRIMARY KEY,
            fname text,
            lname text,
            dob date,
            email text,
            gender text,
            cell text,
            city text,
            apartment text,
            street text,
            country text,
            pcode int,
            gestage float
        );
    """)
    return session

def insert_into_cassandra(session, table_name, data_row):
    if not isinstance(data_row, dict):
        raise TypeError(f"Expected dict, got {type(data_row)}: {data_row}")

    columns = list(data_row.keys())
    if len(columns) != len(set(columns)):
        raise ValueError(f"Duplicate column names detected in row: {columns}")

    placeholders = ', '.join(['%s'] * len(columns))
    column_names = ', '.join(columns)
    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    session.execute(SimpleStatement(query), list(data_row.values()))

async def consume_and_load(timeout_seconds=10):
    print(" Starting Kafka consumer to load transformed data into Cassandra...")

    consumer = await get_kafka_consumer(TOPIC)
    session = create_cassandra_table()
    table_name = "etl_transformed"
    count = 0
    idle_seconds = 0

    try:
        while True:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=1.0)
                idle_seconds = 0  
            except asyncio.TimeoutError:
                idle_seconds += 1
                if idle_seconds >= timeout_seconds:
                    print(f"No new messages in {timeout_seconds} seconds. Stopping consumer.")
                    break
                continue

            try:
                transformed = json.loads(msg.value.decode("utf-8"))

                if isinstance(transformed, dict):
                    insert_into_cassandra(session, table_name, transformed)
                    print(f"✔️ Inserted pid={transformed.get('pid')} into Cassandra.")
                    count += 1
                elif isinstance(transformed, list):
                    for row in transformed:
                        insert_into_cassandra(session, table_name, row)
                        print(f"✔️ Inserted pid={row.get('pid')} into Cassandra.")
                        count += 1
                else:
                    print(f"Skipping unknown message format: {transformed}")
            except Exception as e:
                print(f"Error processing message: {e}")
    finally:
        await consumer.stop()
        print(f"Kafka consumer stopped. Total records inserted into Cassandra: {count}")
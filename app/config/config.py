import os

class Settings:
    kafka_bootstrap_servers = "localhost:9092"
    transformed_topic = "etl_transformed"
    cassandra_keyspace = "etl"
    raw_data_path = os.path.join("resources", "raw_data")
    cleaned_data_path = os.path.join("resources", "cleaned")
    transformed_data_path = os.path.join("resources", "transformed")

settings = Settings()

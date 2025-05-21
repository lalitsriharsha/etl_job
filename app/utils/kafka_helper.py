from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.config.config import settings

async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    return producer

async def get_kafka_consumer(topic):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="etl_group"
    )
    await consumer.start()
    return consumer

import asyncio
from app.controller.producer_transformed import produce_transformed_data
from app.controller.consumer_loader import consume_and_load

async def run_pipeline():
    await produce_transformed_data()
    await consume_and_load()

if __name__ == "__main__":
    asyncio.run(run_pipeline())

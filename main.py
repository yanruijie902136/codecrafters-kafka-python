import asyncio

from kafka import KafkaServer


if __name__ == "__main__":
    asyncio.run(KafkaServer().start())

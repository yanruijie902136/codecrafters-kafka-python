import asyncio

from kafka import KafkaServer


def main():
    server = KafkaServer()
    asyncio.run(server.start())


if __name__ == "__main__":
    main()

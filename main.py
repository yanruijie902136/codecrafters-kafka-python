#!/usr/bin/env python3

import asyncio

from kafka import KafkaServer


def main() -> None:
    server = KafkaServer()
    asyncio.run(server.start())


if __name__ == "__main__":
    main()

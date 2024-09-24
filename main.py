#!/usr/bin/env python3

import asyncio

import kafka


def main() -> None:
    server = kafka.KafkaServer()
    asyncio.run(server.start())


if __name__ == "__main__":
    main()

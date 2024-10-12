import asyncio

import kafka


def main() -> None:
    server = kafka.Server()
    asyncio.run(server.start())


if __name__ == "__main__":
    main()



import asyncio
from broker.test_consumer import tcp_echo_client


if __name__ == "__main__":
    asyncio.run(tcp_echo_client('Hello World!'))
import asyncio

from amp.common.message import MessageMeta
from amp.consumer.consumer import Consumer


async def process_topic1(message):
    print("message1")


def process_topic2(message):
    print("message2")


if __name__ == "__main__":
    consumer = Consumer("localhost", 3325)
    subscribe_info = {"exchange1": {"topic1": process_topic1, "topic2": process_topic2}}
    meta = MessageMeta({"queue1": {"exchange1": ["topic1", "topic2"]}})
    asyncio.run(consumer.start_consume(meta, subscribe_info))

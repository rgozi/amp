import asyncio
from random import randint

from amp.common.message import MessageBase, MessageBody, MessageMeta, MessageType
from amp.producer.producer import Producer

if __name__ == "__main__":
    producer = Producer("localhost", 3325)


    def pick_one_topic():
        return randint(1, 2)


    def generate_data_message():
        message_body = MessageBody("code", "time", "value")
        meta = MessageMeta({"exchange1": f"topic{pick_one_topic()}"})
        message = MessageBase(MessageType.DATA, message_body, meta)
        return message


    async def start_produce():
        try:
            while True:
                message = generate_data_message()
                await producer.publish_message(message)
        except Exception:
            await producer.on_close()


    asyncio.run(start_produce())

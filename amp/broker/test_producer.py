import asyncio
from dataclasses import asdict
from json import dumps
from random import random

from broker.message import MessageBase, MessageBody, MessageMeta, MessageType


async def tcp_echo_client(message):
    reader, writer = await asyncio.open_connection(
        '127.0.0.1', 3325)

    async def send_data_message():
        message_body = MessageBody('code', 'time', 'value')
        meta = MessageMeta({'exchange1': 'topic1'})
        message = MessageBase(MessageType.DATA,
                              message_body, meta)
        message_dict = message.as_dict()
        writer.write(dumps(message_dict).encode())

        data = await reader.read(1024)
        print(f'Received: {data.decode()!r}')

    try:
        while True:
            await send_data_message()
            await asyncio.sleep(random())
    except KeyboardInterrupt:
        pass

    print('Close the connection')
    writer.close()

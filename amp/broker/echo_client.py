import asyncio
from dataclasses import asdict
from json import dumps

from broker.message import MessageBase, MessageBody, MessageMeta, MessageType


async def tcp_echo_client(message):
    reader, writer = await asyncio.open_connection(
        '127.0.0.1', 3325)

    async def send_message():
        message_body = MessageBody('code', 'time', 'value')
        meta = MessageMeta({'queue1': {'exchange1': ['topic1', 'topic2']}})
        message = MessageBase(MessageType.CONNECTION,
                              message_body, meta)
        message_dict = message.as_dict()
        writer.write(dumps(message_dict).encode())

        data = await reader.read(100)
        print(f'Received: {data.decode()!r}')

    tasks = [await send_message() for x in range(10)]

    print('Close the connection')
    writer.close()

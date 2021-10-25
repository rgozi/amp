import asyncio
from dataclasses import asdict
from json import dumps

from asyncio.exceptions import CancelledError

from common.message import MessageBase, MessageBody, MessageMeta, MessageType


async def tcp_echo_client(message):


    async def send_connection_message():
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', 3325)
            message_body = MessageBody('code', 'time', 'value')
            meta = MessageMeta({'queue1': {'exchange1': ['topic1', 'topic2']}})
            message = MessageBase(MessageType.CONNECTION,
                                message_body, meta)
            message_dict = message.as_dict()
            writer.write(dumps(message_dict).encode())
            while True:
                data = await reader.read(1024)
                print(f'Received: {data.decode()!r}')
        except CancelledError:     
            print('Close the connection')
            writer.close()

    tasks = [send_connection_message() for x in range(10)]
    await asyncio.gather(*tasks)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

import asyncio
from asyncio.streams import StreamReader, StreamWriter
from types import MethodType
from broker.connection import ConnectionManager

from broker.message import MessageBase, MessageType
from broker.queue import QueueManager


class Broker:

    def __init__(self, port: int = 3325, *args, **kwargs) -> None:
        self.port = port
        self.connection_manager: ConnectionManager = None
        self.queue_manager: QueueManager = None

    async def start_broker(self, connection_manager: ConnectionManager, queue_manager: QueueManager):
        self.connection_manager = connection_manager
        self.queue_manager = queue_manager
        await self._init_server()

    async def _init_server(self):
        server = await asyncio.start_server(self._receive_message, '127.0.0.1', self.port)
        addr = server.sockets[0].getsockname()
        print('broker serving on:{}'.format(addr))
        async with server:
            await server.serve_forever()

    async def _receive_message(self, reader: StreamReader, writer: StreamWriter):
        while True:
            try:
                data = await reader.read(1024)
                message = data.decode()
                if not message:
                    break
                addr = writer.get_extra_info('peername')
                response = await self.process_message(message, addr)
                print(f"Send: {response!r}")
                writer.write(response.encode())
                await writer.drain()
            except ConnectionResetError:
                print('connection closed')
                break
            except Exception as e:
                print(str(e))
                break

        writer.close()
        await writer.wait_closed()

    async def process_message(self, message: str, addr: str):
        received_message = MessageBase.load_from_string(message)
        if received_message.message_type == MessageType.CONNECTION:
            print('new consumer info received')
            await self._create_consumer(message, addr)
        elif received_message.message_type == MessageType.DATA:
            print('new data received')
            await self._dispatch_common_message(message)

    async def _create_consumer(self, message: MessageBase, reader: StreamReader, writer: StreamWriter, addr: str):
        pass

    async def _dispatch_common_message(self, message: MessageBase):
        pass


if __name__ == "__main__":
    broker = Broker()
    asyncio.run(broker.start_broker())

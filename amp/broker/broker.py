#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

import asyncio
from asyncio.streams import StreamReader, StreamWriter
from broker.connection import Connection
from broker.exchange import Exchange
from broker.queue import MessageQueue
from broker.store import Store

from broker.message import MessageBase, MessageType


class Broker:

    def __init__(self, port: int = 3325, *args, **kwargs) -> None:
        self.port = port
        self.connection_manager: Store = None
        self.queue_manager: Store = None
        self.exchange_manager: Store = None

    async def start_broker(self, connection_manager: Store, queue_manager: Store, exchange_manager: Store):
        self.connection_manager = connection_manager
        self.queue_manager = queue_manager
        self.exchange_manager = exchange_manager
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
                print(message)
                if not message:
                    break
                addr = writer.get_extra_info('peername')
                response = await self.process_message(message, reader, writer, addr)
                print(f"Send: {response!r}")
                writer.write(response.encode())
                await writer.drain()
            except ConnectionResetError:
                print('connection closed')
                break
            except Exception as e:
                print(str(e))
                writer.write('unknown message'.encode())
                await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def process_message(self, message: str, reader: StreamReader, writer: StreamWriter, addr: str):
        received_message = MessageBase.load_from_string(message)
        if received_message.message_type == MessageType.CONNECTION:
            print('new consumer info received')
            return await self._create_consumer(received_message, reader, writer, addr)
        elif received_message.message_type == MessageType.DATA:
            print('new data received')
            return await self._dispatch_common_message(message)

    async def _create_consumer(self, message: MessageBase, reader: StreamReader, writer: StreamWriter, addr: str):
        target_connection: Connection = self.connection_manager.get(
            addr, reader, writer)
        for queue_name, subscibe_info in message.message_meta.info.items():
            target_queue: MessageQueue = self.queue_manager.get(queue_name)
            target_queue.set_subscribe_info(subscibe_info)

            for exchange_name, topic_list in subscibe_info.items():
                target_exchange: Exchange = self.exchange_manager.get(
                    exchange_name)
                for topic in topic_list:
                    target_exchange.bind_topic(topic)

            target_connection.bind_queue(target_queue)
        return 'connection inited'

    async def _dispatch_common_message(self, message: MessageBase):
        pass


def run():
    QManger = Store(MessageQueue)
    CManager = Store(Connection)
    Emanger = Store(Exchange)
    broker = Broker()
    asyncio.run(broker.start_broker(CManager, QManger, Emanger))

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

import asyncio
from asyncio.streams import StreamReader, StreamWriter

from amp.broker.connection import Connection
from amp.broker.exchange import Exchange
from amp.broker.queue import MessageQueue
from amp.broker.router import Router
from amp.broker.store import Store
from amp.common.message import MessageBase, MessageType


class Broker:
    """
    broker manages server side logic
    """

    def __init__(self, port: int = 3325, *args, **kwargs) -> None:
        self.port = port
        self.connection_manager: Store = None
        self.queue_manager: Store = None
        self.exchange_manager: Store = None
        self.router: Router = None

    async def start_broker(
            self,
            connection_manager: Store,
            queue_manager: Store,
            exchange_manager: Store,
            router: Router,
    ):
        """
        start broker
        """
        self.connection_manager = connection_manager
        self.queue_manager = queue_manager
        self.exchange_manager = exchange_manager
        self.router = router
        await self._init_server()

    async def _init_server(self):
        server = await asyncio.start_server(
            self._receive_message, "127.0.0.1", self.port
        )
        addr = server.sockets[0].getsockname()
        print(f"broker serving on:{addr}")
        async with server:
            await server.serve_forever()

    async def _receive_message(self, reader: StreamReader, writer: StreamWriter):
        addr = writer.get_extra_info("peername")
        while True:
            try:
                data = await reader.read(1024)
                message = data.decode()
                print(message)
                if not message:
                    break
                await self._process_message(message, reader, writer, addr)
            except ConnectionResetError:
                print("connection closed")
                break
            except Exception as e:
                print(str(e))
                writer.write("unknown message".encode())
                await writer.drain()

        print("connection closed!")
        if addr in self.connection_manager.context:
            target_connection: Connection = self.connection_manager.get(addr)
            await target_connection.on_close()
        writer.close()
        await writer.wait_closed()

    async def _process_message(
            self, message: str, reader: StreamReader, writer: StreamWriter, addr: str
    ):
        received_message = MessageBase.load_from_string(message)
        if received_message.message_type == MessageType.CONNECTION:
            print("new consumer info received")
            await self._create_consumer(received_message, reader, writer, addr)
        elif received_message.message_type == MessageType.DATA:
            print("new data received")
            await self._dispatch_common_message(received_message)
            writer.write("message dispatched".encode())
            await writer.drain()

    async def _create_consumer(
            self,
            message: MessageBase,
            reader: StreamReader,
            writer: StreamWriter,
            addr: str,
    ):
        target_connection: Connection = self.connection_manager.get(
            addr, reader, writer
        )
        for queue_name, subscibe_info in message.message_meta.info.items():
            target_queue: MessageQueue = self.queue_manager.get(queue_name)
            target_queue.set_subscribe_info(subscibe_info)

            for exchange_name, topic_list in subscibe_info.items():
                target_exchange: Exchange = self.exchange_manager.get(exchange_name)
                for topic in topic_list:
                    target_exchange.bind_topic(topic)

            target_connection.bind_queue(target_queue)
        await target_connection.on_create()

    async def _dispatch_common_message(self, message: MessageBase):
        await self.router.dispatch(message)


def run(port=3325):
    """
    start a broker
    """
    QManger = Store(MessageQueue)
    CManager = Store(Connection)
    Emanger = Store(Exchange)
    broker = Broker(port)
    router = Router(QManger)
    asyncio.run(broker.start_broker(CManager, QManger, Emanger, router))

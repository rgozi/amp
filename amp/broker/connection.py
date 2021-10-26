#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21 15:25:27
# @Author  : iamwm

from asyncio.streams import StreamReader, StreamWriter

from amp.broker.queue import MessageQueue


class Connection:
    """
    connection used by broker
    """

    def __init__(
            self,
            name: str,
            reader: StreamReader,
            writer: StreamWriter,
    ) -> None:
        self.name = name
        self.reader = reader
        self.writer = writer
        self.queue_context = {}

    def bind_queue(self, queue: MessageQueue):
        """
        bind a queue to consumer
        """
        queue.add_consumer({self.name: self})
        self.queue_context.update({queue.name: queue})

    async def on_create(self):
        """
        things to do on connection created
        """
        response = f"connection from:{self.name} created!"
        print(response)
        self.writer.write(response.encode())
        await self.writer.drain()

    async def on_close(self):
        """
        things to do on connection cloesed
        """
        response = f"connection from:{self.name} closed!"
        print(response)
        for _, queue in self.queue_context.items():
            queue.remove_consumer(self.name)

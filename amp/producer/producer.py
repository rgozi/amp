#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-25 16:26:37
# @Author  : iamwm


import asyncio
from json import dumps

from amp.common.message import MessageBase


class Producer:
    """
    producer class used to generate messages to target topic
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._is_connected = False
        self._reader = self._writer = None

    async def _connect(self):
        while True:
            try:
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port
                )
                self._is_connected = True
            except Exception as e:
                print(f"connection error:{str(e)}")
                print("retrying")
                await asyncio.sleep(3)
                await self._connect()
            else:
                break

    async def publish_message(self, message: MessageBase):
        """
        publish message
        """
        if not self._is_connected:
            await self._connect()
        message_dict = message.as_dict()
        self._writer.write(dumps(message_dict).encode())
        await self._writer.drain()

        data = await self._reader.read(1024)
        data_str = data.decode()
        print(f"receive message:{data_str}")

    async def on_close(self):
        """
        things to do to clean up
        """
        self._is_connected = False
        self._writer.close()
        await self._writer.wait_closed()

    async def on_create(self):
        """
        things to do to start up
        """
        pass

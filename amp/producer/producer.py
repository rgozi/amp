#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-25 16:26:37
# @Author  : iamwm


import asyncio


from common.message import MessageBase
from json import dumps


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

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

import asyncio
from json import dumps
from typing import Any
from broker import exchange

from broker.store import Store
from random import choice


class MessageQueue:
    def __init__(self, name: str) -> None:
        self.name = name
        self.q = asyncio.Queue()
        self.subscribe_info = {}
        self.consumers = {}

    def set_subscribe_info(self, info: dict):
        self.subscribe_info = info

    def add_consumer(self, consumer_info: dict):
        self.consumers.update(consumer_info)
        task = self._loop()
        asyncio.create_task(task)

    def remove_consumer(self, consumer_name: str):
        self.consumers.pop(consumer_name, None)
        print(f'remove consumer:{consumer_name} from queue:{self.name}')

    def is_message_subscribed(self, exchange_name: str, topic_name: str):
        return exchange_name in self.subscribe_info and topic_name in self.subscribe_info.get(exchange_name, {})

    def pick_one_consumer(self):
        target_consumer_name = choice(list(self.consumers.keys()))
        return self.consumers.get(target_consumer_name)

    async def put(self, item: Any):
        return await self.q.put(item)

    async def get(self):
        return await self.q.get()

    async def _loop(self):
        while True:
            try:
                message = await self.get()
                if self.consumers:
                    target_consumer = self.pick_one_consumer()
                else:
                    print(f'abandon message of:{self.name}')
                    continue
                print(f"send message to:{target_consumer.name}!")
                message_dict = message.as_dict()
                target_consumer.writer.write(dumps(message_dict).encode())
                await target_consumer.writer.drain()
            except ConnectionResetError as e:
                print(
                    f"connection:{target_consumer.name} closed! retry another!")

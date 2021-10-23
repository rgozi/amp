#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

import asyncio

from broker.store import Store


class MessageQueue:
    def __init__(self, name: str) -> None:
        self.name = name
        self.q = asyncio.Queue()
        self.subscribe_info = {}
        self.consumers = []

    def set_subscribe_info(self, info: dict):
        self.subscribe_info = info

    def add_consumer(self, consumer_name: str):
        self.consumers.append(consumer_name)

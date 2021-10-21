#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

import asyncio
from typing import Mapping


class MessageQueue:
    def __init__(self, name: str) -> None:
        self.name = name
        self.q = asyncio.Queue()


class QueueManager:
    def __init__(self) -> None:
        self.queue_mapper: Mapping[str, MessageQueue] = {}

    def get_queue(self, queue_name: str) -> MessageQueue:
        if queue_name not in self.queue_mapper:
            self.queue_mapper.update({queue_name: MessageQueue(queue_name)})
        return self.queue_mapper.get(queue_name)

    def set_queue(self, queue: MessageQueue) -> bool:
        queue_name = queue.name
        if queue_name in self.queue_mapper:
            return False
        self.queue_mapper.update({queue_name, queue})
        return True


QManger = QueueManager()

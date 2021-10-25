#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-25 10:07:12
# @Author  : iamwm


from broker.message import MessageBase
from broker.store import Store


class Router:
    def __init__(self, queue_manager: Store) -> None:
        self.queue_manager = queue_manager

    async def dispatch(self, message: MessageBase):
        data_meta = message.message_meta
        for exchange_name, topic_name in data_meta.info.items():
            for queue_name, queue in self.queue_manager.context.items():
                if queue.is_message_subscribed(exchange_name, topic_name):
                    print(f'queue:{queue_name} got new message')
                    await queue.put(message)
                else:
                    print(
                        f'no consumer wants message of topic:{topic_name} in exchange:{exchange_name}')

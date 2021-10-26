#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

from amp.broker.store import Store


class Exchange:
    """
    exchange of broker
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.topic_manager = Store(Topic)

    def bind_topic(self, topic_name: str):
        self.topic_manager.get(topic_name)


EManager = Store(Exchange)


class Topic:
    """
    topic of exchange
    """

    def __init__(self, name: str) -> None:
        self.name = name

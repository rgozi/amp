#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21
# @Author  : iamwm

class Exchange:

    def __init__(self, name: str, exchange_type: str) -> None:
        self.name = name
        self.exchange_type = exchange_type

    @classmethod
    def init_exchange_by_name(cls, name: str):
        pass

    def bind_topic(self, topic_name: str):
        pass


class Topic:

    def __init__(self, name: str, bind_to: str) -> None:
        self.name = name
        self.bind_to = bind_to

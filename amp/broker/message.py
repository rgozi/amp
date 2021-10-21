#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21 15:29:45
# @Author  : iamwm
from enum import Enum
from json import loads


class MessageType(Enum):
    CONNECTION = 1
    DATA = 2


class MessageBody:
    def __init__(self, code: str, timestamp: str, value: str) -> None:
        self.code = code
        self.timestamp = timestamp
        self.value = value

    @classmethod
    def load_from_dict(cls, info: dict):
        return cls(**info)


class MessageMeta:
    def __init__(self, info: dict) -> None:
        self.info = info


class ConsumerMeta(MessageMeta):
    def __init__(self, info: dict) -> None:
        super().__init__(info)

    @property
    def queue_name(self):
        return self.info.get('queue_name')

    @property
    def subscribe_info(self):
        return self.info.get('subscribe_info', {})


class MessageBase:
    def __init__(self, message_type: MessageType, message_body: MessageBody, message_meta: MessageMeta) -> None:
        self.message_type = message_type
        self.message_body = message_body
        self.message_meta = message_meta

    @classmethod
    def load_from_string(cls, message_str: str):
        message_dict = loads(message_str)
        try:
            message_type = MessageMeta(message_dict.get('message_type'))
            message_body = MessageBody.load_from_dict(
                message_dict.get('message_body'))
            message_meta = MessageMeta(message_dict.get('message_meta'))
            return cls(message_type, message_body, message_meta)
        except Exception as e:
            raise Exception("load message error")

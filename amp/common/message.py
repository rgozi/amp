#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21 15:29:45
# @Author  : iamwm
from dataclasses import asdict, dataclass
from enum import Enum
from json import loads


class MessageType(Enum):
    CONNECTION = 1
    DATA = 2


@dataclass
class MessageBody:
    code: str
    timestamp: str
    value: str

    @classmethod
    def load_from_dict(cls, info: dict):
        return cls(**info)


class MessageMeta(dict):
    pass

    @property
    def info(self):
        return dict(self)


@dataclass
class MessageBase:
    message_type: MessageType
    message_body: MessageBody
    message_meta: MessageMeta

    def as_dict(self):
        return {
            "message_type": self.message_type.value,
            "message_body": asdict(self.message_body),
            "message_meta": self.message_meta,
        }

    @classmethod
    def load_from_string(cls, message_str: str):
        try:
            message_dict = loads(message_str)
            message_type = MessageType(message_dict.get("message_type"))
            message_body = MessageBody.load_from_dict(message_dict.get("message_body"))
            message_meta = MessageMeta(message_dict.get("message_meta"))
            return cls(message_type, message_body, message_meta)
        except Exception as e:
            raise Exception("load message error")


if __name__ == "__main__":
    message_body = MessageBody("code", "time", "value")
    meta = MessageMeta({"queue1": {"exchange1": ["topic1", "topic2"]}})
    message = MessageBase(MessageType.CONNECTION, message_body, meta)

    print(message.as_dict())

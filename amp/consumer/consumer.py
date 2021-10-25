#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-25 14:26:26
# @Author  : iamwm

import asyncio
from asyncio import iscoroutinefunction
from typing import Mapping


from common.message import MessageBase, MessageBody, MessageMeta, MessageType
from json import dumps


class Consumer:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._consumer_meta = None
        self._process_mapper = None
        self._reader = self._writer = None

    def init_consumer_meta(self, meta: MessageMeta):
        self._consumer_meta = meta

    def init_consumer_process(self, process_mapper: Mapping[str, Mapping[str, callable]]):
        self._process_mapper = process_mapper

    async def _connect(self):
        while True:
            try:
                self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
            except Exception as e:
                print(f'connection error:{str(e)}')
                print('retrying')
                await asyncio.sleep(3)
                await self._connect()
            else:
                break

    async def start_consume(self, consumer_meta: MessageMeta, process_mapper:  Mapping[str, Mapping[str, callable]]):
        await self._connect()
        print('connection created!')
        self.init_consumer_meta(consumer_meta)
        self.init_consumer_process(process_mapper)
        message_body = MessageBody('code', 'time', 'value')
        meta = self._consumer_meta
        message = MessageBase(MessageType.CONNECTION,
                              message_body, meta)
        message_dict = message.as_dict()
        self._writer.write(dumps(message_dict).encode())
        await self._writer.drain()
        print('consumer meta sent')
        while True:
            try:
                data = await self._reader.read(1024)
                data_str = data.decode()
                received_message = await self._convert_message(data_str)
                await self._dispatch_message(received_message)
            except ConnectionResetError:
                print('connection closed')
                break
            except Exception as e:
                print(str(e))
                print('unknown message'.encode())

        print('connection closed!')
        self._writer.close()
        await self._writer.wait_closed()

    async def _convert_message(self, message_str: str) -> MessageBase:
        received_message = MessageBase.load_from_string(message_str)
        return received_message

    async def _dispatch_message(self, message: MessageBase):
        for exchange_name, topic_name in message.message_meta.info.items():
            process_func = self._process_mapper.get(
                exchange_name, {}).get(topic_name)
            if exchange_name in self._process_mapper and process_func:
                if iscoroutinefunction(process_func):
                    await process_func(message.message_body)
                else:
                    process_func(message.message_body)

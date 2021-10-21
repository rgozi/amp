#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-21 15:25:27
# @Author  : iamwm

from asyncio.streams import StreamReader, StreamWriter


class Connection:
    def __init__(self, name: str, reader: StreamReader, writer: StreamWriter) -> None:
        self.name = name
        self.reader = reader
        self.writer = writer


class ConnectionManager:
    pass

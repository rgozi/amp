#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-26 10:17:35
# @Author  : iamwm

import click

from amp.broker.broker import run


@click.command()
@click.option("--port", default=3325, help="port the amp server listen")
def start_server(port: int):
    """
    start amp broker server listen on the given port
    """
    run(port)

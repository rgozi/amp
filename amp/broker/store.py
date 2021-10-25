#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2021-10-23 10:13:19
# @Author  : iamwm


class Store:

    def __init__(self, cls) -> None:
        self.class_type = cls
        self.context = {}

    def get(self, name: str, *args, **kwargs):
        if name not in self.context:
            target_obj = self.class_type(name, *args, **kwargs)
            self.context.update({name: target_obj})
        return self.context.get(name)

    def set(self, obj) -> bool:
        name = obj.name
        if name in self.queue_mapper:
            return False
        self.context.update({name: obj})
        return True

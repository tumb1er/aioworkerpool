# coding: utf-8
import asyncio
import typing

Callback = typing.Union[typing.Callable[[], None],
                        typing.Callable[[], typing.Awaitable[None]]]


CallbackList = typing.List[Callback]


class Signal:
    def __init__(self):
        self._callbacks = []  # type: CallbackList

    def connect(self, callback: Callback):
        self._callbacks.append(callback)

    async def send(self):
        for cb in self._callbacks:
            if asyncio.iscoroutinefunction(cb):
                await cb()
            else:
                cb()

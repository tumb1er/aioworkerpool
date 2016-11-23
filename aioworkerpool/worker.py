# coding: utf-8
import asyncio
from abc import ABCMeta, abstractmethod


class WorkerBase(metaclass=ABCMeta):
    """ Abstract class for worker implementation."""

    def __init__(self, worker_id: int, loop: asyncio.AbstractEventLoop):
        self._worker_id = worker_id
        self._loop = loop
        self._running = False

    @property
    def id(self):
        return self._worker_id

    @property
    def loop(self):
        return self._loop

    def is_running(self):
        return self._running

    def stop(self):
        self._running = False

    @abstractmethod
    async def main(self):
        raise NotImplementedError()

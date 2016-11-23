# coding: utf-8
import asyncio
import sys
from abc import ABCMeta, abstractmethod
from logging import getLogger

from aioworkerpool.signals import Signal, Callback


class WorkerBase(metaclass=ABCMeta):
    """ Abstract class for worker implementation."""
    logger = getLogger('aioworkerpool.Worker')

    def __init__(self, worker_id: int, loop: asyncio.AbstractEventLoop):
        self._worker_id = worker_id
        self._loop = loop
        self._running = False
        self._main_task = None  # type: asyncio.Task
        self._on_start = Signal()
        self._on_shutdown = Signal()
        self._is_shut_down = False

    @property
    def id(self):
        return self._worker_id

    @property
    def loop(self):
        return self._loop

    def is_running(self):
        return self._running

    def on_start(self, callback: Callback):
        self._on_start.connect(callback)

    def on_shutdown(self, callback: Callback):
        self._on_shutdown.connect(callback)

    def stop(self):
        self._running = False

    def interrupt(self):
        self.logger.info("Interrupting...")
        if self._main_task:
            self._main_task.cancel()
        self._shutdown()

    def terminate(self):
        self.logger.info("Terminating...")
        self.stop()
        self._main_task.add_done_callback(lambda f: self._shutdown())

    def _stop_loop(self):
        self._loop.call_soon(self._exit)
        self._loop.stop()

    def _exit(self):
        self._loop.close()
        sys.exit(0)

    def _shutdown(self):
        if self._is_shut_down:
            return
        task = asyncio.Task(self._on_shutdown.send())
        task.add_done_callback(lambda f: self._stop_loop())
        self._is_shut_down = True

    @abstractmethod
    async def main(self):
        raise NotImplementedError()

    def start(self):
        self.logger.debug("Start worker...")
        self._loop.run_until_complete(self._on_start.send())
        self._running = True
        self._main_task = asyncio.Task(self.main(), loop=self._loop)
        self._main_task.add_done_callback(lambda f: self._shutdown())
        self.logger.debug("Worker started")

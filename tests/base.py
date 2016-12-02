# coding: utf-8
import asyncio
import functools
from unittest import TestCase

from aioworkerpool import master, worker


def unittest_with_loop(func):
    """ Runs async test method synchronously."""

    @functools.wraps(func)
    def wrapped(self):
        return self.loop.run_until_complete(func(self))

    return wrapped


class TestCaseBase(TestCase):

    def setUp(self):
        super().setUp()
        self.loop = asyncio.new_event_loop()  # type: asyncio.BaseEventLoop
        asyncio.set_event_loop(None)

    def tearDown(self):
        super().tearDown()
        self.loop.stop()
        self.loop.close()


class TestWorker(worker.WorkerBase):

    def __init__(self, worker_id, loop):
        super().__init__(worker_id, loop)
        self.on_start(self.start_cb)
        self.on_shutdown(self.shutdown_cb)
        self.start_cb_called = False
        self.shutdown_cb_called = False

    async def start_cb(self):
        await asyncio.sleep(0)
        self.start_cb_called = True

    def shutdown_cb(self):
        self.shutdown_cb_called = True

    async def main(self):
        await asyncio.sleep(0.1, loop=self.loop)


class TestSupervisor(master.Supervisor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.on_start(self.start_cb)
        self.on_shutdown(self.shutdown_cb)
        self.start_cb_called = False
        self.shutdown_cb_called = False

    async def start_cb(self):
        await asyncio.sleep(0)
        self.start_cb_called = True

    def shutdown_cb(self):
        self.shutdown_cb_called = True

    async def _check_pool(self):
        alive = await super()._check_pool()
        self._running = False
        return alive



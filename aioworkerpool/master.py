# coding: utf-8
import asyncio
import multiprocessing as mp
import os
import signal
import time
import typing
from logging import getLogger

import sys

from daemon import DaemonContext
from daemon.pidfile import TimeoutPIDLockFile

from aioworkerpool.signals import Signal, Callback
from aioworkerpool.worker import WorkerBase

WorkerFactory = typing.Callable[[int, asyncio.AbstractEventLoop], WorkerBase]
ChildFactory = typing.Callable[
    [int, asyncio.AbstractEventLoop, WorkerFactory], "ChildHandler"]


class ChildHandler:
    """ Worker process handler."""
    logger = getLogger("aioworkerpool.Handler")

    def __init__(self, worker_id: int, loop: asyncio.BaseEventLoop,
                 worker_factory:  WorkerFactory):
        self._last_alive = 0
        self._child = None  # type: mp.Process
        self._loop = loop
        self._exit_future = asyncio.Future(loop=self._loop)
        self._worker_id = worker_id
        self._worker = None  # type: WorkerBase
        self._worker_factory = worker_factory

    @property
    def id(self) -> int:
        return self._worker_id

    @property
    def loop(self):
        return self._loop

    def is_running(self):
        return self._worker and self._worker.is_running()

    def is_stale(self):
        """ Checks if child process hang up."""
        return not self.child_exists() or not self._child.is_alive()

    def child_exists(self):
        """  Checks if child process exists."""
        return self._child and self._child.exitcode is None

    def start(self):
        self.logger.debug("Starting worker with id=%s" % self._worker_id)
        self._child = self.init_child()
        self._child.start()
        # add event loop round-trip to ensure that child has started
        asyncio.Task(self._add_child_handler())

    async def _add_child_handler(self):
        watcher = asyncio.get_child_watcher()
        """:type : asyncio.unix_events.BaseChildWatcher"""
        watcher.add_child_handler(self._child.pid, self.on_child_exit)
        self.logger.debug("Started child %s" % self._child.pid)

    def on_child_exit(self, pid, return_code):
        self.logger.info("Child process %s exited with code %s" % (
            pid, return_code))
        self._exit_future.set_result(return_code)
        self._child = None

    def _main(self):
        self.logger.debug("MAIN()")
        self._loop.call_soon(self._loop.close)
        self._loop.stop()

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._running = True

        self._worker = self.init_worker()
        self._loop.add_signal_handler(signal.SIGINT, self._worker.interrupt)
        self._loop.add_signal_handler(signal.SIGTERM, self._worker.terminate)
        self._worker.start()
        self._loop.run_forever()

    def init_child(self):
        ctx = mp.get_context("fork")
        return ctx.Process(target=self._main, daemon=True)

    def init_worker(self):
        return self._worker_factory(self._worker_id, self._loop, )

    def kill(self):
        return self.send_and_wait(sig=signal.SIGKILL)

    def terminate(self):
        return self.send_and_wait(sig=signal.SIGTERM)

    def interrupt(self):
        return self.send_and_wait(sig=signal.SIGINT)

    def send_and_wait(self, sig: int) -> asyncio.Future:
        """
        Sends signal to child process and waits for process shutdown.

        :param sig: signal to send to child (TERM, INT, KILL)
        :returns: future with exitcode of child process
        """
        if not self._child:
            return self._exit_future
        self.logger.debug("Sending signal %s to %s" % (sig, self._child.pid))
        os.kill(self._child.pid, sig)
        return self._exit_future


class Supervisor:
    """ Worker controller process."""

    logger = getLogger('aioworkerpool.Supervisor')

    def __init__(self, worker_factory: WorkerFactory,
                 loop: asyncio.BaseEventLoop = None, workers: int = 2,
                 check_interval: float = 1.0,
                 child_factory: ChildFactory = ChildHandler):
        self._loop = loop
        self._workers = workers
        self._worker_factory = worker_factory
        self._child_factory = child_factory
        self._check_interval = check_interval

        self._check_task = None  # type: asyncio.Task
        self._wait_task = None   # type: asyncio.Task
        self._pool = dict()      # type: typing.Dict[int, ChildHandler]
        self._on_start = Signal()
        self._on_shutdown = Signal()
        self._last_check = 0
        self._running = False

    @property
    def loop(self):
        return self._loop

    def on_start(self, callback: Callback):
        self._on_start.connect(callback)

    def on_shutdown(self, callback: Callback):
        self._on_shutdown.connect(callback)

    def start(self):
        self.logger.info("Starting pool")
        self._loop = self._loop or asyncio.get_event_loop()
        self._running = True
        self._loop.run_until_complete(self._on_start.send())
        self.loop.add_signal_handler(signal.SIGINT, self.interrupt)
        self.loop.add_signal_handler(signal.SIGTERM, self.terminate)
        self._check_task = asyncio.Task(self._run_forever_loop(),
                                        loop=self._loop)
        self.logger.info("Pool started")

    def interrupt(self):
        self.logger.info("Got SIGINT, shutting down workers...")
        self.loop.remove_signal_handler(signal.SIGINT)
        task = asyncio.Task(self._stop_workers(signal.SIGINT))
        task.add_done_callback(lambda f: self._on_workers_stopped())

    def terminate(self):
        self.logger.info("Got SIGTERM, shutting down workers...")
        self.loop.remove_signal_handler(signal.SIGTERM)
        task = asyncio.Task(self._stop_workers(signal.SIGTERM))
        task.add_done_callback(self._on_workers_stopped)

    def _on_workers_stopped(self):
        task = asyncio.Task(self._shutdown())
        task.add_done_callback(lambda f: self.loop.stop())

    async def _shutdown(self):
        self.logger.info("Shutting down")
        self._running = False
        await self._check_task
        await self._on_shutdown.send()
        self.logger.info("Bye!")

    async def _stop_workers(self, sig):
        futures = []
        for worker in self._pool.values():
            futures.append(worker.send_and_wait(sig))
        await asyncio.gather(*futures, loop=self._loop,
                             return_exceptions=True)

    async def _run_forever_loop(self):
        while self._running:
            self.logger.debug("check pool")
            await self._check_pool()
            now = time.time()
            interval = min(now - self._last_check, self._check_interval)
            self._wait_task = asyncio.Task(asyncio.sleep(interval),
                                           loop=self._loop)
            try:
                await self._wait_task
            except asyncio.CancelledError:
                pass
            self._last_check = now

    async def _check_pool(self):
        # check if some worker processes are stale or exited
        for worker in list(self._pool.values()):
            if not worker.is_stale():
                self.logger.debug("worker %s is not stale" % worker.id)
                continue
            if not worker.child_exists():
                self.logger.warning("Removing worker %s because exited" %
                                    worker)
                self._pool.pop(worker.id)
            else:
                self.logger.warning("Removing stale worker %s" % worker)
                await worker.kill()

        self.logger.debug("%s workers in pool" % len(self._pool))

        if not self._running:
            return
        for worker_id in set(range(self._workers)) - set(self._pool.keys()):
            worker = ChildHandler(worker_id, self._loop, self._worker_factory)
            self._pool[worker_id] = worker
            worker.start()

    def main(self, daemonize=False, pidfile='workerpool.pid'):
        if daemonize:
            context = self.get_daemon_context(pidfile)
            with context:
                self._main()
        else:
            self._main()

    def _main(self):
        self.start()
        self._loop.run_forever()
        self._loop.close()

    # noinspection PyMethodMayBeStatic
    def get_daemon_context(self, pidfile):
        path = os.path.join(os.getcwd(), pidfile)
        pidfile = TimeoutPIDLockFile(path)
        return DaemonContext(pidfile=pidfile, stderr=sys.stderr)

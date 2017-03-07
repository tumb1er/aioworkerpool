# coding: utf-8
import asyncio
import logging
import os
import signal
import sys
import time
import typing
from logging import getLogger

import collections
from daemon import DaemonContext
from daemon.pidfile import TimeoutPIDLockFile

from aioworkerpool.logging import PickleStreamHandler, PicklePipeReader
from aioworkerpool.pipes import PipeProxy, KeepAlivePipe
from aioworkerpool.signals import Signal, Callback
from aioworkerpool.worker import WorkerBase

WorkerFactory = typing.Callable[[int, asyncio.AbstractEventLoop], WorkerBase]
ChildFactory = typing.Callable[
    [int, asyncio.AbstractEventLoop, WorkerFactory], "ChildHandler"]


class ChildHandler:
    """ Worker process handler."""
    logger = getLogger("aioworkerpool.Handler")

    def __init__(self, worker_id: int, loop: asyncio.AbstractEventLoop,
                 worker_factory:  WorkerFactory, *,
                 stdout=sys.stdout, stderr=sys.stderr, worker_timeout=15.0,
                 preserve_fds: typing.Iterable[int]=()):
        """
        :param worker_id: id of created worker
        :param loop: asyncio event loop instance
        :param worker_factory: worker factory
        :param stdout: file-like object to redirect stdout from workers
        :param stderr: file-like object to redirect stderr from workers
        :param worker_timeout: worker keep-alive timeout
        :param preserve_fds: list of file descriptors to preserve on fork()
        """
        self._preserve_fds = preserve_fds
        self._worker_timeout = worker_timeout
        self._child_pid = 0
        self._child_exit_code = None
        self._loop = loop
        self._exit_future = asyncio.Future(loop=self._loop)
        self._worker_id = worker_id
        self._worker = None  # type: WorkerBase
        self._worker_factory = worker_factory
        self._stdout_pipe = None     # type: PipeProxy
        self._stderr_pipe = None     # type: PipeProxy
        self._logging_pipe = None    # type: PicklePipeReader
        self._keepalive_pipe = None  # type: KeepAlivePipe
        self._logging_task = None    # type: asyncio.Task
        self._keepalive_task = None  # type: asyncio.Task
        self._pong_task = None       # type: asyncio.Task
        self._stdout = stdout
        self._stderr = stderr
        self._max_fd = 0
        self.handler = None  # type: PickleStreamHandler
        self._alive = False
        self._pong_stream = None

    @property
    def id(self) -> int:
        return self._worker_id

    @property
    def loop(self):
        return self._loop

    def is_running(self) -> bool:
        """ Returns worker running state."""
        return self._worker and self._worker.is_running()

    def child_exists(self) -> bool:
        """  Checks if child process exists."""
        return self._child_pid and self._child_exit_code is None

    def is_stale(self) -> bool:
        """ Checks if child process hang up."""
        return not self.child_exists() or not self._alive

    def fork(self) -> bool:
        """ Creates worker process with fork() method

        Also initializes pipes between parent and child processes and installs
        pickling logging handler for child process

        :returns: True for parent process and False for child
        """
        with open('/dev/null', 'r') as f:
            self._max_fd = f.fileno()
        r_stdout, w_stdout = os.pipe()
        r_stderr, w_stderr = os.pipe()
        r_logging, w_logging = os.pipe()
        r_keepalive, w_keepalive = os.pipe()

        self._child_pid = os.fork()
        if not self._child_pid:
            os.dup2(w_stdout, sys.stdout.fileno())
            os.close(r_stdout)
            os.close(w_stdout)

            os.dup2(w_stderr, sys.stderr.fileno())
            os.close(r_stderr)
            os.close(w_stderr)

            os.close(r_logging)
            self.init_worker_logging(w_logging)

            os.close(r_keepalive)
            self._pong_stream = os.fdopen(w_keepalive, 'w')
            return False
        os.close(w_stdout)
        os.close(w_stderr)
        os.close(w_logging)
        os.close(w_keepalive)
        self._stdout_pipe = PipeProxy(r_stdout, self._stdout, loop=self._loop)
        self._stderr_pipe = PipeProxy(r_stderr, self._stderr, loop=self._loop)
        self._logging_pipe = PicklePipeReader(r_logging, loop=self._loop)
        self._keepalive_pipe = KeepAlivePipe(r_keepalive, loop=self._loop,
                                             timeout=self._worker_timeout)
        return True

    def init_worker_logging(self, logging_pipe: int):
        """ Initializes handler for passing logs from internal logger to
        parent process through pipe.

        :param logging_pipe: number of file descriptor for logging pipe
        """
        self.handler = PickleStreamHandler(os.fdopen(logging_pipe, 'w'))
        self.handler.setLevel(logging.DEBUG)
        self.logger = getLogger('aioworkerpool')
        self.logger.propagate = False
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.DEBUG)
        # write all log messages to master process
        logging.root.addHandler(self.handler)

    def start(self):
        """ Starts new child process."""
        self.logger.debug("Starting worker with id=%s" % self._worker_id)
        if self.fork():
            self.logger.debug("forked %s" % self._child_pid)
            self._alive = True
            # add event loop round-trip to ensure that child has started
            asyncio.Task(self._add_child_handler(), loop=self._loop)
        else:
            self.cleanup_parent_loop()
            # start worker event loop
            self._main()

    async def _add_child_handler(self):
        """ Initializes handling of started child process.

        - connect pipes for stdout/stderr, logs and keep-alive
        - start async read loops on these pipes

        """
        # noinspection PyBroadException
        try:
            self.logger.debug("add_child_handler...")
            watcher = asyncio.get_child_watcher()
            """:type : asyncio.unix_events.BaseChildWatcher"""
            watcher.add_child_handler(self._child_pid, self.on_child_exit)

            await asyncio.gather(
                self._stdout_pipe.connect_fd(),
                self._stderr_pipe.connect_fd(),
                self._logging_pipe.connect_fd(),
                self._keepalive_pipe.connect_fd(),
                loop=self._loop)

            asyncio.Task(self._stdout_pipe.read_loop(), loop=self._loop)
            asyncio.Task(self._stderr_pipe.read_loop(), loop=self._loop)

            self._logging_task = asyncio.Task(
                self._logging_pipe.read_loop(), loop=self._loop)

            self._keepalive_task = asyncio.Task(
                self._keepalive_pipe.read_loop(), loop=self._loop)
            self._keepalive_task.add_done_callback(self._mark_stale)
            self.logger.debug("Started child %s" % self._child_pid)
        except Exception:
            self.logger.exception("Error while handling child process start")

    def on_child_exit(self, pid: int, return_code: int):
        """ Handles child process exit.

        Stops logging and keepalive tasks.

        :param pid: pid of exited process
        :param return_code: exit code of process
        """
        self.logger.info("Child process %s exited with code %s" % (
            pid, return_code))
        self._child_exit_code = return_code
        self._exit_future.set_result(return_code)
        if self._logging_task:
            self._logging_task.cancel()
        if self._keepalive_task:
            self._keepalive_task.cancel()
        self._child_pid = None

    def _mark_stale(self, future: asyncio.Future):
        """ Marks child process as stale depending of keepalive result."""
        if future.cancelled():
            # child exited
            return
        if future.result():
            # clean pipe reader termination
            return
        # keep-alive task returned False, marking child as not alive.
        self.logger.warning("Marking %s as stale" % self._child_pid)
        self._alive = False

    def cleanup_parent_loop(self):
        """ Cleanups and stops parent event loop in child process.

        Closes file descriptors not listed in preserved_fds.
        """
        self.logger.debug("Cleanup parent loop")
        # remove signal handlers from parent event loop
        self._loop.remove_signal_handler(signal.SIGINT)
        self._loop.remove_signal_handler(signal.SIGTERM)
        # mark parent loop as stopping
        self._loop.stop()
        # closing all file descriptors opened before fork() except
        # STDIN, STDOUT, STDERR instead of trying to close only
        # event loop selector descriptor.
        for fd in set(range(3, self._max_fd)) - set(self._preserve_fds):
            try:
                # not every integer in range is a valid file descriptor
                os.close(fd)
            except IOError:
                pass

    def _main(self):
        """ Initializes and starts worker loop."""
        self.logger.debug("MAIN()")
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._running = True
        self._worker = self.init_worker()
        self._loop.add_signal_handler(signal.SIGINT, self._worker.interrupt)
        self._loop.add_signal_handler(signal.SIGTERM, self._worker.terminate)
        self._pong_task = asyncio.Task(self._pong_loop(), loop=self._loop)
        self._worker.start()  # should call loop.run_forever()
        # exit child process after loop stop.
        sys.exit(0)

    def init_worker(self):
        """ Construct worker object with worker factory."""
        return self._worker_factory(self._worker_id, self._loop)

    def send_and_wait(self, sig: int = signal.SIGKILL) -> asyncio.Future:
        """
        Sends signal to child process and waits for process shutdown.

        :param sig: signal to send to child (TERM, INT, KILL)
        :returns: future with exitcode of child process
        """
        if not self._child_pid:
            return self._exit_future
        self.logger.debug("Sending signal %s to %s" % (sig, self._child_pid))
        try:
            os.kill(self._child_pid, sig)
        except OSError:
            pass
        return self._exit_future

    async def _pong_loop(self):
        """ Periodically writes to keep-alive pipe PONG message."""
        while True:
            await asyncio.sleep(self._worker_timeout / 2.0, loop=self._loop)
            self._pong_stream.write('p')
            self._pong_stream.flush()


class Supervisor:
    """ Controls worker pool.

    :cvar child_factory: callable that instantiates child process handler
    """

    logger = getLogger('aioworkerpool.Supervisor')

    child_factory = ChildHandler

    def __init__(self, worker_factory: WorkerFactory, *,
                 loop: asyncio.AbstractEventLoop = None,
                 workers: typing.Union[int, collections.Iterable] = 2,
                 check_interval: float = 1.0,
                 **kwargs):
        """

        :param worker_factory:
        :param loop: asyncio event loop instance
        :param workers: number of child processes to start
            (or a list of worker ids)
        :param check_interval: periodic pool check interval in seconds
        :param kwargs: keyword arguments passed
        """
        self._kwargs = kwargs
        self._loop = loop or asyncio.get_event_loop()
        self._worker_factory = worker_factory
        self._check_interval = check_interval

        self._check_task = None  # type: asyncio.Task
        self._wait_task = None   # type: asyncio.Task

        if isinstance(workers, int):
            self._workers = workers
            self._pool = {i: None for i in range(workers)}
        elif isinstance(workers, collections.Iterable):
            self._pool = {}
            for i in workers:
                assert isinstance(i, int)
                self._pool[i] = None
            self._workers = len(self._pool)

        else:
            raise ValueError("workers must be int or iterable")
        self._on_start = Signal()
        self._on_shutdown = Signal()
        self._last_check = 0
        self._running = False

    @property
    def loop(self):
        return self._loop

    def on_start(self, callback: Callback):
        """ Appends a callback to a startup callback list."""
        self._on_start.connect(callback)

    def on_shutdown(self, callback: Callback):
        """ Appends a callback to a shutdown callback list."""
        self._on_shutdown.connect(callback)

    def start(self) -> asyncio.Task:
        """ Initializes event loop and prepares infinite pool check loop.

        :returns forever loop task
        """
        self.logger.info("Starting pool")
        self._running = True
        self._loop.run_until_complete(self._on_start.send())
        self.loop.add_signal_handler(signal.SIGINT, self.interrupt)
        self.loop.add_signal_handler(signal.SIGTERM, self.terminate)
        self._check_task = asyncio.Task(self._run_forever_loop(),
                                        loop=self._loop)
        self.logger.info("Pool started")
        return self._check_task

    def stop(self):
        """ Mart supervisor as stopping."""
        self._running = False

    def interrupt(self) -> asyncio.Task:
        """ SIGINT signal handler.

        Interrupts all workers, cleanups event loop and exits process.

        :returns task for stopping workers
        """
        self.stop()
        self.logger.info("Got SIGINT, shutting down workers...")
        self._loop.remove_signal_handler(signal.SIGINT)
        return asyncio.Task(self._shutdown(signal.SIGINT), loop=self._loop)

    def terminate(self):
        """ SIGTERM signal handler.

        Gracefully stops all workers, cleanups event loop and exits process.
        """
        self.stop()
        self.logger.info("Got SIGTERM, shutting down workers...")
        self.loop.remove_signal_handler(signal.SIGTERM)
        return asyncio.Task(self._shutdown(signal.SIGTERM), loop=self._loop)

    async def _shutdown(self, sig):
        """ Shutdowns worker loop.

        Terminates child processes, waits for main loop exit, runs on_shutdown
        callbacks and stops event loop.
        """
        await self._stop_workers(sig)
        if self._check_task:
            await self._check_task
        self.logger.info("Shutting down")
        await self._on_shutdown.send()
        self.loop.stop()
        self.logger.info("Bye!")

    async def _stop_workers(self, sig):
        futures = []
        for worker in filter(None, self._pool.values()):
            futures.append(worker.send_and_wait(sig))
        await asyncio.gather(*futures, loop=self._loop,
                             return_exceptions=True)

    async def _run_forever_loop(self):
        while self._running:
            self.logger.debug("checking pool")
            await self._check_pool()
            now = time.time()
            duration = now - self._last_check
            sleep = max(self._check_interval - duration, 0)
            coro = asyncio.sleep(sleep, loop=self._loop)
            self._wait_task = asyncio.Task(coro, loop=self._loop)
            try:
                await self._wait_task
            except asyncio.CancelledError:
                pass
            self._last_check = time.time()

    def reset_check_interval(self):
        if self._wait_task:
            self._wait_task.cancel()

    async def _check_pool(self) -> int:
        """ Checks if some worker processes are stale or exited.

        Stale processes are killed, exited - restarted, if pool is still
        running.

        :returns: number of alive workers
        """
        alive = 0
        for worker in list(self._pool.values()):
            if worker is None:
                continue
            if not worker.is_stale():
                alive += 1
                continue
            if not worker.child_exists():
                self.logger.warning("Removing worker %s because exited" %
                                    worker)
                self._pool[worker.id] = None
            else:
                self.logger.warning("Removing stale worker %s" % worker)
                await worker.send_and_wait(signal.SIGKILL)

        self.logger.debug("%s alive workers in pool" % alive)

        if not self._running:
            return alive

        for worker_id, handler in self._pool.items():
            if handler is None:
                self.start_worker(worker_id)
                alive += 1
        return alive

    def start_worker(self, worker_id: int):
        """ Starts a new worker for id.

        :param worker_id: logical id of created worker
        """
        worker = self.child_factory(
            worker_id, self._loop, self._worker_factory, **self._kwargs)
        self._pool[worker_id] = worker
        worker.start()

    def add_worker(self, worker_id: int):
        """ Adds a worker with new id and schedules worker startup.

        :raises ValueError: if worker already exists
        """
        if worker_id in self._pool:
            raise ValueError(
                "Worker with id %s is already in pool" % worker_id)

        self._pool[worker_id] = None
        self.reset_check_interval()

    def remove_worker(self, worker_id: int,
                      sig: int=signal.SIGTERM) -> asyncio.Future:
        """ Stops and removes worker with id.

        :param worker_id: worker id to remove
        :param sig: signal to send to child process
        :raises KeyError: if id does not exist.
        :returns: future that happens when child process exits.
        """
        worker = self._pool.pop(worker_id)
        if worker is None:
            f = asyncio.Future(loop=self.loop)
            f.set_result(None)
            return f
        return worker.send_and_wait(sig)

    def main(self, daemonize=False, pidfile='aioworkerpool.pid'):
        """ Supervisor entry point.

        Starts and serves worker pool.

        :param daemonize: option to become a unix daemon
        :param pidfile: path to daemon pid file
        """
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
        """ Initializes daemon context."""
        path = os.path.join(os.getcwd(), pidfile)
        pidfile = TimeoutPIDLockFile(path)
        return DaemonContext(pidfile=pidfile)

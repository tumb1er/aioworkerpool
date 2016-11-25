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
        """

        :param worker_id: worker id in range from 0 to workers
        :param loop: asyncio event loop
        """
        self._worker_id = worker_id
        self._loop = loop
        self._running = False
        self._main_task = None  # type: asyncio.Task
        self._on_start = Signal()
        self._on_shutdown = Signal()
        self._is_shut_down = False
        # Saving internal logger
        self.__logger = self.logger

    @property
    def id(self):
        """ Logical worker id."""
        return self._worker_id

    @property
    def loop(self):
        """ Current event loop instance."""
        return self._loop

    def is_running(self):
        """ Returns worker state, running or terminating.

        :returns: True if worker is in running state
        """
        return self._running

    def on_start(self, callback: Callback):
        """ Appends a callback to a startup callback list."""
        self._on_start.connect(callback)

    def on_shutdown(self, callback: Callback):
        """ Appends a callback to a shutdown callback list."""
        self._on_shutdown.connect(callback)

    def stop(self):
        """ Mark worker as stopping."""
        self._running = False

    def interrupt(self):
        """ SIGINT signal handler.

        Interrupts main(), cleanups event loop and exits child process.
        """
        self.__logger.info("Interrupting...")
        if self._main_task:
            self._main_task.cancel()
        self._shutdown()

    def terminate(self):
        """ SIGTERM signal handler.

        Marks worker as stopping, waits for main() exit, cleanups event loop
        and exits child process.
        """
        self.__logger.info("Terminating...")
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
    def main(self):
        """ Worker infinite loop.

        Must be implemented in descendant classes.
        If main() is a coroutine, called with loop.run_until_completed.
        If not, when main() is called, event loop is not running.
        """
        raise NotImplementedError()

    def start(self):
        """ Worker entry point.

        Runs on_start callbacks, starts main() infinite loop,
        waits for main() exit, then executes on_shutdown callbacks.
        """
        self.__logger.debug("Start worker...")
        self._loop.run_until_complete(self._on_start.send())
        self._running = True
        self.__logger.debug("Worker started")
        if asyncio.iscoroutinefunction(self.main):
            self._main_task = asyncio.Task(self.main(), loop=self._loop)
            self._main_task.add_done_callback(lambda f: self._shutdown())
            self._loop.run_until_complete(self._main_task)
        else:
            self.main()
            self._shutdown()

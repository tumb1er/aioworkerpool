# coding: utf-8
import asyncio
import os
import typing


class BasePipeReader:
    """ Base class for reading from pipes in asyncio."""

    def __init__(self, fd: int, loop: asyncio.AbstractEventLoop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._fd = fd
        self.reader = asyncio.StreamReader(loop=self._loop)
        self._proto = asyncio.StreamReaderProtocol(
            self.reader, loop=self._loop)
        self._transport = None

    async def connect_fd(self):
        """ Connects to pipe() file descriptor as reader."""
        self._transport, _ = await self._loop.connect_read_pipe(
            lambda: self._proto, os.fdopen(self._fd)
        )

    def ready(self):
        return not self.reader.at_eof()


class PipeProxy(BasePipeReader):
    """ Proxies STDOUT/STDERR from child processes to file descriptor."""

    def __init__(self, fd: int, file: typing.io.TextIO=None,
                 loop: asyncio.AbstractEventLoop=None):
        """
        :param fd: pipe file descriptor
        :param file: file to write to
        :param loop: asyncio event loop
        """
        super().__init__(fd, loop=loop)
        self._file = file

    async def read_loop(self):
        while self.ready():
            some_bytes = await self.reader.read(-1)
            self._file.write(some_bytes.decode('utf-8'))
            self._file.flush()


class KeepAlivePipe(BasePipeReader):
    """ Keep-alive mechanics on top of pipes."""

    def __init__(self, fd: int, loop: asyncio.AbstractEventLoop = None,
                 timeout=15.0):
        super().__init__(fd, loop)
        self._timeout = timeout

    async def read_loop(self):
        """
        Marks reader as stale if no bytes are written to pipe for a while.

        :returns: True in case of clean termination or False if timeouted
        """
        while self.ready():
            try:
                await asyncio.wait_for(self.read_pong(), self._timeout,
                                       loop=self._loop)
            except asyncio.TimeoutError:
                return False
        return True

    async def read_pong(self):
        await self.reader.read(1)

# coding: utf-8
import asyncio
import os


class PipeProxy:
    """ Proxies STDOUT/STDERR from child processes to file descriptor."""

    def __init__(self, fd: int, file=None, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._file = file
        self._fd = fd
        self._reader = asyncio.StreamReader(loop=self._loop)
        self._proto = asyncio.StreamReaderProtocol(
            self._reader, loop=self._loop)
        self._transport = None

    async def connect_fd(self):
        """ Connects to pipe() file descriptor as reader."""
        self._transport, _ = await self._loop.connect_read_pipe(
            lambda: self._proto, os.fdopen(self._fd)
        )

    async def read_loop(self):
        while not self._reader.at_eof():
            some_bytes = await self._reader.read(2 ** 16)
            self._file.write(some_bytes.decode('utf-8'))
            self._file.flush()

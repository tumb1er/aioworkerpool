# coding: utf-8
import asyncio
import logging
import pickle
import typing
from base64 import b64encode, b64decode

from aioworkerpool.pipes import PipeProxy


class PickleStreamHandler(logging.StreamHandler):
    """
    Pickle-to-pipe logging handler.

    Formats message record to pickle and writes it to stream.

    :cvar instance: instance of PickleStreamHandler, if initialized
    :param stream: writable file-like object for pickled messages

    """

    instance = None  # type: PickleStreamHandler

    def __init__(self, stream: typing.io.TextIO=None):
        """
        :param stream: writable stream object
        """
        super().__init__(stream)
        if self.instance:
            raise RuntimeError("Can't instantiate second handler instance")
        self.__class__.instance = self

    def format(self, record: logging.LogRecord) -> str:
        """ Formats log record to base64-encoded pickled string.

        :param record: log record

        :returns: encoded string

        """
        return b64encode(pickle.dumps(record)).decode('ascii')


class PicklePipeReader(PipeProxy):
    """
    Reads encoded log records from pipe and re-emits it in master process.

    :param fd: pipe file descriptor
    :param file: file to write to
    :param loop: asyncio event loop
    """

    async def read_loop(self):
        """
        Reads, decodes and re-emits log records in infinite loop.
        """
        terminator = PickleStreamHandler.terminator.encode('ascii')
        while not self.reader.at_eof():
            try:
                data = await self.reader.readuntil(terminator)
            except asyncio.streams.IncompleteReadError:
                break
            record = pickle.loads(b64decode(data))
            logger = logging.getLogger(record.name)
            logger.handle(record)

# coding: utf-8
import asyncio
import pickle
from base64 import b64encode, b64decode
from logging import StreamHandler, getLogger

from aioworkerpool.pipes import PipeProxy


class PickleStreamHandler(StreamHandler):
    """ Formats message record to pickle and writes it to stream."""

    def format(self, record):
        return b64encode(pickle.dumps(record)).decode('ascii')


class PicklePipeReader(PipeProxy):
    async def read_loop(self):
        terminator = PickleStreamHandler.terminator.encode('ascii')
        while not self._reader.at_eof():
            try:
                data = await self._reader.readuntil(terminator)
            except asyncio.streams.IncompleteReadError:
                break
            record = pickle.loads(b64decode(data))
            logger = getLogger(record.name)
            logger.handle(record)

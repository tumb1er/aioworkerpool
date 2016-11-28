# coding: utf-8
import logging
import os
import pickle
from base64 import b64encode
from io import StringIO
from unittest import mock

from aioworkerpool.logging import PickleStreamHandler, PicklePipeReader
from tests import base


class LoggingTestCase(base.TestCaseBase):


    def tearDown(self):
        super().tearDown()
        PickleStreamHandler.instance = None

    def test_pickle_handler(self):
        stream = StringIO()
        handler = PickleStreamHandler(stream=stream)
        logger = logging.getLogger('test')
        logger.addHandler(handler)

        # saving log record for compare
        with mock.patch.object(logger, 'handle') as handle_mock:
            try:
                raise RuntimeError("Test exception")
            except Exception:
                logger.exception("Test exception handling")

        record = handle_mock.call_args[0][0]

        # checking what is written to stream
        logger.handle(record)

        expected = b64encode(pickle.dumps(record)) + b'\n'
        self.assertEqual(stream.getvalue().encode('ascii'), expected)

    def test_pickle_second_init(self):
        stream = StringIO()
        PickleStreamHandler(stream=stream)
        with self.assertRaises(RuntimeError):
            PickleStreamHandler(stream=stream)

    @base.unittest_with_loop
    async def test_pickle_pipe_reader(self):
        r_fd, w_fd = os.pipe()
        log_reader = PicklePipeReader(r_fd, loop=self.loop)
        await log_reader.connect_fd()
        pipe_write_stream = os.fdopen(w_fd, 'w')
        handler = PickleStreamHandler(stream=pipe_write_stream)
        logger = logging.getLogger('test')
        logger.addHandler(handler)

        logger.error("TEST")
        pipe_write_stream.close()
        with mock.patch.object(logger, 'handle') as handle_mock:
            await log_reader.read_loop()
        self.assertEqual(handle_mock.call_count, 1)
        record = handle_mock.call_args[0][0]
        self.assertEqual(record.levelname, "ERROR")
        self.assertEqual(record.msg, "TEST")

        # check read after close
        with mock.patch.object(logger, 'handle') as handle_mock:
            await log_reader.read_loop()

        self.assertFalse(handle_mock.called)



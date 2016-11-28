# coding: utf-8
import os

from io import StringIO

from aioworkerpool import pipes
from tests import base


__all__ = ['PipesTestCase']


class PipesTestCase(base.TestCaseBase):

    @base.unittest_with_loop
    async def test_pipe_proxy(self):
        r_fd, w_fd = os.pipe()
        file = os.fdopen(w_fd, 'w')
        output = StringIO()
        pipe = pipes.PipeProxy(r_fd, file=output, loop=self.loop)
        await pipe.connect_fd()
        self.assertTrue(pipe.ready())
        # check basic pipe connectivity
        file.write('x')
        file.flush()
        data = await pipe.reader.read(1)
        self.assertEqual(data, b'x')

        # check proxying data to file
        file.write('XYZ')
        file.close()
        await pipe.read_loop()
        self.assertEqual(output.getvalue(), 'XYZ')

        # check read after close
        pipe._file = output = StringIO()
        await pipe.read_loop()
        self.assertEqual(output.getvalue(), '')

    @base.unittest_with_loop
    async def test_keep_alive_pipe(self):
        r_fd, w_fd = os.pipe()
        file = os.fdopen(w_fd, 'w')
        pipe = pipes.KeepAlivePipe(r_fd, loop=self.loop, timeout=0.2)
        await pipe.connect_fd()

        def write():
            file.write('b')
            file.close()

        self.loop.call_later(0.1, write)
        result = await pipe.read_loop()
        self.assertTrue(result)

    @base.unittest_with_loop
    async def test_keep_alive_pipe_timeouted(self):
        r_fd, w_fd = os.pipe()
        file = os.fdopen(w_fd, 'w')
        pipe = pipes.KeepAlivePipe(r_fd, loop=self.loop, timeout=0.2)
        await pipe.connect_fd()

        file.write('b')
        # first iteration is ok, but second does not receive any ping data,
        # thus is timeout-ed.
        result = await pipe.read_loop()
        self.assertFalse(result)

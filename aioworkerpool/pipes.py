# coding: utf-8
import asyncio


class PipeProxyProtocol(asyncio.SubprocessProtocol):
    def pipe_data_received(self, fd, data):
        super().pipe_data_received(fd, data)
        print(data)
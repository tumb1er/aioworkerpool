# coding: utf-8
import asyncio
import logging
import os
import socket
import sys

from aiohttp import web

from aioworkerpool import master
from aioworkerpool import worker


async def pong(request):
    return web.Response(text='pong from %s!\n' % os.getpid())


def init_logging():
    h = logging.StreamHandler(sys.stderr)
    h.setLevel(logging.DEBUG)
    h.setFormatter(
        logging.Formatter(
            fmt="[%(process)d] %(name)s %(levelname)s: %(message)s"))
    logging.root.addHandler(h)
    logging.root.setLevel(logging.DEBUG)


def init_worker_logging():
    print(logging.root.handlers)
    global app
    app = web.Application()
    app.router.add_route('GET', '/', pong)


def open_socket():
    global sock
    sock = socket.socket()
    sock.setsockopt(
        socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    sock.bind(('127.0.0.1', 8080))


class WorkerHandler(worker.WorkerBase):
    def __init__(self, worker_id: int, loop: asyncio.AbstractEventLoop):
        super().__init__(worker_id, loop)
        self.on_start(init_worker_logging)
        self.server = None

    async def main(self):
        self.logger.debug("WorkerHandler.main()")
        handler = app.make_handler()
        self.server = await self.loop.create_server(handler, sock=sock)
        await app.startup()
        self.logger.info("Run forever")
        while self.is_running():
            await asyncio.sleep(1)
        self.logger.info("Closing server")

        self.server.close()
        await self.server.wait_closed()
        await app.shutdown()
        await handler.finish_connections(1.0)
        await app.cleanup()


open_socket()


s = master.Supervisor(worker_factory=WorkerHandler,
                      stderr=open('/tmp/stderr.txt', 'w'),
                      stdout=open('/tmp/stdout.txt', 'w'),
                      worker_timeout=3.0,
                      preserve_fds=(sock.fileno(),)
                      )
s.on_start(init_logging)
s.main(daemonize=False, pidfile='/tmp/main.pid')

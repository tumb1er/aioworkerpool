# coding: utf-8
import asyncio
import logging
import socket
import sys

from aiohttp import web

from aioworkerpool import master
from aioworkerpool import worker


async def pong(request):
    return web.Response(text='pong')


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

    def main(self):
        self.logger.debug("MAINSADSD")
        try:
            server = self.loop.create_server(app.make_handler(), sock=sock)
            self.loop.run_until_complete(server)
            self.loop.run_until_complete(app.startup())
        except Exception:
            self.logger.exception(":(")
        else:
            self.logger.info("Run forever")
            self.loop.run_forever()

open_socket()


s = master.Supervisor(worker_factory=WorkerHandler,
                      stderr=open('/tmp/stderr.txt', 'w'),
                      stdout=open('/tmp/stdout.txt', 'w'),
                      worker_timeout=3.0,
                      preserve_fds=(sock.fileno(),)
                      )
s.on_start(init_logging)
s.main(daemonize=False, pidfile='/tmp/main.pid')

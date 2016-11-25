import asyncio
import os
import socket

from aiohttp import web

from aioworkerpool import master, worker


def open_socket():
    # open socket and prepare it to be handled in worker processes
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    s.bind(('127.0.0.1', 8080))
    return s


class WorkerHandler(worker.WorkerBase):
    def __init__(self, worker_id, loop):
        super().__init__(worker_id, loop)
        # add custom initialization on worker startup
        self.on_start(self.init_web_app)
        self.server = self.app = self.handler = None

    async def pong(self, _):
        # aiohttp view handler
        return web.Response(text='pong from #%s %s!\n' %
                                 (self.id, os.getpid()))

    async def init_web_app(self):
        # initialize aiohttp.web.Application instance with new event
        # loop only after child process creation
        self.app = web.Application(loop=self.loop)
        self.app.router.add_route('GET', '/', self.pong)
        # make handler for app
        self.handler = self.app.make_handler()
        # start async tcp server
        self.server = await self.loop.create_server(
            self.handler, sock=sock)
        # call app startup callbacks
        await self.app.startup()

    async def shutdown_web_app(self):
        # close tcp server
        self.server.close()
        # wait for closing
        await self.server.wait_closed()
        # call app shutdown callbacks
        await self.app.shutdown()
        # cleanup connections
        await self.handler.finish_connections(1.0)
        # call app cleanup callbacks
        await self.app.cleanup()

    async def main(self):
        # aiohttp application is already completely initialized and
        # receiving requests, so main loop just checks periodically
        # that worker is still running.
        while self.is_running():
            await asyncio.sleep(1)


# open socket in master process to share it with workeres
sock = open_socket()

supervisor = master.Supervisor(
    worker_factory=WorkerHandler,
    # add opened socket to preserved file descriptor list
    preserve_fds=[sock.fileno()])

supervisor.main()

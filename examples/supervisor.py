#!/usr/bin/env python
import asyncio
import logging
import os
import sys
import time

from aioworkerpool import master, worker


def init_logging():
    h = logging.StreamHandler(sys.stderr)
    h.setLevel(logging.DEBUG)
    h.setFormatter(
        logging.Formatter(
            fmt="[%(process)d] %(name)s %(levelname)s: %(message)s"))
    logging.root.addHandler(h)
    logging.root.setLevel(logging.DEBUG)


def init_worker_logging():
    logging.root.handlers.clear()

async def sleep():
    l = logging.getLogger('test')
    l.info("sleep...")
    await asyncio.sleep(1.0)
    l.info("awaken")


async def worker_shutdown():
    l = logging.getLogger('worker')
    l.info("worker shutdown...")
    await asyncio.sleep(1.0)
    l.info("done")


class WorkerHandler(worker.WorkerBase):
    def __init__(self, worker_id: int, loop: asyncio.AbstractEventLoop):
        super().__init__(worker_id, loop)
        self.on_start(init_worker_logging)
        self.on_shutdown(worker_shutdown)

    async def main(self):
        while self.is_running():
            print(self.id, os.getpid(), time.time())
            self.logger.info("Print")
            await asyncio.shield(asyncio.sleep(1), loop=self.loop)


s = master.Supervisor(worker_factory=WorkerHandler,
                      stderr=open('/tmp/stderr.txt', 'w'),
                      stdout=open('/tmp/stdout.txt', 'w'))
s.on_start(init_logging)
s.on_shutdown(sleep)
s.main(daemonize=False, pidfile='/tmp/main.pid')

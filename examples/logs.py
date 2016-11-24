import asyncio
import logging
import sys

from aioworkerpool import master, worker
from aioworkerpool.logging import PickleStreamHandler


def init_master_logging():
    # Write all logs to stderr
    logging.basicConfig(
        level=logging.DEBUG, stream=sys.stderr,
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s")

    # Our special handler
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter("Custom: %(message)s"))
    l = logging.getLogger("custom")
    l.propagate = False
    l.addHandler(h)


def init_worker_logging():
    # Remove all handlers initialized in master process
    logging.root.handlers.clear()
    # Add proxy handler to our custom logger for passing all messages to master
    # process.
    l = logging.getLogger("custom")
    l.handlers.clear()
    l.addHandler(PickleStreamHandler.instance)


class WorkerHandler(worker.WorkerBase):

    def __init__(self, worker_id: int, loop: asyncio.AbstractEventLoop):
        super().__init__(worker_id, loop)
        # re-initializing logging on
        self.on_start(init_worker_logging)
        self.logger = logging.getLogger("custom")

    async def main(self):
        while self.is_running():
            # self.logger name by default is aioworkerpool.Worker
            self.logger.info("I am here!")
            await asyncio.sleep(1)


# Setup aioworkerpool.master.Supervisor instance
s = master.Supervisor(WorkerHandler)

# Add pre-start signal handler
s.on_start(init_master_logging)

s.main()

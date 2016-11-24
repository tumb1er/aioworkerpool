import asyncio
import os

from aioworkerpool import master, worker


# Add some useful code
class WorkerHandler(worker.WorkerBase):

    async def main(self):
        while self.is_running():
            print("%s: I am here!" % os.getpid())
            await asyncio.sleep(1)


# Setup aioworkerpool.master.Supervisor instance
s = master.Supervisor(WorkerHandler)

# Run worker pool
s.main()

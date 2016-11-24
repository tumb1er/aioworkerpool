.. _aioworkerpool-master:

Master
======

Master process does lots of things.

- Maintains worker process pool
- Redirects worker stdout and stderr streams
- Checks if worker alive
- Collects worker logs
- Handles correct pool termination
- Performs daemonization if necessary

Configuring Supervisor
----------------------

Worker pool parameters are configured with :class:`aioworkerpool.master.Supervisor`.

.. code-block:: python

    master.Supervisor(
        WorkerHandler,
        loop=asyncio.get_event_loop(),
        workers=10,
        check_interval=15,
        **kwargs
    )

Parameters:

- **WorkerHandler** - worker factory, callable that returns an
    :class:`worker.WorkerBase` descendant which implements
    application logics.
- **loop** - asyncio event loop instance, default is :code:`None`
- **workers** - number of child processes to start
- **check_interval** - pool consistency check period in seconds
- **kwargs** - additional :class:`master.ChildHandler` arguments

Configuring ChildHandler
------------------------

Child handler interacts with a single child process. It takes care about
`stdout/stderr` redirection, logging and keepalive. :code:`Supervisor` calls
:code:`Supervisor.child_factory` with these arguments:

.. code-block:: python

    worker = self.child_factory(
            worker_id,
            self._loop,
            self._worker_factory,
            **self._kwargs)

- **worker_id** - worker id in range from :code:`0` to :code:`workers`
- **loop** - asyncio event loop instance
- **worker_factory** - worker factory callable
- **kwargs** - extra keyword arguments including:

    - **stderr** - file-like object to redirect `stderr` from workers
    - **stdout** - file-like object to redirect `stdout` from workers
    - **worker_timeout** - worker keep-alive timeout in seconds
    - **preserve_fds** - list of file descriptors to preserve on :code:`fork()`


Module reference
----------------

.. automodule:: aioworkerpool.master
.. autoclass:: aioworkerpool.master.Supervisor
    :members:

.. autoclass:: aioworkerpool.master.ChildHandler
    :members:



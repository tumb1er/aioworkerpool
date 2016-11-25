.. _aioworkerpool-worker:

Worker
======

On the child process side all the fun starts from :meth:`worker.WorkerBase.main`
method. After `fork()` child process:

- closes parent event loop, disconnects signal handlers from it;
- creates new event loop
- calls :meth:`worker.WorkerBase.on_start` callbacks
- calls :meth:`worker.WorkerBase.main`
- on :code:`main()` exit, calls :meth:`worker.WorkerBase.on_shutdown` callbacks
- cleanups event loop and exits child process.

Quickstart
----------

1. Inherit from :class:`worker.WorkerBase` and implement :meth:`main()`
   coroutine or method. It should be a forever loop with periodic
   :meth:`WorkerBase.is_running()` check.
2. Add initialization callbacks with :meth:`worker.WorkerBase.on_start`


.. warning::

   Don't initialize anything related to ``asyncio`` before :code:`on_start`
   callbacks, parent event loop will be used and after closing it child
   process will just do nothing.

3. Cleanup callbacks should be added with :meth:`worker.WorkerBase.on_shutdown`.
4. Don't forget to re-initialize logging, if necessary.

Child termination
-----------------

If got ``SIGTERM`` signal, :meth:`worker.WorkerBase.stop` is called to mark
worker as not running. After that worker waits for :code:`main()` and executes
shutdown sequence.

.. warning::

    It is on a developer to periodically check and gracefully stop
    :code:`main()` loop if :meth:`worker.WorkerBase.is_running`
    returns :code:`False`.

If got ``SIGINT`` signal, worker cancels :code:`main()` if it is a coroutine.
If not, worker just executes shutdown sequence:

- schedules :meth:`worker.WorkerBase.on_shutdown` callbacks;
- schedules :meth:`asyncio.AbstractEventLoop.stop`;
- closes event loop and exists child process.

Example
-------

(see `sockets.py <https://github.com/tumb1er/aioworkerpool/blob/master/examples/sockets.py>`_)

.. literalinclude:: ../examples/sockets.py
    :language: python

Module reference
----------------

.. automodule:: aioworkerpool.worker
.. autoclass:: aioworkerpool.worker.WorkerBase
    :members:

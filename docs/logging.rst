.. _aioworkerpool-logging:

Logging
=======

Aioworkerpool uses standart :mod:`logging` library to track pool activity.

It defines several loggers:

- ``'aioworkerpool.Supervisor'`` to track process pool activity,
- ``'aioworkerpool.Handler'`` to track invididual child process behabior,
- ``'aioworkerpool.Worker'`` to track what happens in child process.

Default logging setup
---------------------

By default, `master process` does not configure any logging handlers.
A `worker process` instead configures :class:`aioworkerpool.logging.PickleStreamHandler`
instance that proxies all messages to master process via pipe.
This handler is automatically added to ``'aioworkerpool'`` logger. It `pickles`
log record content and pass it to master process. At master process record
is reconstructed and re-emitted with initial logger.

Custom logging
--------------

**Master process** may configure logging before `supervisor` startup, or while it
is starting.

.. warning::

    On start **worker process** close all file descriptors except
    `stdout/stderr`, so it's on developer to close all file/stream handlers or
    add corresponding file descriptors to preserved descriptors list.

**Worker process** should re-configure logging on startup phase. It may use
:attr:`PickleStreamHandler.instance` to reuse proxy handler.

Example
-------

.. literalinclude:: ../examples/logs.py
    :language: python

Module reference
----------------

.. automodule:: aioworkerpool.logging
.. autoclass:: aioworkerpool.logging.PickleStreamHandler
    :members:

.. autoclass:: aioworkerpool.logging.PicklePipeReader
    :members:




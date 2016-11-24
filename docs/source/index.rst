.. aioworkerpool documentation master file, created by
   sphinx-quickstart on Thu Nov 24 10:00:56 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

aioworkerpool: async worker pool
================================

Master/Worker pool for `asyncio` (:pep:`3156`).

.. _Github: https://github.com/tumb1er/aioworkerpool

Features
--------

- Uses `fork()` to reuse python interpreter
- Supports proxying of `stdout`/`stderr` from workers to files
- Provides multiprocess-aware logging transport
- Supports file descriptor preserving
- May run as UNIX daemon

Library installation
--------------------

.. code-block:: bash

    $ pip install aioworkerpool

Getting started
---------------

Example (see `quickstart.py <https://github.com/tumb1er/aioworkerpool/blob/master/examples/quickstart.py>`_)

.. literalinclude:: ../../examples/quickstart.py

Running examples:

.. code-block:: bash

    $ cd aioworkerpool;
    $ python -m examples.quickstart

Source code
-----------

- Hosted on `GitHub <https://github.com/tumb1er/aioworkerpool>`_.
- Uses `Travis <https://travis-ci.org>`_ for continuous integration.
- Documentation on `ReadTheDocs <https://readthedocs.org>`_.

Dependencies
------------

- POSIX operation system
- Python 3.5+ (basically, `asyncio` and `async/await` syntax)
- `python-daemon <https://pypi.python.org/pypi/python-daemon>`_

Current development status
--------------------------

Library is under active development so API may change dramatically.

Contents
--------

.. toctree::
   :maxdepth: 2



Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


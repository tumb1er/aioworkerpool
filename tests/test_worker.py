# coding: utf-8
from random import randint

import asyncio
from unittest import mock

from tests import base


__all__ = ['WorkerTestCase']


class WorkerTestCase(base.TestCaseBase):

    def setUp(self):
        super().setUp()
        self.worker = base.TestWorker(worker_id=0, loop=self.loop)

    def test_worker_init(self):
        worker_id = randint(0, 100)
        worker = base.TestWorker(worker_id=worker_id, loop=self.loop)
        self.assertEqual(worker.id, worker_id)
        self.assertFalse(worker.is_running())
        self.assertIs(worker.loop, self.loop)
        self.assertIn(worker.start_cb, worker._on_start._callbacks)
        self.assertIn(worker.shutdown_cb, worker._on_shutdown._callbacks)

    def test_worker_stop(self):
        self.worker._running = True
        self.assertTrue(self.worker.is_running())
        self.worker.stop()
        self.assertFalse(self.worker.is_running())

    def test_worker_interrupt(self):
        f = self.worker.interrupt()
        self.assertIsInstance(f, asyncio.Future)
        self.loop.run_until_complete(f)
        self.assertTrue(self.worker.shutdown_cb_called)

    def test_worker_interrupt_running(self):
        self.worker._running = True
        self.worker._main_task = mock.MagicMock()
        f = self.worker.interrupt()
        self.assertIsInstance(f, asyncio.Future)
        self.loop.run_until_complete(f)
        self.assertTrue(self.worker.shutdown_cb_called)
        cancel_main_mock = self.worker._main_task.cancel  # type: mock.MagicMock
        self.assertTrue(cancel_main_mock.called)

    def test_worker_terminate(self):
        self.worker.terminate()
        self.assertFalse(self.worker.is_running())

    def test_worker_terminate_running(self):
        self.worker._running = True
        self.worker._main_task = mock.MagicMock()
        self.worker.terminate()
        self.assertFalse(self.worker.is_running())
        add_cb_mock = self.worker._main_task.add_done_callback
        """:type add_cb_mock: mock.MagicMock"""

        add_cb_mock.assert_called_once_with(self.worker._shutdown)

    def test_worker_exit(self):
        with mock.patch('sys.exit') as exit_mock:
            with mock.patch.object(self.worker.loop, 'close') as close_mock:
                self.worker._exit()
        close_mock.assert_called_once_with()
        exit_mock.assert_called_once_with(0)

    def test_worker_shutdown(self):
        send_call = object()
        self.worker._on_shutdown.send = mock.MagicMock(return_value=send_call)

        send_task = mock.MagicMock()
        with mock.patch('asyncio.Task', return_value=send_task) as task_mock:
            f = self.worker._shutdown()

        self.assertIsInstance(f, asyncio.Future)
        self.assertIs(f._loop, self.loop)

        # worker scheduled self._on_shutdown.send() coroutine
        task_mock.assert_called_once_with(send_call, loop=self.worker.loop)

        # worker added self._stop_loop() callback to send() task
        add_cb_mock = send_task.add_done_callback  # type: mock.MagicMock
        add_cb_mock.assert_called_once_with(self.worker._stop_loop)

        # second call returns same future object
        f2 = self.worker._shutdown()
        self.assertIs(f, f2)

    def test_worker_start(self):
        f = self.worker.start()
        self.assertIsInstance(f, asyncio.Future)
        self.loop.run_until_complete(f)
        self.assertTrue(self.worker.start_cb_called)
        self.assertTrue(self.worker.shutdown_cb_called)

    def test_worker_sync_main(self):
        main = mock.MagicMock(return_value=None)
        main._is_coroutine = False
        self.worker.main = main
        f = self.worker.start()
        self.assertTrue(f.done())
        self.assertTrue(self.worker.shutdown_cb_called)

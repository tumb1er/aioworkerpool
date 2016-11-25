# coding: utf-8
import signal
from random import randint

import asyncio
from unittest import mock

from aioworkerpool import master
from tests import base


__all__ = ['SupervisorTestCase']


class SupervisorTestCase(base.TestCaseBase):
    def setUp(self):
        super().setUp()
        self.supervisor = base.TestSupervisor(base.TestWorker, loop=self.loop,
                                              check_interval=0.1)
        self.handler_patcher = mock.patch(
            'aioworkerpool.master.ChildHandler.start')
        self.handler_start = self.handler_patcher.start()

    def tearDown(self):
        super().tearDown()
        self.handler_patcher.stop()

    def test_supervisor_init(self):
        workers = randint(0, 10)
        interval = randint(0, 10)
        s = base.TestSupervisor(base.TestWorker, loop=self.loop,
                                workers=workers,
                                check_interval=interval,
                                custom_args='custom_value')
        self.assertIs(s._worker_factory, base.TestWorker)
        self.assertIs(s.loop, self.loop)
        self.assertEqual(s._workers, workers)
        self.assertEqual(s._check_interval, interval)
        self.assertDictEqual(s._kwargs, {'custom_args': 'custom_value'})
        self.assertIn(s.start_cb, s._on_start._callbacks)
        self.assertIn(s.shutdown_cb, s._on_shutdown._callbacks)

    def test_start(self):
        run_coro = object()
        check_task = object()
        with mock.patch.object(self.loop, 'add_signal_handler') \
                as add_sig_mock:  # type: mock.MagicMock
            with mock.patch.object(self.supervisor, '_run_forever_loop',
                                   return_value=run_coro):
                with mock.patch(
                        'asyncio.Task', return_value=check_task) as task_mock:
                    t = self.supervisor.start()
        self.assertIs(t, check_task)
        task_mock.assert_called_once_with(run_coro, loop=self.loop)
        self.assertEqual(add_sig_mock.call_count, 2)
        self.assertIn(mock.call(signal.SIGINT, self.supervisor.interrupt),
                      add_sig_mock.call_args_list)
        self.assertIn(mock.call(signal.SIGTERM, self.supervisor.terminate),
                      add_sig_mock.call_args_list)
        self.assertTrue(self.supervisor.start_cb_called)

    def test_stop(self):
        self.supervisor._running = True
        self.supervisor.stop()
        self.assertFalse(self.supervisor._running)

    def test_interrupt(self):
        self.supervisor._running = True
        stop_coro = object()
        stop_task = object()
        with mock.patch.object(self.loop, 'remove_signal_handler') \
                as remove_sig_mock:  # type: mock.MagicMock
            with mock.patch.object(self.supervisor, '_shutdown',
                                   return_value=stop_coro)\
                    as stop_mock:  # type: mock.MagicMock
                with mock.patch(
                        'asyncio.Task', return_value=stop_task) as task_mock:
                    t = self.supervisor.interrupt()
        stop_mock.assert_called_once_with(signal.SIGINT)
        self.assertIs(t, stop_task)
        task_mock.assert_called_once_with(stop_coro, loop=self.loop)
        remove_sig_mock.assert_called_once_with(signal.SIGINT)

    def test_terminate(self):
        self.supervisor._running = True
        stop_coro = object()
        stop_task = object()
        with mock.patch.object(self.loop, 'remove_signal_handler') \
                as remove_sig_mock:  # type: mock.MagicMock
            with mock.patch.object(self.supervisor, '_shutdown',
                                   return_value=stop_coro) \
                    as stop_mock:  # type: mock.MagicMock
                with mock.patch(
                        'asyncio.Task', return_value=stop_task) as task_mock:
                    t = self.supervisor.terminate()
        stop_mock.assert_called_once_with(signal.SIGTERM)
        self.assertIs(t, stop_task)
        task_mock.assert_called_once_with(stop_coro, loop=self.loop)
        remove_sig_mock.assert_called_once_with(signal.SIGTERM)

    @base.unittest_with_loop
    async def test_shutdown(self):
        done_future = asyncio.Future(loop=self.loop)
        done_future.set_result(None)
        with mock.patch.object(self.supervisor, '_stop_workers',
                               return_value=done_future) as stop_workers_mock:
            with mock.patch.object(self.loop, 'stop') \
                    as stop_mock:  # type: mock.MagicMock
                await self.supervisor._shutdown(signal.SIGTERM)
        stop_workers_mock.assert_called_once_with(signal.SIGTERM)
        stop_mock.assert_called_once_with()
        self.assertTrue(self.supervisor.shutdown_cb_called)

    @base.unittest_with_loop
    async def test_wait_check_task_on_shutdown(self):
        with mock.patch.object(self.loop, 'stop') \
                as stop_mock:  # type: mock.MagicMock
            f = asyncio.Future(loop=self.loop)
            self.supervisor._check_task = f
            # checks that _shutdown blocks until self._check_task
            with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.supervisor._shutdown(signal.SIGTERM), 0.1,
                        loop=self.loop)

            f = asyncio.Future(loop=self.loop)
            self.supervisor._check_task = f
            f.set_result(None)
            await self.supervisor._shutdown(signal.SIGTERM)
            # checks that _shutdown continues after self._check_task is done
            await asyncio.wait_for(self.supervisor._shutdown(signal.SIGTERM),
                                   0.1, loop=self.loop)
        self.assertEqual(stop_mock.call_count, 2)



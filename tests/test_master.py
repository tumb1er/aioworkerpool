# coding: utf-8
import asyncio
import os
import signal
from contextlib import ExitStack
from random import randint, random
from unittest import mock

import sys

from aioworkerpool import pipes
from aioworkerpool.logging import PicklePipeReader
from aioworkerpool.master import ChildHandler
from tests import base

__all__ = ['SupervisorTestCase', 'ChildHandlerTestCase']


class SupervisorTestCase(base.TestCaseBase):
    def setUp(self):
        super().setUp()
        self.supervisor = base.TestSupervisor(base.TestWorker, loop=self.loop,
                                              check_interval=0.1,
                                              stdout=None, stderr=None)
        self.handler_patcher = mock.patch(
            'aioworkerpool.master.ChildHandler.start')
        self.start_mock = self.handler_patcher.start()

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
        self.assertTrue(self.supervisor._running)

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
                                   return_value=stop_coro) \
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

    @base.unittest_with_loop
    async def test_stop_workers(self):
        done_future = asyncio.Future(loop=self.loop)
        done_future.set_result(None)

        worker = mock.MagicMock()
        self.supervisor._pool[0] = worker
        sig = randint(0, 10)
        with mock.patch.object(worker, 'send_and_wait',
                               return_value=done_future) as worker_mock:
            await self.supervisor._stop_workers(sig)

        worker_mock.assert_called_once_with(sig)

    @base.unittest_with_loop
    async def test_run_forever_loop(self):
        self.supervisor._running = True
        done_future = asyncio.Future(loop=self.loop)
        done_future.set_result(None)

        def check_pool():
            self.supervisor._running = False
            return done_future

        with mock.patch.object(self.supervisor, '_check_pool',
                               side_effect=check_pool) as check_mock:
            await self.supervisor._run_forever_loop()

        check_mock.assert_called_once_with()

    def test_reset_check_interval(self):
        self.assertIsNone(self.supervisor._wait_task)
        self.supervisor.reset_check_interval()

        self.supervisor._wait_task = wait_task_mock = mock.MagicMock()
        self.supervisor.reset_check_interval()
        wait_task_mock.cancel.assert_called_once_with()

    @base.unittest_with_loop
    async def test_sleep_in_forever_loop(self):
        self.supervisor._running = True
        done_future = asyncio.Future(loop=self.loop)
        done_future.set_result(None)

        def check_pool():
            self.supervisor._running = False
            return done_future

        sleep_coro = object()

        with ExitStack() as stack:
            check_task = stack.enter_context(
                mock.patch.object(self.supervisor, '_check_pool',
                                  side_effect=check_pool))
            sleep_mock = stack.enter_context(
                mock.patch('asyncio.sleep', return_value=sleep_coro))
            task_mock = stack.enter_context(
                mock.patch('asyncio.Task', return_value=done_future))

            await self.supervisor._run_forever_loop()

        check_task.assert_called_once_with()
        self.assertTrue(sleep_mock.called)
        task_mock.assert_called_once_with(sleep_coro, loop=self.loop)

        # Check that in run_forever_loop() asyncio.Task(asyncio.sleep) is
        # saved in self._wait_task for supervisor
        self.assertIs(self.supervisor._wait_task, done_future)

    @base.unittest_with_loop
    async def test_sleep_cancellation(self):
        self.supervisor._running = True
        self.loop.call_later(0.1, lambda: self.supervisor._wait_task.cancel())
        await self.supervisor._run_forever_loop()

    def _init_worker(self, stale=False, exists=True, worker_id=None, add=True):
        if worker_id is None:
            worker_id = 0
        worker = mock.MagicMock()
        worker.is_stale = lambda: stale
        worker.child_exists = lambda: exists
        worker.id = worker_id
        if add:
            self.supervisor._pool[worker.id] = worker
            self.supervisor._workers = len(self.supervisor._pool)
        return worker

    @base.unittest_with_loop
    async def test_check_loop_remove_exited(self):
        self.supervisor._running = True
        worker = self._init_worker(True, False)
        with mock.patch.object(self.supervisor, 'start_worker') as start_mock:
            await self.supervisor._check_pool()

        start_mock.assert_called_once_with(worker.id)

    @base.unittest_with_loop
    async def test_check_loop_kill_stale(self):
        self.supervisor._running = True
        worker = self._init_worker(True, True)

        def send_and_wait(_):
            done_future = asyncio.Future(loop=self.loop)
            done_future.set_result(None)
            self.supervisor._pool.pop(worker.id)
            return done_future

        with mock.patch.object(self.supervisor, 'start_worker') as start_mock:
            with mock.patch.object(worker, 'send_and_wait',
                                   side_effect=send_and_wait) as kill_mock:
                await self.supervisor._check_pool()

        kill_mock.assert_called_once_with(signal.SIGKILL)
        start_mock.assert_called_once_with(worker.id)

    @base.unittest_with_loop
    async def test_check_loop_alive_workers(self):
        self.supervisor._running = True
        # if worker.is_stale returns False, other checks are skipped
        worker = self._init_worker(False, False)
        done_future = asyncio.Future(loop=self.loop)
        done_future.set_result(None)

        with mock.patch.object(self.supervisor, 'start_worker') as start_mock:
            with mock.patch.object(worker, 'send_and_wait',
                                   return_value=done_future) as kill_mock:
                await self.supervisor._check_pool()

        self.assertDictEqual(self.supervisor._pool, {worker.id: worker})
        self.assertFalse(kill_mock.called)
        self.assertFalse(start_mock.called)

    @base.unittest_with_loop
    async def test_check_loop_not_running(self):
        self.supervisor._running = False
        worker = self._init_worker(True, False)
        with mock.patch.object(self.supervisor, 'start_worker') as start_mock:
            await self.supervisor._check_pool()

        self.assertNotIn(worker.id, self.supervisor._pool)
        self.assertFalse(start_mock.called)

    @base.unittest_with_loop
    async def test_check_loop_not_enough_workers(self):
        self.supervisor._running = True
        worker = self._init_worker(False, False, worker_id=2)
        self.supervisor._workers = 3
        with mock.patch.object(self.supervisor, 'start_worker') \
                as start_mock:  # type: mock.MagicMock
            await self.supervisor._check_pool()

        self.assertIn(worker.id, self.supervisor._pool)
        # worker started for missing IDs
        self.assertEqual(start_mock.call_count, 2)
        self.assertIn(mock.call(0), start_mock.call_args_list)
        self.assertIn(mock.call(1), start_mock.call_args_list)

    def test_start_worker(self):
        worker = self._init_worker()
        with mock.patch.object(
                self.supervisor, 'child_factory', return_value=worker) \
                as cf_mock:  # type: mock.MagicMock
            self.supervisor.start_worker(worker.id)

        cf_mock.assert_called_once_with(worker.id, self.loop,
                                        self.supervisor._worker_factory,
                                        **self.supervisor._kwargs)
        worker.start.assert_called_once_with()

    def test_daemon_context(self):
        lock_obj = object()
        ctx_obj = object()
        with mock.patch('aioworkerpool.master.TimeoutPIDLockFile',
                        return_value=lock_obj) \
                as lock_mock:  # type: mock.MagicMock
            with mock.patch('aioworkerpool.master.DaemonContext',
                            return_value=ctx_obj) \
                    as dm_mock:  # type: mock.MagicMock
                ctx = self.supervisor.get_daemon_context('pidfile.txt')

        self.assertIs(ctx, ctx_obj)
        dm_mock.assert_called_once_with(pidfile=lock_obj)
        lock_mock.assert_called_once_with(
            os.path.join(os.getcwd(), 'pidfile.txt'))

    def test_main(self):
        self.supervisor._loop = loop_mock = mock.MagicMock()
        with mock.patch.object(self.supervisor, 'start') \
                as start_mock:  # type: mock.MagicMock
            self.supervisor.main()
        start_mock.assert_called_once_with()
        loop_mock.run_forever.assert_called_once_with()
        loop_mock.close.assert_called_once_with()

    def test_main_daemon(self):
        lock_obj = object()
        ctx_obj = mock.MagicMock()
        with mock.patch('aioworkerpool.master.TimeoutPIDLockFile',
                        return_value=lock_obj) \
                as lock_mock:  # type: mock.MagicMock
            with mock.patch('aioworkerpool.master.DaemonContext',
                            return_value=ctx_obj) \
                    as dm_mock:  # type: mock.MagicMock
                with mock.patch.object(self.supervisor, '_main') \
                        as start_mock:  # type: mock.MagicMock
                    self.supervisor.main(daemonize=True, pidfile='pid.txt')
        lock_mock.assert_called_once_with(os.path.join(os.getcwd(), 'pid.txt'))
        dm_mock.assert_called_once_with(pidfile=lock_obj)
        ctx_obj.__enter__.assert_called_once_with()
        start_mock.assert_called_once_with()
        ctx_obj.__exit__.assert_called_once_with(None, None, None)

    def test_main_no_daemon(self):
        with mock.patch.object(self.supervisor, 'get_daemon_context') \
                as ctx_mock:  # type: mock.MagicMock
            with mock.patch.object(self.supervisor, '_main') \
                    as start_mock:  # type: mock.MagicMock
                self.supervisor.main(daemonize=False)

        start_mock.assert_called_once_with()
        self.assertFalse(ctx_mock.called)


class ChildHandlerTestCase(base.TestCaseBase):
    def setUp(self):
        super().setUp()
        self.fork_patcher = mock.patch('os.fork', side_effect=self.fork)
        self.fork_mock = self.fork_patcher.start()
        self.fork_to_child = False
        self.worker_id = randint(0, 10)
        self.child_pid = None
        self.handler = ChildHandler(self.worker_id, self.loop,
                                    base.TestWorker)

    def tearDown(self):
        super().tearDown()
        self.fork_patcher.stop()

    def fork(self):
        self.child_pid = 0 if self.fork_to_child else randint(10000, 20000)
        return self.child_pid

    def test_init_handler(self):
        stdout = object()
        stderr = object()
        fd = randint(20, 30)
        worker_timeout = random()
        handler = ChildHandler(self.worker_id, self.loop, base.TestWorker,
                               stdout=stdout, stderr=stderr,
                               worker_timeout=worker_timeout,
                               preserve_fds=(fd,))
        self.assertEqual(handler.id, self.worker_id)
        self.assertIs(handler.loop, self.loop)
        self.assertIs(handler._worker_factory, base.TestWorker),
        self.assertIs(handler._stdout, stdout)
        self.assertIs(handler._stderr, stderr)
        self.assertEqual(handler._worker_timeout, worker_timeout)
        self.assertTupleEqual(handler._preserve_fds, (fd,))

    def test_is_running(self):
        self.assertFalse(self.handler.is_running())
        self.handler._worker = mock.MagicMock()
        self.handler._worker.is_running = lambda: False
        self.assertFalse(self.handler.is_running())
        self.handler._worker.is_running = lambda: True
        self.assertTrue(self.handler.is_running())

    def test_child_exists(self):
        self.assertFalse(self.handler.child_exists())
        self.handler._child_pid = randint(10000, 20000)
        self.handler._child_exit_code = None
        self.assertTrue(self.handler.child_exists())
        self.handler._child_exit_code = 0
        self.assertFalse(self.handler.child_exists())

    def test_is_stale(self):
        self.handler._alive = True
        # no child pid
        self.assertTrue(self.handler.is_stale())
        self.handler._child_pid = randint(10000, 20000)
        # child pid exists and alive flag
        self.assertFalse(self.handler.is_stale())
        # alive flag is missing
        self.handler._alive = False
        self.assertTrue(self.handler.is_stale())

    def test_fork_as_parent(self):
        self.fork_to_child = False
        self.handler._stdout = object()
        self.handler._stderr = object()
        fds = [object() for _ in range(8)]
        pipe_pairs = [fds[0: 2], fds[2: 4], fds[4: 6], fds[6: 8]]

        with mock.patch('os.pipe', side_effect=pipe_pairs) as pipe_mock:
            with mock.patch('os.close') as close_mock:
                self.assertTrue(self.handler.fork())

        self.assertEqual(self.handler._child_pid, self.child_pid)

        self.assertEqual(pipe_mock.call_count, 4)
        self.assertEqual(close_mock.call_count, 4)
        # all "write" ends of fds pairs are closed in parent process
        close_calls = [mock.call(fd) for fd in fds[1::2]]
        self.assertListEqual(close_mock.call_args_list, close_calls)

        # read-sides of pipes are connected to corresponding handlers
        self.assertIsInstance(self.handler._stdout_pipe, pipes.PipeProxy)
        self.assertIs(self.handler._stdout_pipe._fd, fds[0])
        self.assertIs(self.handler._stdout_pipe._file, self.handler._stdout)
        self.assertIs(self.handler._stdout_pipe._loop, self.loop)

        self.assertIsInstance(self.handler._stderr_pipe, pipes.PipeProxy)
        self.assertIs(self.handler._stderr_pipe._fd, fds[2])
        self.assertIs(self.handler._stderr_pipe._file, self.handler._stderr)
        self.assertIs(self.handler._stderr_pipe._loop, self.loop)

        self.assertIsInstance(self.handler._logging_pipe, PicklePipeReader)
        self.assertIs(self.handler._logging_pipe._fd, fds[4])
        self.assertIs(self.handler._logging_pipe._loop, self.loop)

        self.assertIsInstance(self.handler._keepalive_pipe, pipes.KeepAlivePipe)
        self.assertIs(self.handler._keepalive_pipe._fd, fds[6])
        self.assertIs(self.handler._keepalive_pipe._loop, self.loop)
        self.assertEqual(self.handler._keepalive_pipe._timeout,
                         self.handler._worker_timeout)

    def test_fork_as_child(self):
        self.fork_to_child = True
        self.handler._stdout = object()
        self.handler._stderr = object()
        fds = [object() for _ in range(8)]
        pipe_pairs = [fds[0: 2], fds[2: 4], fds[4: 6], fds[6: 8]]

        fd_obj = object()

        with ExitStack() as stack:
            stack.enter_context(mock.patch('os.pipe', side_effect=pipe_pairs))
            dup_mock = stack.enter_context(mock.patch('os.dup2'))
            fdopen_mock = stack.enter_context(
                mock.patch('os.fdopen', return_value=fd_obj))
            close_mock = stack.enter_context(mock.patch('os.close'))
            logging_mock = stack.enter_context(mock.patch.object(
                self.handler, 'init_worker_logging'))
            self.assertFalse(self.handler.fork())

        self.assertFalse(self.handler._child_pid)

        # stdout/stderr are replaced with write-sides of corresponding pipes
        dup_calls = [
            mock.call(fds[1], sys.stdout.fileno()),
            mock.call(fds[3], sys.stderr.fileno())
        ]
        self.assertListEqual(dup_mock.call_args_list, dup_calls)

        # stdout/stderr pipes are completely closed
        close_calls = [mock.call(fd) for fd in fds[0:4]]
        # also read sides of logging and keepalive pipes are closed
        close_calls.extend([
            mock.call(fds[4]), # r_logging
            mock.call(fds[6])  # r_keepalive
        ])

        self.assertListEqual(close_mock.call_args_list, close_calls)

        logging_mock.assert_called_once_with(fds[5]) # w_logging

        fdopen_mock.assert_called_once_with(fds[7], 'w')  # w_keepalive
        self.assertIs(self.handler._pong_stream, fd_obj)




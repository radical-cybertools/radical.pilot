#!/usr/bin/env python3

import io
import os
import sys
import time
import queue


import radical.utils   as ru
import radical.pilot   as rp

import multiprocessing as mp
import threading       as mt

import dragon                      # pylint: disable=unused-import, import-error
mp.set_start_method("dragon")

from dragon.native.process       import Popen
from dragon.native.process       import Process
from dragon.native.process       import ProcessTemplate
from dragon.native.process_group import ProcessGroup


# ------------------------------------------------------------------------------
#
def get_stdio(conn) -> str:

    data = ''
    try:
        while True:
            data += conn.recv()

    except EOFError:
        return data

    finally:
        conn.close()


# ------------------------------------------------------------------------------
#
class Handler(object):
    '''
    Abstract handler to watch for dragon things completing.  Implementations
    will wrap around dragon's `ProcessGroup`s and
    `mp.Pool.apply_async.AsyncResult` objects.
    '''

    def is_alive(self):
        raise NotImplementedError('is_alive not implemented')

    def get_results(self):
        raise NotImplementedError('get_results not implemented')


# ------------------------------------------------------------------------------
#
class AsyncResultHandler(Handler):

    def __init__(self, res):
        self._res = res

    def is_alive(self):
        return not self._res.ready()


    # --------------------------------------------------------------------------
    #
    def get_results(self):

        assert not self.is_alive(), 'task still running'

        out = ''
        err = ''
        ret = 0
        val = None
        exc = None

        try:
            out, err, ret, val, exc = self._res.get()

        except Exception as e:
            err = repr(e)
            exc = repr(e)
            ret = -1

        return out, err, ret, val, exc


# ------------------------------------------------------------------------------
#
class GroupHandler(Handler):

    # --------------------------------------------------------------------------
    #
    def __init__(self, group):
        self._group = group

    def is_alive(self):
        return not self._group.inactive_puids


    # --------------------------------------------------------------------------
    #
    def get_results(self):

        assert not self.is_alive(), 'group still running'

        out = ''
        err = ''
        ret = 0
        val = None
        exc = None

        try:

            for item in self._group.inactive_puids:

                proc = Process(None, ident=item[0])
                out  += get_stdio(proc.stdout_conn)
                err  += get_stdio(proc.stderr_conn)

                if item[1] != 0:
                    ret = item[1]

            self._group.close()
            self._group = None

        except Exception as e:
            err = repr(e)
            exc = e
            ret = -1

        return out, err, ret, val, exc


# ------------------------------------------------------------------------------
#
class Server(object):

    # FIXME: profile events

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._uid = ru.generate_id('radical.pilot.dragon')

        self._log  = ru.Logger(self._uid)
        self._prof = ru.Profiler(self._uid, path='.')
        self._term = mt.Event()

        self._log.info('start dragon executor server %s', self._uid)

        self._pwatcher = ru.PWatcher(uid='%s.pw' % self._uid, log=self._log)
        self._pwatcher.watch(os.getpid())
        self._pwatcher.watch(os.getppid())

        self._pipe_in  = None
        self._pipe_out = None

        self._pool  = None
      # self._slots = mp.cpu_count()
        self._slots = int(sys.argv[1])
        self._free  = self._slots

        self._watcher_queue = queue.Queue()
        self._logger_queue  = mp.Queue()

        self._logger = mt.Thread(target=self._logger_thread)
        self._logger.daemon = True
        self._logger.start()

        self._watcher = mt.Thread(target=self._watcher_thread)
        self._watcher_event  = mt.Event()
        self._watcher.daemon = True
        self._watcher.start()

        self._worker = mt.Thread(target=self._worker_thread)
        self._worker_event  = mt.Event()
        self._worker.daemon = True
        self._worker.start()

        self._worker_event.wait(timeout=5)
        self._watcher_event.wait(timeout=5)

        if not self._worker_event.is_set():
            raise RuntimeError('launcher thread did not start')

        if not self._watcher_event.is_set():
            raise RuntimeError('watcher thread did not start')

        assert self._pipe_in,  'pipe_in not set'
        assert self._pipe_out, 'pipe_out not set'

        url_in  = ru.Url(ru.as_string(self._pipe_in.url))
        url_out = ru.Url(ru.as_string(self._pipe_out.url))

        url_in.host  = ru.get_hostip()
        url_out.host = ru.get_hostip()

        msg = 'ZMQ_ENDPOINTS %s %s\n' % (str(url_in), str(url_out))
        self._log.debug('URLs: %s', msg)
        sys.stdout.write(msg)
        sys.stdout.flush()

        # run forever
        while not self._term.is_set():
            time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def _worker_thread(self):
        '''
        handle incoming requests from the parent process

        The requests are expected to be dictionaries with the following keys:
            - cmd:  the command to execute (run, cancel, stop)
            - task: the task to execute the command on
        '''

        self._log.info('start launcher')

        self._pipe_in  = ru.zmq.Pipe(ru.zmq.MODE_PULL)
        self._worker_event.set()

        self._pool = mp.Pool(self._slots)

        try:
            while True:

                msg = self._pipe_in.get_nowait(1)

                if not msg:
                    continue

                cmd = msg.get('cmd')

                if cmd == 'stop':
                    self._log.info('stop')
                    self._term.set()
                    sys.exit(0)

                elif cmd == 'cancel':
                    self._log.error('task cancellation not yet implemented')

                elif cmd == 'run':
                    self._launch(msg['task'])

                else:
                    raise ValueError('unknown command %s' % cmd)


        except Exception as e:
            self._log.exception('error in main loop: %s', e)
            raise

        finally:
            self._term.set()


    # --------------------------------------------------------------------------
    #
    def _launch(self, task):

        tid   = task['uid']
        ranks = task['description']['ranks']
        mode  = task['description']['mode']

        # we may need to wait for free slots.
        # FIXME: check GPUs, multithreadind, mem etc.
        while self._free < ranks:
            self._log.debug_9('wait for slots for %s [%d] - %d free', tid,
                              ranks, self._free)
            time.sleep(1)

        self._free -= ranks
        self._log.debug('found slots for %s [%d] - %d free', tid, ranks,
                        self._free)


        if mode == rp.TASK_EXECUTABLE:
            if ranks > 1:
                return self._launch_mpi(task)
            else:
                return self._launch_nonmpi(task)
        else:
            return self._launch_function(task)


    # --------------------------------------------------------------------------
    #
    def _launch_mpi(self, task):

        self._log.debug('task %s: launch', task['uid'])

        tid   = task['uid']
        sbox  = task['task_sandbox_path']
        ranks = task['description']['ranks']
        exe   = task['exec_path']
        out   = task['stdout_file']
        err   = task['stderr_file']

        cmd   = '/bin/sh'
        args  = ['-c', '%s >> %s 2>> %s' % (exe, out, err)]
        env   = os.environ.copy()

        self._log.debug('task %s: [%d ranks]: %s %s', tid, ranks, cmd, args)

        # TODO: use placement policies for GPU tasks
        group = ProcessGroup(restart=False, pmi_enabled=True, policy=None)

        for rank in range(ranks):

            env['DRAGON_RANK'] = str(rank)
            tmp = ProcessTemplate(target=cmd, args=args, env=env, cwd=sbox,
                                  stdout=Popen.PIPE, stderr=Popen.PIPE,
                                  stdin=Popen.DEVNULL)
            group.add_process(nproc=1, template=tmp)

        group.init()
        group.start()

        handler = GroupHandler(group)

        task['dragon_handler'] = (handler, ranks)

        self._log.debug('task %s: dragon handler %s', tid, handler)
        self._watcher_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _launch_nonmpi(self, task):

        self._log.debug('task %s: nonmpi launch', task['uid'])

        ranks = task['description']['ranks']
        assert ranks == 1, 'non-MPI tasks must have ranks==1'

        res = self._pool.apply_async(self._run_nonmpi,
                                     [task, self._logger_queue])

        handler = AsyncResultHandler(res)

        task['dragon_handler'] = (handler, ranks)
        self._watcher_queue.put(task)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _run_nonmpi(task, lq):

        val = None
        exc = None
        out = ''
        err = ''
        ret = 0

        tid = task['uid']
        exe = task['exec_path']

        lq.put(('debug', 'task %s: launch', tid))

        try:
            # TODO: use placement policies for GPU tasks
            out, err, ret = ru.sh_callout('/bin/sh %s' % exe, shell=True)
            lq.put(('debug', 'task %s: done: %s : %s : %s', tid, out, err, ret))

        except Exception as e:
            lq.put(('exception', 'task %s: call failed: %s', tid, e))
            exc = repr(e)
            ret = 1
            err += '\ncall failed: %s' % e

        return out, err, ret, val, exc


    # --------------------------------------------------------------------------
    #
    def _launch_function(self, task):

        self._log.debug('task %s: launch func', task['uid'])

        ranks = task['description']['ranks']

        assert ranks == 1, 'function tasks must have ranks==1'

        res = self._pool.apply_async(self._run_function, [task,
                                                          self._logger_queue])
        handler = AsyncResultHandler(res)

        task['dragon_handler'] = (handler, ranks)
        self._watcher_queue.put(task)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _run_function(task, lq):

        tid    = task['uid']
        func   = task['description']['function']
        args   = task['description']['args']
        kwargs = task['description']['kwargs']

        lq.put(('debug', 'task %s: run function', tid))

        # FIXME: populate sbox?
      # sbox   = task['task_sandbox_path']
      # out    = task['stdout_file']
      # err    = task['stderr_file']

        # check if `func_name` is a global name
        names   = dict(list(globals().items()) + list(locals().items()))
        to_call = names.get(func)

        # check if we have a serialized object
        if not to_call:
            lq.put(('debug', 'function serialized: %d: %s', len(func), func))

            try:
                to_call, _args, _kwargs = rp.PythonTask.get_func_attr(func)

            except Exception:
                lq.put('exception', 'function is not a PythonTask [%s] ', tid)

            else:
                if args or kwargs:
                    raise ValueError('`args` and `kwargs` must be empty for '
                                     'PythonTask function [%s]' % tid)
                else:
                    args   = _args
                    kwargs = _kwargs

        if not to_call:
            lq.put(('debug', 'no %s in \n%s\n', func, names))
            raise ValueError('%s callable %s not found: %s' % (tid, func, task))

        # make sure we capture stdout / stderr
        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        # set the task environment
        # FIXME

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            lq.put(('prof', 'rank_start', tid, time.time()))
            lq.put(('debug', 'to call %s: %s : %s', to_call, args, kwargs))

            val = to_call(*args, **kwargs)

            lq.put(('prof', 'rank_stop', tid, time.time()))
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = (None, None)
            ret = 0

        except Exception as e:
            lq.put(('exception', '_call failed: %s', tid, e))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            exc = repr(e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            # FIXME
            # os.environ = old_env

        lq.put(('debug', '%s: got "%s"', tid, out))

        return out, err, ret, val, exc


    # --------------------------------------------------------------------------
    #
    def _watcher_thread(self):

        try:

            to_collect = dict()

            self._log.info('start watcher')

            self._pipe_out = ru.zmq.Pipe(ru.zmq.MODE_PUSH)
            self._log.debug('pipe_out PUSH created: %s - %s',
                            ru.as_string(self._pipe_out.url), ru.get_hostip())
            self._watcher_event.set()

            time.sleep(0.5)

            self._pipe_out.put({'cmd': 'hello'})
            self._log.debug('pipe_out sent hello')

            # get completed tasks
            while True:

              # self._log.debug('watcher loop: %s', list(to_collect.keys()))

                # FIXME: get_nowait
                while not self._watcher_queue.empty():

                    task  = self._watcher_queue.get()
                    tid   = task['uid']
                    to_collect[tid] = task

                    self._log.debug('task %s: watch', tid)

                if not to_collect:
                    time.sleep(0.1)
                 #  self._log.debug('no tasks to collect')
                    continue

                collected = list()
                for tid,task in to_collect.items():

                    handler, _ = task['dragon_handler']
                  # self._log.debug('task %s: check handler %s', tid, handler)

                    if handler.is_alive():
                        pass

                    else:
                        del task['dragon_handler']

                        ranks = task['description']['ranks']
                        self._free += ranks

                        self._log.debug('task %s: collect [%d] - %d free',
                                        tid, ranks, self._free)

                        out, err, ret, val, exc = handler.get_results()

                        self._log.debug('task %s: completed %s : %s : %s : %s : %s',
                                        tid, out, err, ret, val, exc)

                        task['stdout']       = out
                        task['stderr']       = err
                        task['exit_code']    = ret
                        task['return_value'] = val
                        task['exception']    = str(exc)

                        if ret == 0: task['target_state'] = rp.DONE
                        else       : task['target_state'] = rp.FAILED

                        collected.append(tid)

                     #  self._log.debug('push %s: completed %s', tid, task)
                        self._pipe_out.put({'cmd': 'done',
                                            'task': task})
                     #  self._log.debug('pushed %s', tid)

                for tid in collected:
                    del to_collect[tid]
                  # self._log.debug('deleted %s', tid)

                # avoid busy loop
                if not collected:
                    time.sleep(0.5)

        except:
            self._log.exception('error in watcher thread')
            raise


    # --------------------------------------------------------------------------
    #
    def _logger_thread(self):

        while True:

            vals = self._logger_queue.get()

            mode = vals[0]
            if mode == 'prof':
                self._prof.prof(vals[1], uid=vals[2], ts=vals[3])

            elif mode == 'debug':
                self._log.debug(vals[1], *vals[2:])

            elif mode == 'exception':
                self._log.exception(vals[1], *vals[2:])

            else:
                self._log.error('unknown logger mode %s: %s', mode, vals)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    s = Server()


# ------------------------------------------------------------------------------


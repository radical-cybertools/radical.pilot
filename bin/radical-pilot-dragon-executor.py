#!/usr/bin/env python3

import os
import sys
import time
import queue


import multiprocessing as mp
import threading       as mt
import subprocess      as sp

import radical.utils   as ru
import radical.pilot   as rp

import dragon                      # pylint: disable=unused-import, import-error
mp.set_start_method("dragon")

from dragon.native.process       import Popen
from dragon.native.process       import Process
from dragon.native.process       import ProcessTemplate
from dragon.native.process_group import ProcessGroup

# ------------------------------------------------------------------------------
#
class Server(object):

    # FIXME: profile events

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._uid = ru.generate_id('radical.pilot.dragon')

        self._log  = ru.Logger(self._uid, path='.')
        self._term = mt.Event()

        self._pwatcher = ru.PWatcher(uid='%s.pw' % self._uid, log=self._log)
        self._pwatcher.watch(os.getpid())
        self._pwatcher.watch(os.getppid())

        self._pipe_in  = None
        self._pipe_out = None

        self._worker = mt.Thread(target=self._worker_thread)
        self._worker_event  = mt.Event()
        self._worker.daemon = True
        self._worker.start()

        self._watcher_queue = queue.Queue()

        self._watcher = mt.Thread(target=self._watcher_thread)
        self._watcher_event  = mt.Event()
        self._watcher.daemon = True
        self._watcher.start()

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

        # strangely enough, hostnames are not set in the out URL...
        url_in.host  = ru.get_hostname()
        url_out.host = ru.get_hostname()

        sys.stdout.write('ZMQ_ENDPOINTS %s %s\n' % (str(url_in), str(url_out)))
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

        try:
            while True:

                msg = self._pipe_in.get_nowait(1)

                if not msg:
                    continue

                if not isinstance(msg, dict):
                    self._log.error('invalid message type %s', type(msg))

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
        grp = ProcessGroup(restart=False, pmi_enabled=True, policy=None)

        for rank in range(ranks):

            env['DRAGON_RANK'] = str(rank)
            tmp = ProcessTemplate(target=cmd, args=args, env=env, cwd=sbox,
                                  stdout=Popen.PIPE, stderr=Popen.PIPE,
                                  stdin=Popen.DEVNULL)
            grp.add_process(nproc=1, template=tmp)

        grp.init()
        grp.start()

        task['dragon_group'] = (grp, ranks)
        self._watcher_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _watcher_thread(self):

        try:

            to_collect = dict()

            print('start watcher')

            self._pipe_out = ru.zmq.Pipe(ru.zmq.MODE_PUSH)
            self._watcher_event.set()

            # get completed tasks
            while True:

                while not self._watcher_queue.empty():

                    task = self._watcher_queue.get()
                    tid  = task['uid']
                    to_collect[tid] = task

                if not to_collect:
                    time.sleep(1)
                    continue

                print('watching %d tasks' % len(to_collect))

                collected = list()
                for tid,task in to_collect.items():

                    proc = task['proc']

                    if proc.is_alive():

                        print('task %s is alive' % tid)

                    else:

                        print('collecting %s 1' % tid)

                        # make sure proc is collected
                        proc.join()
                        task['exit_code'] = proc.exitcode
                        del task['proc']

                        print('collecting %s 2' % tid)

                        collected.append(tid)

                        self._log.debug('exit code %s: %s', tid, proc.exitcode)
                        print('task %s: exit code %s' % (tid, proc.exitcode))

                        if proc.exitcode != 0:
                            task['target_state'] = rp.FAILED
                        else:
                            task['target_state'] = rp.DONE

                        self._log.debug('collect task %s', tid)
                        print('task %s: collected' % tid)

                        self._pipe_out.put({'cmd': 'done',
                                            'task': task})

                        print('task %s: sent done message' % tid)

                for tid in collected:
                    del to_collect[tid]

                # avoid busy loop
                if not collected:
                    time.sleep(0.1)

                print('watcher loop')

        except Exception as e:
            self._log.exception('error in watcher thread')
            raise



# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    s = Server()


# ------------------------------------------------------------------------------


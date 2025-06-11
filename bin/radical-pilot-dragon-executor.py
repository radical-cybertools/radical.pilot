#!/usr/bin/env python3

# debug flag in case we want to test with plain Python3
USE_DRAGON = True

import sys
import time
import queue

if USE_DRAGON:
    import dragon                  # pylint: disable=unused-import, import-error

import multiprocessing as mp
import threading       as mt
import subprocess      as sp

import radical.utils   as ru
import radical.pilot   as rp

# FIXME: dragon documentation requires this - but when starting this script with
#        `dragon <script>`, then setting the mp start method to dragon causes
#        the tasks to fail
#
# if USE_DRAGON:
#     mp.set_start_method("dragon")


# ------------------------------------------------------------------------------
#
class Server(object):

    # FIXME: profile events

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._log  = ru.Logger('radical.pilot.dragon', path='.')
        self._term = mt.Event()

        self._pipe_in  = None
        self._pipe_out = None

        self._launcher = mt.Thread(target=self._launcher_thread)
        self._launcher_event  = mt.Event()
        self._launcher.daemon = True
        self._launcher.start()

        self._watcher_queue = queue.Queue()

        self._watcher = mt.Thread(target=self._watcher_thread)
        self._watcher_event  = mt.Event()
        self._watcher.daemon = True
        self._watcher.start()

        self._launcher_event.wait(timeout=5)
        self._watcher_event.wait(timeout=5)

        if not self._launcher_event.is_set():
            raise RuntimeError('launcher thread did not start')

        if not self._watcher_event.is_set():
            raise RuntimeError('watcher thread did not start')

        assert self._pipe_in,  'pipe_in not set'
        assert self._pipe_out, 'pipe_out not set'

        self._pipe_in.url.host = ru.get_hostname()
        self._pipe_out.url.host = ru.get_hostname()

        sys.stdout.write('ZMQ_ENDPOINTS %s %s\n'
                         % (ru.as_string(self._pipe_in.url),
                            ru.as_string(self._pipe_out.url)))
        sys.stdout.flush()

        # run forever
        while not self._term.is_set():
            time.sleep(1)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _popen(task):

        # NOTE: we can't use the logger here, as dragon wants to pickle
        #       arguments to the process which looses the log handler.

        tid  = task['uid']
        sbox = task['task_sandbox_path']

        print('task %s: fork' % tid)

        launch_path = task['launch_path']
        launch_out  = '%s/%s.launch.out' % (sbox, tid)
        launch_err  = '%s/%s.launch.err' % (sbox, tid)
        launch_cmd  = '%s > %s 2> %s' % (launch_path, launch_out, launch_err)
        print('task %s: launch command: %s' % (tid, launch_cmd))

      # out, err, ret = ru.sh_callout(launch_cmd, shell=True)
        p = sp.Popen(launch_cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        stdout, stderr = p.communicate()

        ret = p.returncode
        out = stdout.decode('utf-8')
        err = stderr.decode('utf-8')

        print('task %s: completed %s : %s : %s' % (tid, out, err, ret))


    # --------------------------------------------------------------------------
    #
    def _launcher_thread(self):
        '''
        handle incoming requests from the parent process

        The requests are expected to be dictionaries with the following keys:
            - cmd:  the command to execute (run, cancel, stop)
            - task: the task to execute the command on
        '''

        print('start launcher')

        self._pipe_in  = ru.zmq.Pipe(ru.zmq.MODE_PULL)
        self._launcher_event.set()

        try:
            while True:

                self._log.debug('waiting for request')
                msg = self._pipe_in.get_nowait(5)

                if not msg:
                    continue

                if not isinstance(msg, dict):
                    self._log.error('invalid message type %s', type(msg))

                cmd = msg.get('cmd')

                if cmd == 'stop':
                    self._log.info('stopping')
                    self._term.set()
                    sys.exit(0)

                elif cmd == 'cancel':
                    self._log.error('task cancellation not yet implemented')

                elif cmd == 'run':

                    task = msg['task']

                    self._log.debug('launch task %s', task['uid'])
                    task['proc'] = mp.Process(target=self._popen, args=[task])
                    task['proc'].start()
                    self._log.debug('task %s launched', task['uid'])

                    self._watcher_queue.put(task)

                else:
                    raise ValueError('unknown command %s' % cmd)


        except Exception as e:
            self._log.exception('error in main loop: %s', e)
            raise

        finally:
            self._term.set()


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


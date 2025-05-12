#!/usr/bin/env python3

# debug flag in case we want to test with plain Python3
USE_DRAGON = True

import os
import sys
import enum
import time
import queue
import signal

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

    class Command(enum.Enum):
        TO_WATCH  = 1
        TO_CANCEL = 2


    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._log = ru.Logger('radical.pilot.dragon', path='.')

        self._pin  = ru.zmq.Pipe(ru.zmq.MODE_PULL)
        self._pout = ru.zmq.Pipe(ru.zmq.MODE_PUSH)

        sys.stdout.write('ZMQ_ENDPOINTS %s %s\n'
                         % (ru.as_string(self._pin.url),
                            ru.as_string(self._pout.url)))
        sys.stdout.flush()

        self._watch_queue = queue.Queue()
        self._done_queue  = queue.Queue()

        self._watcher = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def serve(self):

        # FIXME: profile events
        self._log.info('serving')

        self._log.info('serving dragon')

        try:
            while True:

                self._log.debug('main loop')

                saw_request = self._handle_requests()
                saw_result  = self._handle_results()

                if not saw_request and not saw_result:
                    time.sleep(1)

        except Exception as e:
            self._log.exception('error in main loop: %s', e)
            raise


    # --------------------------------------------------------------------------
    #
    def _handle_requests(self):
        '''
        handle incoming requests from the parent process

        The requests are expected to be dictionaries with the following keys:
            - cmd:  the command to execute (run, cancel, stop)
            - task: the task to execute the command on
        '''

        self._log.debug('waiting for request')

        msg = self._pin.get_nowait(1)

        if not msg:
            return False

        if not isinstance(msg, dict):
            self._log.error('invalid message type %s', type(msg))

        cmd  = msg.get('cmd')

        if cmd == 'stop':
            self._log.info('stopping')
            sys.exit(0)

        elif cmd == 'cancel':

            task = msg['task']

            self._log.debug('cancel task %s', task['uid'])
            self._watch_queue.put((self.Command.TO_CANCEL, task))

        elif cmd == 'run':

            task = msg['task']

            self._log.debug('launch task %s', task['uid'])
            task['proc'] = mp.Process(target=self._fork_task, args=[task])
            task['proc'].start()
            self._log.debug('task %s launched', task['uid'])

            # FIXME: this should be done in the watcher
            # self.handle_timeout(task)

            # watch task for completion
            self._watch_queue.put((self.Command.TO_WATCH, task))

            self._log.debug('task %s watched', task['uid'])

        else:
            self._log.error('unsupported command %s', cmd)

        return True


    # --------------------------------------------------------------------------
    #
    def _handle_results(self):

        if self._done_queue.empty():
            return False

        task = self._done_queue.get()
        self._log.debug('collect task %s', task['task']['uid'])
        self._pout.put(task)

        return True


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _fork_task(task):

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
    def _watch(self):

        to_watch  = list()
        to_cancel = list()

        # get completed tasks
        while True:

            if not self._watch_queue.empty():

                cmd, task = self._watch_queue.get()

                if cmd == self.Command.TO_WATCH:
                    to_watch.append(task)

                elif cmd == self.Command.TO_CANCEL:
                    to_cancel.append(task)

                else:
                    raise ValueError('unsupported cmd %s' % cmd)

            self._check_running(to_watch, to_cancel)


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self, to_watch, to_cancel):

        # `to_watch.remove()` in the loop requires copy to iterate over the list
        for task in list(to_watch):

            tid  = task['uid']
            proc = task['proc']

            tasks_to_advance = list()
            tasks_to_cancel  = list()

            if proc.is_alive():

                # process is still running - cancel if needed
                if tid in to_cancel:

                    # got a request to cancel this task - send SIGTERM to the
                    # process group (which should include the actual launch
                    # method)
                    try:
                        # kill the whole process group
                        proc.kill()

                    except OSError:
                        # lost race: task is already gone, we ignore this
                        # FIXME: collect and move to DONE/FAILED
                        pass

                    proc.join()

                    to_cancel.remove(tid)
                    to_watch.remove(task)
                    del task['proc']  # proc is not json serializable

                    tasks_to_cancel.append(task)

            else:

                # make sure proc is collected
                proc.join()
                task['exit_code'] = proc.exitcode

                # Free the Slots, Flee the Flots, Ree the Frots!
                to_watch.remove(task)
                if tid in to_cancel:
                    to_cancel.remove(tid)

                del task['proc']  # proc is not json serializable
                tasks_to_advance.append(task)

                self._log.debug('exit code %s: %s', task['uid'], proc.exitcode)

                if proc.exitcode != 0:
                    # task failed - fail after staging output
                    task['exception']        = 'RuntimeError("task failed")'
                    task['exception_detail'] = 'exit code: %s' % proc.exitcode
                    task['target_state'    ] = rp.FAILED

                else:
                    # The task finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there
                    # are no directives -- at the very least, we'll upload
                    # stdout/stderr
                    task['target_state'] = rp.DONE

                self._done_queue.put({'cmd' : 'done',
                                      'task': task})


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    s = Server()
    s.serve()


# ------------------------------------------------------------------------------


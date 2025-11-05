
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'


import os
import time

from typing import List

from rc.process import Process

import threading     as mt

import radical.utils as ru

from ...  import states    as rps
from ...  import constants as rpc

from .popen import Popen


# ------------------------------------------------------------------------------
#
class Dragon(Popen):

    # --------------------------------------------------------------------------
    #
    def initialize(self):
        '''
        This method is called by the base class during component
        initialization.

        It will start the dragon executor. The executor will communicate the endpoint
        URLs back to us. We will then use those endpoints to communicate with the dragon
        process.
        '''

        super().initialize()

        self._url_in    = None
        self._url_out   = None
        self._start_evt = mt.Event()

        # ----------------------------------------------------------------------
        #
        def line_cb(proc: Process, lines : List[str]) -> None:
            for line in lines:
                self._log.info('line: %s', line)
                if line.startswith('ZMQ_ENDPOINTS '):
                    _, self._url_out, self._url_in = line.split()
                    self._start_evt.set()
                    break

        def state_cb(proc: Process, state: str):
            self._log.debug('process state: %s' % state)
            if state == 'failed':
                self._log.error('dragon executor failed')
                self._log.error('stdout: %s', proc.stdout)
                self._log.error('stderr: %s', proc.stderr)
                self._start_evt.set()
        # ----------------------------------------------------------------------

        n_nodes = len(self._rm.info.node_list)
        n_slots = self._rm.info.cores_per_node * n_nodes

        cmd  = ''
        if os.path.exists('/opt/cray/libfabric/1.22.0/lib64'):
            cmd += 'dragon-config '
            cmd += '-a "ofi-runtime-lib=/opt/cray/libfabric/1.22.0/lib64"; '

        cmd += 'dragon -N %d ' % n_nodes
        cmd += '-l ERROR '
        cmd += 'radical-pilot-dragon-executor.py %d' % n_slots

        self._log.debug('=== cmd: %s', cmd)

        p = Process(cmd, shell=True)
        p.register_cb(p.CB_OUT_LINE, line_cb)
        p.register_cb(p.CB_STATE, state_cb)
        p.polldelay = 0.1
        p.start()

        self._pwatcher = ru.PWatcher(uid='%s.pw' % self._uid, log=self._log)
        self._pwatcher.watch(os.getpid())
        self._pwatcher.watch(p.pid)

        self._start_evt.wait()

        self._log.debug('dragon eps: %s - %s', self._url_in, self._url_out)

        assert self._url_in
        assert self._url_out

        self._pipe_out = ru.zmq.Pipe(ru.zmq.MODE_PUSH, self._url_out)

        # run watcher thread
        self._watcher = mt.Thread(target=self._dragon_watch, args=[self._url_in])
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task):

        # before we start handling the task, check if it should run in a named
        # env.  If so, inject the activation of that env in the task's pre_exec
        # directives.
        tid  = task['uid']
        td   = task['description']
        sbox = task['task_sandbox_path']

        # prepare stdout/stderr
        task['stdout'] = ''
        task['stderr'] = ''

        stdout_file    = td.get('stdout') or '%s.out' % tid
        stderr_file    = td.get('stderr') or '%s.err' % tid

        if stdout_file[0] != '/':
            task['stdout_file']       = '%s/%s' % (sbox, stdout_file)
            task['stdout_file_short'] = '$RP_TASK_SANDBOX/%s' % stdout_file
        else:
            task['stdout_file']       = stdout_file
            task['stdout_file_short'] = stdout_file

        if stderr_file[0] != '/':
            task['stderr_file']       = '%s/%s' % (sbox, stderr_file)
            task['stderr_file_short'] = '$RP_TASK_SANDBOX/%s' % stderr_file
        else:
            task['stderr_file']       = stderr_file
            task['stderr_file_short'] = stderr_file

        launcher, lname = self._rm.find_launcher(task)

        if not launcher:
            raise RuntimeError('no launcher found for %s' % task)

        task['launcher_name'] = lname

        _, exec_fullpath = self._create_exec_script(launcher, task)
        task['exec_path']   = exec_fullpath

        # send task to dragon
        self._log.debug('launch task %s', task['uid'])

        if False:
            task['target_state'] = rps.DONE
            task['exit_code']    = 0

            self._prof.prof('unschedule_start', uid=tid)

            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, [task])

            self.advance([task], rps.AGENT_STAGING_OUTPUT_PENDING,
                                 publish=True, push=True)

        else:
            self._pipe_out.put({'cmd': 'run', 'task': task})
            self._log.debug('sent task %s', task['uid'])

        # handle task timeout if needed
        self.handle_timeout(task)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):
        '''
        This method is called by the base class to actually
        cancel the task.
        '''

        # send cancel request to dragon
        self._pipe_out.put({'cmd': 'cancel', 'uid': task['uid']})


    # --------------------------------------------------------------------------
    #
    def _dragon_watch(self, url_in):
        '''
        This method watches the dragon endpoint for task completion messages.

        :param url_in:  the dragon endpoint to watch
        '''

        self._log.debug('dragon watch: %s', url_in)

        pipe_in  = ru.zmq.Pipe(ru.zmq.MODE_PULL, url_in)
        self._log.debug('pipe_in  PUll created: %s - %s',
                            ru.as_string(pipe_in.url), ru.get_hostname())
        time.sleep(0.5)

        try:
            while not self._term.is_set():

              # self._log.debug('pipe_in  receive')
                msg = pipe_in.get_nowait(5.1)

                if not msg:
                  # self._log.info('pipe_in  no msg')
                    continue

                cmd = msg.get('cmd')

                self._log.debug('pipe_in  receive [%s]', cmd)

                if cmd == 'hello':
                    self._log.info('pipe_in  hello pipe')
                    continue

                if cmd != 'done':
                    raise ValueError('unsupported command %s' % cmd)

                task = msg['task']
                tid  = task['uid']

                self._prof.prof('unschedule_start', uid=tid)
                self._log.debug('dragon watch: %s : %s', cmd, tid)

                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, [task])

                self.advance([task], rps.AGENT_STAGING_OUTPUT_PENDING,
                                     publish=True, push=True)

        except Exception as e:
            self._log.exception('Error in ExecWorker watch loop (%s)' % e)
            self.stop()


# ------------------------------------------------------------------------------


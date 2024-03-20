
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import time

import threading     as mt
import subprocess    as sp

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

        self._pid = self.session.cfg.pid

        # run dragon execution helper in a separate process which actually uses
        # dragon's multiprocessing implementation
        self._dragon = sp.Popen(
                args   = ['dragon', 'radical-pilot-dragon-executor.py',
                          self.session._cfg.path],
                stdout = sp.PIPE,
                stderr = sp.STDOUT)

        # run this loop for 10 seconds to get the endpoints
        start = time.time()
        while True:

            if self._dragon.poll() is not None:
                self._log.error('%s', str(self._dragon.communicate()))
                raise RuntimeError('dragon process died')

            line = ru.as_string(self._dragon.stdout.readline().strip())
            self._log.debug('line: [%s]', line)

            if line.startswith('ZMQ_ENDPOINTS '):
                _, url_out, url_in = line.split()
                break

            if time.time() - start > 3:
                self._log.debug('%s', str(self._dragon.communicate()))
                raise RuntimeError('failed to get dragon endpoint')

        self._log.debug('dragon eps: %s - %s', url_in, url_out)

        self._pipe_out = ru.zmq.Pipe(ru.zmq.MODE_PUSH, url_out)

        # run watcher thread
        self._watcher = mt.Thread(target=self._dragon_watch, args=[url_in])
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def _launch_task(self, task):
        '''
        This method is called by the base class to actually
        launch the task.
        '''

        # send task to dragon
        self._log.debug('launch task %s', task['uid'])
        self._pipe_out.put({'cmd': 'run', 'task': task})

        # handle task timeout if needed
        self.handle_timeout(task)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, uid):
        '''
        This method is called by the base class to actually
        cancel the task.
        '''

        # send cancel request to dragon
        self._pipe_out.put({'cmd': 'cancel', 'uid': uid})


    # --------------------------------------------------------------------------
    #
    def _dragon_watch(self, url_in):
        '''
        This method watches the dragon endpoint for task completion messages.

        :param url_in:  the dragon endpoint to watch
        '''

        pipe_in  = ru.zmq.Pipe(ru.zmq.MODE_PULL, url_in)

        try:
            while not self._term.is_set():

                msg = pipe_in.get_nowait(0.1)

                if not msg:
                    continue

                cmd = msg.get('cmd')

                if cmd != 'done':
                    raise ValueError('unsupported command %s' % cmd)

                task = msg['task']
                tid  = task['uid']

                self._prof.prof('unschedule_start', uid=tid)

                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, [task])

                self.advance([task], rps.AGENT_STAGING_OUTPUT_PENDING,
                                     publish=True, push=True)

        except Exception as e:
            self._log.exception('Error in ExecWorker watch loop (%s)' % e)
            self.stop()


# ------------------------------------------------------------------------------


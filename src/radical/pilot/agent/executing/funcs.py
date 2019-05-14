
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import stat
import time
import Queue
import signal
import tempfile
import threading
import traceback
import subprocess

import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ==============================================================================
#
class FUNCS(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)

        self._watcher   = None
        self._terminate = threading.Event()


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        self.register_publisher ("funcexec_pubsub")
        self.register_subscriber("funcexec_queue", self.collect_cb)

        self._cancel_lock    = threading.RLock()
        self._cus_to_cancel  = list()
        self._cus_to_watch   = list()
        self._watch_queue    = Queue.Queue ()

        self._pilot_id = self._cfg['pilot_id']

        # run watcher thread
        self._watcher = ru.Thread(target=self._watch, name="Watcher")
        self._watcher.start()

        # we need to launch the executors on all nodes, and use the
        # task_launcher for that
        self._launcher = rp.agent.LM.create(
                name    = self._cfg.get('task_launch_method'),
                cfg     = self._cfg,
                session = self._session)

        # now run the func launcher on all nodes
        self._executors = list()
        self._nodes     = self._cfg['lrms_info']['node_list']
        for idx, node in enumerate(self._nodes):
            uid   = 'func_exec.%04d' % idx
            unit  = {'uid'        : uid,
                     'description': {'executable'   : None,  # FIXME
                                     'cpu_processes': 1,
                                    }
                     'slots'      : [{'name' : node['name'], 
                                      'uid'  : node['uid'], 
                                      'cores': [[0]], 
                                      'gpus' : []}]
                    }
            self._spawn(self._launcher, unit)


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_units':

            self._log.info("cancel_units command (%s)" % arg)
            with self._cancel_lock:
                self._cus_to_cancel.extend(arg['uids'])

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)


        for unit in units:
            # TODO: publish tasks toward "funcexec_pubsub"
            pass


    # --------------------------------------------------------------------------
    #
    def _spawn(self, launcher, cu):

        # NOTE: see documentation of cu['sandbox'] semantics in the ComputeUnit
        #       class definition.
        sandbox = '%s/%s' % (self._pwd, cu['uid'])

        with open(launch_script_name, "w") as launch_script:
            launch_script.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            env_string = ''
            env_string += 'export RP_SESSION_ID="%s"\n'   % self._cfg['session_id']
            env_string += 'export RP_PILOT_ID="%s"\n'     % self._cfg['pilot_id']
            env_string += 'export RP_AGENT_ID="%s"\n'     % self._cfg['agent_name']
            env_string += 'export RP_SPAWNER_ID="%s"\n'   % self.uid
            env_string += 'export RP_UNIT_ID="%s"\n'      % cu['uid']
            env_string += 'export RP_GTOD="%s"\n'         % self.gtod
            env_string += 'export RP_TMP="%s"\n'          % self._cu_tmp
            env_string += 'export RP_PILOT_STAGING="%s/staging_area"\n' \
                                                          % self._pwd
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                env_string += 'export RP_PROF="%s/%s.prof"\n' % (sandbox, cu['uid'])
            else:
                env_string += 'unset  RP_PROF\n'

            launch_command, hop_cmd = launcher.construct_command(cu, launch_script_name)

            if hop_cmd : cmdline = hop_cmd
            else       : cmdline = launch_script_name

            # also add any env vars requested in the unit description
            if descr['environment']:
                for key,val in descr['environment'].iteritems():
                    env_string += 'export "%s=%s"\n' % (key, val)

            launch_script.write('prof executor_start\n')
            launch_script.write('%s\n' % launch_command)
            launch_script.write('RETVAL=$?\n')
            launch_script.write('prof executor_stop\n')
            launch_script.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)

        # prepare stdout/stderr
        stdout_file = descr.get('stdout') or 'STDOUT'
        stderr_file = descr.get('stderr') or 'STDERR'

        cu['stdout_file'] = os.path.join(sandbox, stdout_file)
        cu['stderr_file'] = os.path.join(sandbox, stderr_file)

        _stdout_file_h = open(cu['stdout_file'], "w")
        _stderr_file_h = open(cu['stderr_file'], "w")

        self._log.info("Launching unit %s via %s in %s", cu['uid'], cmdline, sandbox)

        self._prof.prof('exec_start', uid=cu['uid'])
        cu['proc'] = subprocess.Popen(args       = cmdline,
                                      executable = None,
                                      stdin      = None,
                                      stdout     = _stdout_file_h,
                                      stderr     = _stderr_file_h,
                                      preexec_fn = os.setsid,
                                      close_fds  = True,
                                      shell      = True,
                                      cwd        = sandbox)
        self._prof.prof('exec_ok', uid=cu['uid'])


    # --------------------------------------------------------------------------
    #
    def _collect(self):

        while not self._terminate.is_set():

            # pull units from "funcexec_out_queue"
            units = list()

            if units:
                self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING,
                             publish=True, push=True)

            else:
                time.sleep(0.1)


# ------------------------------------------------------------------------------


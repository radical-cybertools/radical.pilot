
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import stat
import time
import queue
import threading as mt
import subprocess

import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class FUNCS(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)

        self._collector = None
        self._terminate = mt.Event()


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()
        self.gtod = "%s/gtod" % self._pwd

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        addr_wrk = self._cfg['bridges']['funcs_wrk_queue']
        addr_res = self._cfg['bridges']['funcs_res_queue']

        self._log.debug('wrk in  addr: %s', addr_wrk['addr_in' ])
        self._log.debug('res out addr: %s', addr_res['addr_out'])

        self._funcs_wrk = rpu.Queue(self._session, 'funcs_wrk_queue',
                                    rpu.QUEUE_INPUT, self._cfg,
                                    addr_wrk['addr_in'])
        self._funcs_res = rpu.Queue(self._session, 'funcs_res_queue',
                                    rpu.QUEUE_OUTPUT, self._cfg,
                                    addr_res['addr_out'])

        self._cancel_lock    = ru.RLock()
        self._cus_to_cancel  = list()
        self._cus_to_watch   = list()
        self._watch_queue    = queue.Queue ()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._collector = mt.Thread(target=self._collect)
        self._collector.daemon = True
        self._collector.start()

        # we need to launch the executors on all nodes, and use the
        # agent_launcher for that
        self._launcher = rp.agent.LM.create(
                name    = self._cfg.get('agent_launch_method'),
                cfg     = self._cfg,
                session = self._session)

        # now run the func launcher on all nodes
        ve  = os.environ.get('VIRTUAL_ENV',  '')
        exe = ru.which('radical-pilot-agent-funcs')

        if not exe:
            exe = '%s/rp_install/bin/radical-pilot-agent-funcs' % self._pwd

        for idx, node in enumerate(self._cfg['lrms_info']['node_list']):
            uid   = 'func_exec.%04d' % idx
            pwd   = '%s/%s' % (self._pwd, uid)
            funcs = {'uid'        : uid,
                     'description': {'executable'   : exe,
                                     'arguments'    : [pwd, ve],
                                     'cpu_processes': 1,
                                     'environment'  : [],
                                    },
                     'slots'      : {'nodes'        : [{'name'  : node[0],
                                                        'uid'   : node[1],
                                                        'cores' : [[0]],
                                                        'gpus'  : []
                                                       }]
                                    },
                     'cfg'        : {'addr_wrk'     : addr_wrk['addr_out'],
                                     'addr_res'     : addr_res['addr_in']
                                    }
                    }
            self._spawn(self._launcher, funcs)


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
    def _spawn(self, launcher, funcs):

        # NOTE: see documentation of funcs['sandbox'] semantics in the ComputeUnit
        #       class definition.
        sandbox = '%s/%s'     % (self._pwd, funcs['uid'])
        fname   = '%s/%s.sh'  % (sandbox,   funcs['uid'])
        cfgname = '%s/%s.cfg' % (sandbox,   funcs['uid'])
        descr   = funcs['description']

        rpu.rec_makedir(sandbox)
        ru.write_json(funcs.get('cfg'), cfgname)

        launch_cmd, hop_cmd = launcher.construct_command(funcs, fname)

        if hop_cmd : cmdline = hop_cmd
        else       : cmdline = fname

        with open(fname, "w") as fout:

            fout.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            fout.write('export RP_SESSION_ID="%s"\n' % self._cfg['sid'])
            fout.write('export RP_PILOT_ID="%s"\n'   % self._cfg['pid'])
            fout.write('export RP_AGENT_ID="%s"\n'   % self._cfg['aid'])
            fout.write('export RP_SPAWNER_ID="%s"\n' % self.uid)
            fout.write('export RP_FUNCS_ID="%s"\n'   % funcs['uid'])
            fout.write('export RP_GTOD="%s"\n'       % self.gtod)
            fout.write('export RP_TMP="%s"\n'        % self._cu_tmp)

            # also add any env vars requested in the unit description
            if descr.get('environment', []):
                for key,val in descr['environment'].items():
                    fout.write('export "%s=%s"\n' % (key, val))

            fout.write('\n%s\n\n' % launch_cmd)
            fout.write('RETVAL=$?\n')
            fout.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(fname)
        os.chmod(fname, st.st_mode | stat.S_IEXEC)

        fout = open('%s/%s.out' % (sandbox, funcs['uid']), "w")
        ferr = open('%s/%s.err' % (sandbox, funcs['uid']), "w")

        self._prof.prof('exec_start', uid=funcs['uid'])
        funcs['proc'] = subprocess.Popen(args       = cmdline,
                                         executable = None,
                                         stdin      = None,
                                         stdout     = fout,
                                         stderr     = ferr,
                                         preexec_fn = os.setsid,
                                         close_fds  = True,
                                         shell      = True,
                                         cwd        = sandbox)

        self._prof.prof('exec_ok', uid=funcs['uid'])


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)

        for unit in units:
            assert(unit['description']['cpu_process_type'] == 'FUNC')
            self._funcs_wrk.put(unit)


    # --------------------------------------------------------------------------
    #
    def _collect(self):

        while not self._terminate.is_set():

            # pull units from "funcs_out_queue"
            units = self._funcs_res.get_nowait(1000)

            if units:

                for unit in units:
                    unit['target_state'] = unit['state']
                    unit['pilot']        = self._pid

                  # self._log.debug('got %s [%s] [%s] [%s]',
                  #                 unit['uid'],    unit['state'],
                  #                 unit['stdout'], unit['stderr'])

                self.advance(units, rps.AGENT_STAGING_OUTPUT_PENDING,
                             publish=True, push=True)
            else:
                time.sleep(0.1)


# ------------------------------------------------------------------------------


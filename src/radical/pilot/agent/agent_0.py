
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import copy
import stat
import time
import pprint
import subprocess        as sp
import radical.utils     as ru

from .. import states    as rps
from .. import constants as rpc
from .. import utils     as rpu
from .. import Session   as rp_Session

from .  import rm        as rpa_rm
from .  import lm        as rpa_lm


# this needs git attribute 'ident' set for this file
git_ident = "$Id$"


class Agent_0(rpu.Worker):

    # This is the base agent.  It does not do much apart from starting
    # sub-agents and watching them  If any of the sub-agents die, it will shut
    # down the other sub-agents and itself.  
    #
    # This class inherits the rpu.Worker, so that it can use the communication
    # bridges and callback mechniams.  It will own a session (which creates said
    # communication bridges (or at least some of them); and a controller, which
    # will control the sub-agents.
    
    # --------------------------------------------------------------------------
    #
    def __init__(self, agent_name, agent_part):

        # load config, create session and controller, init rpu.Worker

        # load the agent config, and overload the config dicts
        cfg               = ru.read_json_str("%s/agent.cfg" % (os.getcwd()))
        self._uid         = agent_name
        self._part        = agent_part
        self._starttime   = time.time()
        self._final_cause = None
        self._lrms        = None

        # sanity check on config settings
        if not 'cores'               in cfg: raise ValueError("Missing number of cores")
        if not 'debug'               in cfg: raise ValueError("Missing DEBUG level")
        if not 'lrms'                in cfg: raise ValueError("Missing LRMS")
        if not 'dburl'               in cfg: raise ValueError("Missing DBURL")
        if not 'pilot_id'            in cfg: raise ValueError("Missing pilot id")
        if not 'runtime'             in cfg: raise ValueError("Missing or zero agent runtime")
        if not 'scheduler'           in cfg: raise ValueError("Missing agent scheduler")
        if not 'session_id'          in cfg: raise ValueError("Missing session id")
        if not 'spawner'             in cfg: raise ValueError("Missing agent spawner")
        if not 'task_launch_method'  in cfg: raise ValueError("Missing unit launch method")
        if not 'agent_layout'        in cfg: raise ValueError("Missing agent layout")

        self._pilot_id    = cfg['pilot_id']
        self._session_id  = cfg['session_id']
        self._runtime     = cfg['runtime']

        # Check for the RADICAL_PILOT_DB_HOSTPORT env var, which will hold
        # the address of the tunnelized DB endpoint. If it exists, we
        # overrule the agent config with it.
        hostport = os.environ.get('RADICAL_PILOT_DB_HOSTPORT')
        if hostport:
            dburl = ru.Url(cfg['dburl'])
            dburl.host, dburl.port = hostport.split(':')
            cfg['dburl'] = str(dburl)
        
        # Create a session which connects to MongoDB.
        # This session will also create any communication channels and
        # components/workers specified in the config.
        self._session = rp_Session(cfg=cfg, uid=self._session_id, _connect=True)
        ru.dict_merge(cfg, self._session.ctrl_cfg, ru.PRESERVE)
        pprint.pprint(cfg)

        # set up a logger and profiler
        self._prof = self._session._get_profiler('bootstrap_3')
        self._prof.prof('sync ref', msg='%s start' % self._uid, uid=self._pilot_id)

        self._log  = self._session._get_logger('bootstrap_3', cfg.get('debug'))
        self._log.info('start')

        if not self._session.is_connected:
            raise RuntimeError('agent could not connect to mongodb')

        # at this point the session is up, and the session controller should
        # have brought up all communication bridges and the UpdateWorker.  
        rpu.Worker.__init__(self, cfg, self._session)


    # --------------------------------------------------------------------------
    #
    def initialize_parent(self):

        # Create LRMS which will give us the set of agent_nodes to use for
        # sub-agent startup.  Add the remaining LRMS information to the
        # config, for the benefit of the scheduler).
        self._lrms = rpa_rm.RM.create(name=self._cfg['lrms'], cfg=self._cfg, 
                                      session=self._session)

        # add the resource manager information to our own config
        self._cfg['lrms_info'] = self._lrms.lrms_info

        # create the sub-agent configs
        self._write_sa_configs()

        # and start the sub agents
        self._start_sub_agents()

        # register the command callback which pulls the DB for commands
        self.register_timed_cb(self._agent_command_cb, 
                               timer=self._cfg['heartbeat_interval'])

        # registers the staging_input_queue as this is what we want to push
        # units to
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        # sub-agents are started, components are started, bridges are up: we are
        # ready to roll!
        pilot = {'type'    : 'pilot',
                 'uid'     : self._pilot_id,
                 'state'   : rps.PMGR_ACTIVE,
                 'lm_info' : self._lrms.lm_info.get('version_info'),
                 '$set'    : 'lm_info'}
        self.advance(pilot, publish=True, push=False, prof=True)

        # register idle callback, to pull for units -- which is the only action
        # we have to perform, really
        self.register_timed_cb(self._check_units, 
                               timer=self._cfg['db_poll_sleeptime'])


    # --------------------------------------------------------------------------
    #
    def finalize_parent(self):

        # tear things down in reverse order

        if self._lrms:
            self._lrms.stop()

        if   self._final_cause == 'timeout'  : state = rps.DONE 
        elif self._final_cause == 'cancel'   : state = rps.CANCELED
        elif self._final_cause == 'sys.exit' : state = rps.CANCELED
        else                                 : state = rps.FAILED

        # we don't rely on the existence / viability of the update worker at
        # that point.
        self._update_db(state, self._final_cause)

        if self._session:
            self._session.close()

        self._prof.close()


    # --------------------------------------------------------------------------
    #
    def _update_db(self, state, msg=None):
    
        self._log.info('pilot state: %s', state)
        self._log.info('rusage: %s', rpu.get_rusage())
        self._log.info(msg)

        now = rpu.timestamp()
        out = None
        err = None
        log = None
    
        try    : out = open('./agent.out', 'r').read()
        except : pass
        try    : err = open('./agent.err', 'r').read()
        except : pass
        try    : log = open('./agent.log', 'r').read()
        except : pass
    
        self._session.get_db()._c.update(
                {'type'   : 'pilot', 
                 "uid"    : self._pilot_id},
                {"$push"  : {"states"        : state},
                 "$set"   : {"state"         : state, 
                             "stdout"        : rpu.tail(out),
                             "stderr"        : rpu.tail(err),
                             "logfile"       : rpu.tail(log),
                             "finished"      : now}
                })
    

    # --------------------------------------------------------------------------
    #
    def _write_sa_configs(self):

        # use our own config sans components as a basis for the sub-agent
        # configs.
        sa_cfg = copy.deepcopy(self._cfg)
        sa_cfg['components'] = list()
 
        # we have all information needed by the subagents -- write the
        # sub-agent config files.

        # write deep-copies of the config (with the corrected agent_name) for
        # each sub-agent (apart from agent_0)
        sa_cfg_0 = None
        for sa in self._cfg.get('agent_layout', []):

            sa_name = sa.replace('_', '.')

            tmp_cfg = copy.deepcopy(sa_cfg)

            # merge sub_agent layout into the confoig
            ru.dict_merge(tmp_cfg, self._cfg['agent_layout'][sa], ru.OVERWRITE)

            tmp_cfg['agent_name'] = sa_name
            tmp_cfg['owner']      = self._pilot_id
            # FIXME: should 'owner' be the sub-agent, possibly?

            ru.write_json(tmp_cfg, './%s.cfg' % sa_name)


    # --------------------------------------------------------------------------
    #
    def _start_sub_agents(self):
        """
        For the list of sub_agents, get a launch command and launch that
        agent instance on the respective node.  We pass it to the seconds
        bootstrap level, there is no need to pass the first one again.
        """
    
        # FIXME: we need a watcher cb to watch sub-agent state
    
        self._log.debug('start_sub_agents')
    
        if not self._cfg['agent_layout']:
            self._log.debug('start_sub_agents noop')
            return

        # the configs are written, and the sub-agents can be started.  To know
        # how to do that we create the agent launch method, have it creating
        # the respective command lines per agent instance, and run via
        # popen.
        #
        # actually, we only create the agent_lm once we really need it for
        # non-local sub_agents.
        agent_lm   = None
        sub_agents = list()
        for sa in self._cfg['agent_layout']:

            sa_name = sa.replace('_', '.')

            target    = self._cfg['agent_layout'][sa]['target']
            partition = self._cfg['agent_layout'][sa]['partition']

            if partition != self._part:
                # this sub-agent belongs to a different partition
                continue

            if target == 'local':
                # start agent locally
                cmdline = "/bin/sh -l %s/bootstrap_2.sh %s %d" \
                        % (os.getcwd(), sa_name, partition)

            elif target == 'node':
                if not agent_lm:
                    agent_lm = rpa_lm.LM.create(
                        name    = self._cfg['agent_launch_method'],
                        cfg     = self._cfg,
                        session = self._session)
    
                node = self._cfg['lrms_info']['agent_nodes'][sa_name]
                # start agent remotely, use launch method
                # NOTE:  there is some implicit assumption that we can use
                #        the 'agent_node' string as 'agent_string:0' and
                #        obtain a well format slot...
                # FIXME: it is actually tricky to translate the agent_node
                #        into a viable 'opaque_slots' structure, as that is
                #        usually done by the schedulers.  So we leave that
                #        out for the moment, which will make this unable to
                #        work with a number of launch methods.  Can the
                #        offset computation be moved to the LRMS?
                ls_name = "%s/%s.sh" % (os.getcwd(), sa_name)
                opaque_slots = {
                        'task_slots'   : ['%s:0' % node],
                        'task_offsets' : [],
                        'lm_info'      : self._cfg['lrms_info']['lm_info']}
                agent_cmd = {
                        'opaque_slots' : opaque_slots,
                        'description'  : {
                            'cores'      : 1,
                            'executable' : "/bin/sh",
                            'arguments'  : ["%s/bootstrap_2.sh" % os.getcwd(),
                                            sa_name, partition]
                            }
                        }
                cmd, hop = agent_lm.construct_command(agent_cmd,
                        launch_script_hop='/usr/bin/env RP_SPAWNER_HOP=TRUE "%s"' % ls_name)
    
                with open (ls_name, 'w') as ls:
                    # note that 'exec' only makes sense if we don't add any
                    # commands (such as post-processing) after it.
                    ls.write('#!/bin/sh\n\n')
                    ls.write("exec %s\n" % cmd)
                    st = os.stat(ls_name)
                    os.chmod(ls_name, st.st_mode | stat.S_IEXEC)
    
                if hop : cmdline = hop
                else   : cmdline = ls_name
    
            # spawn the sub-agent
            self._log.info ("create sub-agent %s: %s" % (sa_name, cmdline))
            sa_out = open("%s.out" % sa_name, "w")
            sa_err = open("%s.err" % sa_name, "w")
            sa_proc = sp.Popen(args=cmdline.split(), stdout=sa_out, stderr=sa_err)
    
            # make sure we can stop the sa_proc
            sa_proc.name = sa_name
            sa_proc.stop = sa_proc.terminate
            sub_agents.append(sa_proc)
    
        # the agents are up - let the session controller manage them from here
        if sub_agents:
            self._session._controller.add_watchables(sub_agents, owner=self._pilot_id)
    
        self._log.debug('%d sub_agents started', len(sub_agents))


    # --------------------------------------------------------------------------
    #
    def _agent_command_cb(self):

        self._prof.prof('heartbeat', msg='Listen! Listen! Listen to the heartbeat!',
                        uid=self._owner)
        self._check_commands()
        self._check_state   ()
        return True


    # --------------------------------------------------------------------------
    #
    def _check_commands(self):

        # Check if there's a command waiting
        # FIXME: this pull should be done by the update worker, and commands
        #        should then be communicated over the command pubsub
        # FIXME: commands go to pmgr, umgr, session docs
        # FIXME: this is disabled right now
        return
        retdoc = self._session._dbs._c.find_and_modify(
                    query  = {"uid"  : self._owner},
                    update = {"$set" : {rpc.COMMAND_FIELD: []}}, # Wipe content of array
                    fields = [rpc.COMMAND_FIELD]
                    )

        if not retdoc:
            return

        self._log.debug('hb %s got %s', self._owner, retdoc['_id'])

        for command in retdoc.get(rpc.COMMAND_FIELD, []):

            cmd = command[rpc.COMMAND_TYPE]
            arg = command[rpc.COMMAND_ARG]

            self._prof.prof('ingest_cmd', msg="mongodb to HeartbeatMonitor (%s : %s)" \
                            % (cmd, arg), uid=self._owner)

            if cmd == rpc.COMMAND_CANCEL_PILOT:
                self._log.info('cancel pilot cmd')
                self.final_cause = 'cancel'
                self._cb_lock.release()
                self.stop()

            elif cmd == rpc.COMMAND_CANCEL_COMPUTE_UNIT:
                self._log.info('cancel unit cmd')
                self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_unit',
                                                  'arg' : command})


    # --------------------------------------------------------------------------
    #
    def _check_state(self):

        # Make sure that we haven't exceeded the runtime (if one is set). If
        # we have, terminate.
        if self._runtime:
            if time.time() >= self._starttime + (int(self._runtime) * 60):
                self._log.info("reached runtime limit (%ss).", self._runtime*60)
                self._final_cause = 'timeout'
                self._cb_lock.release()
                self.stop()


    # --------------------------------------------------------------------------
    #
    def _check_units(self):

        # Check if there are compute units waiting for input staging
        # and log that we pulled it.
        #
        # FIXME: Unfortunately, 'find_and_modify' is not bulkable, so we have
        #        to use 'find'.  To avoid finding the same units over and over 
        #        again, we update the 'control' field *before* running the next
        #        find -- so we do it right here.
        #        This also blocks us from using multiple ingest threads, or from
        #        doing late binding by unit pull :/
        unit_cursor = self._session._dbs._c.find(spec = {'type'    : 'unit',
                                                         'pilot'   : self._pilot_id,
                                                         'control' : 'agent_pending'})

        if not unit_cursor.count():
            # no units whatsoever...
            self._log.info("units pulled:    0")
            return False

        # update the units to avoid pulling them again next time.
        unit_list = list(unit_cursor)
        unit_uids = [unit['uid'] for unit in unit_list]

        self._log.info("units PULLED: %4d", len(unit_list))

        self._session._dbs._c.update(multi    = True,
                        spec     = {'type'  : 'unit',
                                    'uid'   : {'$in'     : unit_uids}},
                        document = {'$set'  : {'control' : 'agent'}})

        self._log.info("units pulled: %4d", len(unit_list))
        self._prof.prof('get', msg="bulk size: %d" % len(unit_list), uid=self._pilot_id)

        for unit in unit_list:

            # FIXME: raise or fail unit!
            if unit['control'] != 'agent_pending':
                self._log.error(' === invalid control: %s', (pprint.pformat(unit)))

            if unit['state'] != rps.AGENT_STAGING_INPUT_PENDING:
                self._log.error(' === invalid state: %s', (pprint.pformat(unit)))

            unit['control'] = 'agent'

            # we need to make sure to have the correct state:
            unit['state'] = rps._unit_state_collapse(unit['states'])
            self._prof.prof('get', msg="bulk size: %d" % len(unit_list), uid=unit['uid'])

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).  We don't publish nor profile as advance,
        # since that happened already on the module side when the state was set.
        self.advance(unit_list, publish=False, push=True, prof=False)

        # indicate that we did some work (if we did...)
        return True


# ------------------------------------------------------------------------------


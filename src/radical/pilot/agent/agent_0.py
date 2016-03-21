#!/usr/bin/env python

"""

This script is part of the pilot bootstrapping routing, representing the entry
into Python.  It is started on the landing node of the pilot job.

"""

__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import copy
import stat
import time
import pprint
import signal
import subprocess as sp
import setproctitle

import saga                    as rs
import radical.utils           as ru
import radical.pilot           as rp
import radical.pilot.utils     as rpu
import radical.pilot.states    as rps
import radical.pilot.constants as rpc


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
    def __init__(self):

        # load config, create session and controller, init rpu.Worker

        # load the agent config, and overload the config dicts
        cfg               = ru.read_json_str("%s/agent.cfg" % (os.getcwd()))
        self._uid         = 'agent_0'
        self._pilot_id    = cfg['pilot_id']
        self._session_id  = cfg['session_id']
        self._runtime     = cfg['runtime']
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

        # set up a logger and profiler
        self._prof = rpu.Profiler ('bootstrap_3')
        self._log  = ru.get_logger('bootstrap_3', '.', cfg.get('debug', 'DEBUG'))
        self._log.info('start')

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
        self._session = rp.Session(cfg=cfg, uid=self._session_id, _connect=True)
        ru.dict_merge(cfg, self._session.ctrl_cfg, ru.PRESERVE)
        pprint.pprint(cfg)

        if not self._session.is_connected:
            raise RuntimeError('agent could not connect to mongodb')

        # at this point the session is up, and the session controller should
        # have brought up all communication bridges and the UpdateWorker.  
        rpu.Worker.__init__(self, cfg, self._session, spawn=False)


    # --------------------------------------------------------------------------
    #
    def initialize_parent(self):

        # Create LRMS which will give us the set of agent_nodes to use for
        # sub-agent startup.  Add the remaining LRMS information to the
        # config, for the benefit of the scheduler).
        self._lrms = rp.agent.RM.create(name=self._cfg['lrms'], cfg=self._cfg, 
                                        session=self._session)

        # add the resource manager information to our own config
        self._cfg['lrms_info'] = self._lrms.lrms_info

        # create the sub-agent configs
        self._write_sa_configs()

        # and start the sub agents
        self._start_sub_agents()

        # register the command callback which pulls the DB for commands
        self.register_idle_cb(self._agent_command_cb, 
                              timeout=self._cfg['heartbeat_interval'])

        # registers the staging_input_queue as this is what we want to push
        # units to
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        # sub-agents are started, components are started, bridges are up: we are
        # ready to roll!
        pilot = {'type'    : 'pilot',
                 'uid'     : self._pilot_id,
                 'state'   : rps.ACTIVE,
                 'lm_info' : self._lrms.lm_info.get('version_info'),
                 '$set'    : 'lm_info'}
        self.advance(pilot, publish=True, push=False, prof=True)

        # register idle callback, to pull for units -- which is the only action
        # we have to perform, really
        self.register_idle_cb(self._idle_cb, timeout=self._cfg['db_poll_sleeptime'])


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


    # --------------------------------------------------------------------------
    #
    def _update_db(self, state, msg=None):
    
        self._log.info('pilot state: %s', state)
        self._log.info('rusage: %s', rpu.get_rusage())
        self._log.info(msg)

        if state == rp.FAILED:
            self._log.info(ru.get_trace())
    
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

            tmp_cfg = copy.deepcopy(sa_cfg)

            # merge sub_agent layout into the confoig
            ru.dict_merge(tmp_cfg, self._cfg['agent_layout'][sa], ru.OVERWRITE)

            tmp_cfg['agent_name'] = sa
            tmp_cfg['owner']      = self._pilot_id

            ru.write_json(tmp_cfg, './%s.cfg' % sa)


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
    
        if not self._cfg['agent_layout'].keys():
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
    
            target = self._cfg['agent_layout'][sa]['target']
    
            if target == 'local':
    
                # start agent locally
                cmdline = "/bin/sh -l %s/bootstrap_2.sh %s" % (os.getcwd(), sa)
    
            elif target == 'node':
    
                if not agent_lm:
                    agent_lm = rp.agent.LM.create(
                        name    = self._cfg['agent_launch_method'],
                        cfg     = self._cfg,
                        session = self._session)
    
                node = self._cfg['lrms_info']['agent_nodes'][sa]
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
                ls_name = "%s/%s.sh" % (os.getcwd(), sa)
                opaque_slots = {
                        'task_slots'   : ['%s:0' % node],
                        'task_offsets' : [],
                        'lm_info'      : self._cfg['lrms_info']['lm_info']}
                agent_cmd = {
                        'opaque_slots' : opaque_slots,
                        'description'  : {
                            'cores'      : 1,
                            'executable' : "/bin/sh",
                            'arguments'  : ["%s/bootstrap_2.sh" % os.getcwd(), sa]
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
            self._log.info ("create sub-agent %s: %s" % (sa, cmdline))
            sa_out = open("%s.out" % sa, "w")
            sa_err = open("%s.err" % sa, "w")
            sa_proc = sp.Popen(args=cmdline.split(), stdout=sa_out, stderr=sa_err)
    
            # make sure we can stop the sa_proc
            sa_proc.name = sa
            sa_proc.stop = sa_proc.terminate
            sub_agents.append(sa_proc)
    
        # the agents are up - register an idle callback to watch them
        # FIXME: make timeout configurable?
        self._session._controller.add_things(sub_agents, owner=self._pilot_id)
      # self.register_idle_cb(self._sa_watcher_cb, timeout=1.0)
    
        self._log.debug('start_sub_agents done')


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
    def _idle_cb(self):
        """
        This method will be driving all other agent components, in the sense
        that it will manage the connection to MongoDB to retrieve units, and
        then feed them to the respective component queues.
        """

        try:
            # check for new units
            return self._check_units()

        except Exception as e:
            # exception in the main loop is fatal
            self._log.exception("ERROR in agent main loop: %s" % e)
            sys.exit(1)


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
        unit_cursor = self._session._dbs._c.find(spec  = {
            'type'    : 'unit',
            'pilot'   : self._pilot_id,
            'control' : 'agent_pending'})

        if not unit_cursor.count():
            # no units whatsoever...
            self._log.info("units pulled:    0")
            return False

        # update the units to avoid pulling them again next time.
        unit_list = list(unit_cursor)
        unit_uids = [unit['uid'] for unit in unit_list]

        self._session._dbs._c.update(multi    = True,
                        spec     = {'type'  : 'unit',
                                    'uid'   : {'$in'     : unit_uids}},
                        document = {'$set'  : {'control' : 'agent'}})

        self._log.info("units pulled: %4d"   % len(unit_list))
        self._prof.prof('get', msg="bulk size: %d" % len(unit_list), uid=self._pilot_id)
        for unit in unit_list:
            unit['control'] = 'agent'
            self._prof.prof('get', msg="bulk size: %d" % len(unit_list), uid=unit['uid'])

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).  We don't publish nor profile as advance,
        # since that happened already on the module side when the state was set.
        self.advance(unit_list, publish=False, push=True, prof=False)

        # indicate that we did some work (if we did...)
        return True


# ------------------------------------------------------------------------------
#
def bootstrap_3():
    """
    This is only executed by agent
    """

    try:
        setproctitle.setproctitle('rp.agent_0')

        agent_0 = Agent_0()
        agent_0.start()

        # we never really quit this way, but instead the agent_0 command_cb may
        # pick up a shutdown signal, the watcher_cb may detect a failing
        # component or sub-agent, or we get a kill signal from the RM.  In all
        # three cases, we'll end up in agent_0.stop()
        while True:
            time.sleep(1)

    except SystemExit:
        log.exception("Exit running agent_0")

    except Exception as e:
        log.exception("Error running agent_0")

    finally:

        # in all cases, make sure we perform an orderly shutdown.  I hope python
        # does not mind doing all those things in a finally clause of
        # (essentially) main...
        agent_0.stop()


# ==============================================================================
#
# Agent bootstrap stage 4
#
# ==============================================================================
#
# avoid undefined vars on finalization / signal handling
def bootstrap_4(agent_name):
    """
    This method continues where the bootstrapper left off, but will soon pass
    control to the Agent class which will spawn the functional components.
    Before doing so, we will check if we happen to be agent instance zero.  If
    that is the case, some additional python level bootstrap routines kick in,
    to set the stage for component and sub-agent spawning.

    The agent interprets a config file, which will specify in an agent_layout
    section:
      - what nodes should be used for sub-agent startup
      - what bridges should be started
      - what are the endpoints for bridges which are not started
      - what components should be started
    bootstrap_3 will create derived config files for all sub-agents.
    """

    try:
        assert(agent_name != 'agent_0')

        print "startup agent %s" % agent_name
        setproctitle.setproctitle('rp.%s' % agent_name)

        # load the agent config, and overload the config dicts
        agent      = None
        agent_cfg  = "%s/%s.cfg" % (os.getcwd(), agent_name)
        cfg        = ru.read_json_str(agent_cfg)
        pilot_id   = cfg['pilot_id']
        session_id = cfg['session_id']

        # set up a logger and profiler
        prof = rpu.Profiler ('%s.bootstrap_3' % agent_name)
        prof.prof('sync ref', msg='%s start' % agent_name, uid=pilot_id)

        log = ru.get_logger('%s.bootstrap_3'     % agent_name,
                            '%s.bootstrap_3.log' % agent_name, cfg.get('debug', 'INFO'))
        log.info('start')

        print "Agent config (%s):\n%s\n\n" % (agent_cfg, pprint.pformat(cfg))

        # des Pudels Kern
        agent = rp.worker.Agent(cfg)
        agent.start()
        agent.join()
        log.debug('%s joined', agent_name)


    except SystemExit:
        log.exception("Exit running %s" % agent_name)

    except Exception as e:
        log.exception("Error running %s" % agent_name)

    finally:

        # in all cases, make sure we perform an orderly shutdown.  I hope python
        # does not mind doing all those things in a finally clause of
        # (essentially) main...
        if agent:
            agent.stop()

        log.debug('%s finalized' % agent_name)
        prof.prof('stop', msg='finally clause %s' % agent_name, uid=pilot_id)
        prof.close()


# ==============================================================================
#
if __name__ == "__main__":

    print "---------------------------------------------------------------------"
    print
    print "PYTHONPATH: %s"  % sys.path
    print "python: %s"      % sys.version
    print "utils : %-5s : %s" % (ru.version_detail, ru.__file__)
    print "saga  : %-5s : %s" % (rs.version_detail, rs.__file__)
    print "pilot : %-5s : %s" % (rp.version_detail, rp.__file__)
    print "        type  : multicore"
    print "        gitid : %s" % git_ident
    print
    print "---------------------------------------------------------------------"
    print

    
    agent_name=sys.argv[1]

    if agent_name == 'agent_0':
        # spawn sub agents
        bootstrap_3()
        print "bootstrap_3 done"

    else:
        # this is a sub agent - bootstrap it!
        bootstrap_4(agent_name)
        print "bootstrap_4 done (%s)" % agent_name



#
# ------------------------------------------------------------------------------


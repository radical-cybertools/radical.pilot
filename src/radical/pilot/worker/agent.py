
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import copy
import stat
import time
import subprocess

import radical.utils as ru

from ..  import utils     as rpu
from ..  import states    as rps
from ..  import constants as rpc


# ==============================================================================
# defaults
DEFAULT_HEARTBEAT_INTERVAL = 10.0   # seconds


# ==============================================================================
#
class Agent(rpu.Worker):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.final_cause  = None
        self._lrms        = None

        self._agent_name  = cfg['agent_name']
        self._session_id  = cfg['session_id']
        self._pilot_id    = cfg['pilot_id']

        self._uid  = self._agent_name

        rpu.Worker.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._log.debug('starting AgentWorker for %s' % self.uid)


    # --------------------------------------------------------------------------
    #
    @property
    def agent_name(self):
        return self._agent_name


    # --------------------------------------------------------------------------
    #
    def _agent_barrier_cb(self, topic, msg):
        """
        bar until we have seen all agents coming live
        """

        # FIXME: use same barrier logic as Component

        self._log.debug('agent_barrier_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'agent_alive':

            if arg in self._sub_agents:
                self._log.debug("sub-agent ALIVE (%s)" % arg)
                self._sub_agents[arg]['alive'] = True


    # --------------------------------------------------------------------------
    #
    def _write_sub_configs(self):
        """
        create a sub_config for each sub-agent we intent to spawn
        """

        if self._agent_name != 'agent_0':
            raise RuntimeError('only agent_0 writes config files')
    
        # write deep-copies of the config (with the corrected agent_name) for each
        # sub-agent (apart from agent_0)
        for sa in self.cfg.get('agent_layout', []):
            if sa != 'agent_0':
                sa_cfg = copy.deepcopy(self.cfg)
                sa_cfg['agent_name'] = sa
                sa_cfg['owner']      = self._pilot_id
                ru.write_json(sa_cfg, './%s.cfg' % sa)
    
    
    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        Read the configuration file, setup logging and mongodb connection.
        This prepares the stage for the component setup (self._setup()).
        """

        # keep track of sub-agents we want to possibly spawn
        # sanity check on config settings
        if not 'cores'               in self._cfg: raise ValueError("Missing number of cores")
        if not 'debug'               in self._cfg: raise ValueError("Missing DEBUG level")
        if not 'lrms'                in self._cfg: raise ValueError("Missing LRMS")
        if not 'mongodb_url'         in self._cfg: raise ValueError("Missing MongoDB URL")
        if not 'pilot_id'            in self._cfg: raise ValueError("Missing pilot id")
        if not 'runtime'             in self._cfg: raise ValueError("Missing or zero agent runtime")
        if not 'scheduler'           in self._cfg: raise ValueError("Missing agent scheduler")
        if not 'session_id'          in self._cfg: raise ValueError("Missing session id")
        if not 'spawner'             in self._cfg: raise ValueError("Missing agent spawner")
        if not 'task_launch_method'  in self._cfg: raise ValueError("Missing unit launch method")
        if not 'agent_layout'        in self._cfg: raise ValueError("Missing agent layout")

        self._runtime    = self._cfg['runtime']
        self._starttime  = time.time()
        self._sub_agents = dict()
        self._layout     = self._cfg['agent_layout'][self._agent_name]
        self._pull_units = self._layout.get('pull_units', False)

        # this better be on a shared FS!
        self._cfg['workdir'] = os.getcwd()

        # another sanity check for agent_0
        if self.agent_name == 'agent_0':
            if self._layout.get('target', 'local') != 'local':
                raise ValueError("agent_0 must run on target 'local'")

        # the master agent has a couple of additional tasks
        if self.agent_name == 'agent_0':

            from ... import pilot as rp

            # only the master agent creates LRMS and sub-agent config files.
            # The LRMS which will give us the set of agent_nodes to use for
            # sub-agent startup.  Add the remaining LRMS information to the
            # config, for the benefit of the scheduler).
            self._lrms = rp.agent.RM.create(name    = self._cfg['lrms'],
                                            cfg     = self._cfg,
                                            session = self._session)
            self._cfg['lrms_info'] = self._lrms.lrms_info

            # we now have correct bridge addresses added to the agent_0.cfg, and all
            # other agents will have picked that up from their config files -- we
            # can start the agent and all its components!

            # the master agent also is the only one which starts bridges.  This
            # will store the bridge addresses in self._cfg, so that we can use
            # them for starting components.
            bridges = self._cfg['agent_layout']['agent_0'].get('bridges', {})
            self.start_bridges(bridges)

            # we have all information needed by the subagents -- write the
            # sub-agent config files.
            self._write_sub_configs()

        # make sure we collect commands, specifically to implement the startup
        # barrier on bootstrap_4
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._agent_barrier_cb)


        # register the heartbeat callback which pulls the DBfor command and
        # idle callback)
        self.register_idle_cb(self._agent_heartbeat_cb, 
                              timeout=self._cfg.get('heartbeat_interval', 
                                                      DEFAULT_HEARTBEAT_INTERVAL))

        # Now instantiate all communication and notification channels, and all
        # components and workers.  It will then feed a set of units to the
        # lead-in queue (staging_input).  A state notification callback will
        # then register all units which reached a final state (DONE).  Once all
        # units are accounted for, it will tear down all created objects.
        try:
            self.start_sub_agents()

            # create the required number of agent components and workers,
            # according to the config
            clist = self._layout.get('components',{})
            import pprint
            self._log.debug(pprint.pformat(self._cfg))
            self.start_components(components=clist)

        except Exception as e:
            self._log.exception("Agent setup error: %s" % e)
            raise

        self._prof.prof('Agent setup done', logger=self._log.debug, uid=self._pilot_id)

        # once bootstrap_4 is done, we signal success to the parent agent
        # -- if we have any parent...
        if self.agent_name != 'agent_0':
            self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'agent_alive',
                                              'arg' : self.agent_name})

        # sub-agents are started, components are started, bridges are up -- we
        # are ready to roll!
        # FIXME: this should be a state advance!
        if self.agent_name == 'agent_0':
            self._log.debug('### agent db update: %s', self._session._dbs._c)
            now = rpu.timestamp()
            ver = self._lrms.lm_info.get('version_info')
            ret = self._session._dbs._c.update(
                    {'type' : 'pilot',
                     "uid"  : self._pilot_id},
                    {"$set" : {"state"        : rps.ACTIVE,
                               "started"      : now,
                               "lm_info"      : ver},
                     "$push": {"states"       : rps.ACTIVE}})


            # TODO: Check for return value, update should be true!
            self._log.info("Database updated: %s", ret)



        # the pulling agent registers the staging_input_queue as this is what we want to push to
        # FIXME: do a sanity check on the config that only one agent pulls, as
        #        this is a non-atomic operation at this point
        self._log.debug('agent will pull units: %s' % bool(self._pull_units))
        if self._pull_units:

            self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                                 rpc.AGENT_STAGING_INPUT_QUEUE)

            # register idle callback, to pull for units -- which is the only action
            # we have to perform, really
            self.register_idle_cb(self._idle_cb, 
                                  timeout=self._cfg.get('db_poll_sleeptime', 1.0))


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        self._log.info("Agent finalizes")
        self._prof.prof('stop', uid=self._pilot_id)

        # burn the bridges, burn EVERYTHING
        for sa in self._sub_agents:
            thing = self._sub_agents[sa]
            try:
                self._log.info("closing sub-agent %s", sa)
                thing['handle'].stop()
            except Exception as e:
                self._log.exception('ignore failing sub-agent terminate')

        if self._lrms:
            self._lrms.stop()

        self._log.info("Agent finalized")


    # --------------------------------------------------------------------------
    #
    def _sa_watcher_cb(self):
        """
        we do a poll() on all sub-agent, to check if they are still alive.  
        If any goes AWOL, we will begin to tear down this agent.
        """

        self._log.debug('checking %s things' % len(self._sub_agents))
        for sa in self._sub_agents:
            thing = self._sub_agents[sa]
            state = thing['handle'].poll()
            if state == None:
                self._log.debug('%-40s: ok' % sa)
            else:
                raise RuntimeError ('%s died - shutting down' % sa)

        return True # always idle


    # --------------------------------------------------------------------------
    #
    def start_sub_agents(self):
        """
        For the list of sub_agents, get a launch command and launch that
        agent instance on the respective node.  We pass it to the seconds
        bootstrap level, there is no need to pass the first one again.
        """

        # FIXME: we need a watcher cb to watch sub-agent state

        from ... import pilot as rp

        self._log.debug('start_sub_agents')

        sa_list = self._layout.get('sub_agents', [])

        if not sa_list:
            self._log.debug('start_sub_agents noop')
            return

        # the configs are written, and the sub-agents can be started.  To know
        # how to do that we create the agent launch method, have it creating
        # the respective command lines per agent instance, and run via
        # popen.
        #
        # actually, we only create the agent_lm once we really need it for
        # non-local sub_agents.
        agent_lm = None
        for sa in sa_list:

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
                # FIXME: are we using the 'hop' correctly?
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
            self._prof.prof("create", msg=sa, uid=self._pilot_id)
            self._log.info ("create sub-agent %s: %s" % (sa, cmdline))
            sa_out = open("%s.out" % sa, "w")
            sa_err = open("%s.err" % sa, "w")
            sa_proc = subprocess.Popen(args=cmdline.split(), stdout=sa_out, stderr=sa_err)

            # make sure we can stop the sa_proc
            sa_proc.stop = sa_proc.terminate

            self._sub_agents[sa] = {'handle': sa_proc,
                                    'out'   : sa_out,
                                    'err'   : sa_err,
                                    'pid'   : sa_proc.pid,
                                    'alive' : False}
            self._prof.prof("created", msg=sa, uid=self._pilot_id)

        # the agents are up - register an idle callback to watch them
        # FIXME: make timeout configurable?
        self.register_idle_cb(self._sa_watcher_cb, timeout=1.0)

        self._log.debug('start_sub_agents done')


    # --------------------------------------------------------------------------
    #
    def _idle_cb(self):
        """
        This method will be driving all other agent components, in the sense
        that it will manage the connection to MongoDB to retrieve units, and
        then feed them to the respective component queues.
        """

        # only do something if configured to do so
        if not self._pull_units:
            self._log.debug('not configured to pull for units')
            return True  # fake work to avoid busy noops

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


    # --------------------------------------------------------------------------
    #
    def _agent_heartbeat_cb(self):

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



# ------------------------------------------------------------------------------


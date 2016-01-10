
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
#
class Agent(rpu.Worker):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self.agent_name = cfg['agent_name']
        rpu.Worker.__init__(self, 'AgentWorker', cfg)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._log.debug('starting AgentWorker for %s' % self.agent_name)

        # everything which comes after the worker init is limited in scope to
        # the current process, and will not be available in the worker process.
        self._pilot_id    = self._cfg['pilot_id']
        self._session_id  = self._cfg['session_id']
        self.final_cause  = None

        # all components use the command channel for control messages
        self.declare_subscriber('command', rpc.AGENT_COMMAND_PUBSUB, self.command_cb)


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        # This callback is invoked as a thread in the process context of the
        # main agent (parent process) class.
        #
        # NOTE: That means it is *not* joined in the finalization of the run
        # loop (child), and the subscriber thread needs to be joined specifically in the
        # current process context.  At the moment that requires a call to
        # self._finalize() in the main process.

        cmd = msg['cmd']
        arg = msg['arg']

        self._log.info('agent command: %s %s' % (cmd, arg))

        if cmd == 'shutdown':

            # let agent know what caused the termination (first cause)
            if not self.final_cause:
                self.final_cause = arg

                self._log.info("shutdown command (%s)" % arg)
                self.stop()

            else:
                self._log.info("shutdown command (%s) - ignore" % arg)


    # --------------------------------------------------------------------------
    #
    def barrier_cb(self, topic, msg):

        # This callback is invoked in the process context of the run loop, and
        # will be cleaned up automatically.

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'alive':

            name = arg
            self._log.debug('waiting alive: \n%s\n%s\n%s'
                    % (self._components.keys(), self._workers.keys(),
                        self._sub_agents.keys()))

            # we only look at ALIVE messages which come from *this* agent, and
            # simply ignore all others (this is a shared medium after all)
            if name.startswith (self.agent_name):

                if name in self._components:
                    self._log.debug("component ALIVE (%s)" % name)
                    self._components[name]['alive'] = True

                elif name in self._workers:
                    self._log.debug("worker    ALIVE (%s)" % name)
                    self._workers[name]['alive'] = True

                else:
                    self._log.error("unknown   ALIVE (%s)" % name)

            elif name in self._sub_agents:
                self._log.debug("sub-agent ALIVE (%s)" % name)
                self._sub_agents[name]['alive'] = True


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        Read the configuration file, setup logging and mongodb connection.
        This prepares the stage for the component setup (self._setup()).
        """

        # keep track of objects we need to stop in the finally clause
        self._sub_agents = dict()
        self._components = dict()
        self._workers    = dict()

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

        self._pilot_id   = self._cfg['pilot_id']
        self._session_id = self._cfg['session_id']
        self._runtime    = self._cfg['runtime']
        self._sub_cfg    = self._cfg['agent_layout'][self.agent_name]
        self._pull_units = self._sub_cfg.get('pull_units', False)

        # this better be on a shared FS!
        self._cfg['workdir'] = os.getcwd()

        # another sanity check
        if self.agent_name == 'agent_0':
            if self._sub_cfg.get('target', 'local') != 'local':
                raise ValueError("agent_0 must run on target 'local'")

        # configure the agent logger
        self._log.setLevel(self._cfg['debug'])

        # set up db connection -- only for the master agent and for the agent
        # which pulls units (which might be the same)
        if self.agent_name == 'agent_0' or self._pull_units:
            self._log.debug('connecting to mongodb at %s for unit pull')
            _, mongo_db, _, _, _  = ru.mongodb_connect(self._cfg['mongodb_url'])

            self._p  = mongo_db["%s.p"  % self._session_id]
            self._cu = mongo_db["%s.cu" % self._session_id]
            self._log.debug('connected to mongodb')

        # first order of business: set the start time and state of the pilot
        # Only the master agent performs this action
        if self.agent_name == 'agent_0':
            now = rpu.timestamp()
            ret = self._p.update(
                {"_id": self._pilot_id},
                {"$set" : {"state"        : rps.ACTIVE,
                           "started"      : now},
                 "$push": {"statehistory" : {"state"    : rps.ACTIVE,
                                             "timestamp": now}}
                })
            # TODO: Check for return value, update should be true!
            self._log.info("Database updated: %s", ret)

        # make sure we collect commands, specifically to implement the startup
        # barrier on bootstrap_4
        self.declare_publisher ('command', rpc.AGENT_COMMAND_PUBSUB)
        self.declare_subscriber('command', rpc.AGENT_COMMAND_PUBSUB, self.barrier_cb)

        # Now instantiate all communication and notification channels, and all
        # components and workers.  It will then feed a set of units to the
        # lead-in queue (staging_input).  A state notification callback will
        # then register all units which reached a final state (DONE).  Once all
        # units are accounted for, it will tear down all created objects.

        # we pick the layout according to our role (name)
        # NOTE: we don't do sanity checks on the agent layout (too lazy) -- but
        #       we would hiccup badly over ill-formatted or incomplete layouts...
        if not self.agent_name in self._cfg['agent_layout']:
            raise RuntimeError("no agent layout section for %s" % self.agent_name)

        try:
            self.start_sub_agents()
            self.start_components()

            # before we declare bootstrapping-success, the we wait for all
            # components, workers and sub_agents to complete startup.  For that,
            # all sub-agents will wait ALIVE messages on the COMMAND pubsub for
            # all entities it spawned.  Only when all are alive, we will
            # continue here.
            self.alive_barrier()

        except Exception as e:
            self._log.exception("Agent setup error: %s" % e)
            raise

        self._prof.prof('Agent setup done', logger=self._log.debug, uid=self._pilot_id)

        # also watch all components (once per second)
        self.declare_idle_cb(self.watcher_cb, 10.0)

        # once bootstrap_4 is done, we signal success to the parent agent
        # -- if we have any parent...
        if self.agent_name != 'agent_0':
            self.publish('command', {'cmd' : 'alive',
                                     'arg' : self.agent_name})

        # the pulling agent registers the staging_input_queue as this is what we want to push to
        # FIXME: do a sanity check on the config that only one agent pulls, as
        #        this is a non-atomic operation at this point
        self._log.debug('agent will pull units: %s' % bool(self._pull_units))
        if self._pull_units:

            self.declare_output(rps.AGENT_STAGING_INPUT_PENDING, rpc.AGENT_STAGING_INPUT_QUEUE)
            self.declare_publisher('state', rpc.AGENT_STATE_PUBSUB)

            # register idle callback, to pull for units -- which is the only action
            # we have to perform, really
            self.declare_idle_cb(self.idle_cb, self._cfg['db_poll_sleeptime'])


    # --------------------------------------------------------------------------
    #
    def alive_barrier(self):

        # FIXME: wait for bridges, too?  But we need pubsub for counting... Duh!
        total = len(self._components) + \
                len(self._workers   ) + \
                len(self._sub_agents)
        start   = time.time()
        timeout = 300

        while True:
            # check the procs for all components which are not yet alive
            to_check  = self._components.items() \
                      + self._workers.items() \
                      + self._sub_agents.items()

            alive_cnt = 0
            total_cnt = len(to_check)
            for name,c in to_check:
                if c['alive']:
                    alive_cnt += 1
                else:
                    self._log.debug('checking %s: %s', name, c)
                    if None != c['handle'].poll():
                        # process is dead and has never been alive.  Oops
                        raise RuntimeError('component %s did not come up' % name)

            self._log.debug('found alive: %2d / %2d' % (alive_cnt, total_cnt))

            if alive_cnt == total_cnt:
                self._log.debug('bootstrap barrier success')
                break

            if time.time() - timeout > start:
                raise RuntimeError('component barrier failed (timeout)')

            time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def watcher_cb(self):
        """
        we do a poll() on all our bridges, components, workers and sub-agent,
        to check if they are still alive.  If any goes AWOL, we will begin to
        tear down this agent.
        """

        to_watch = list(self._components.iteritems()) \
                 + list(self._workers.iteritems())    \
                 + list(self._sub_agents.iteritems())

      # self._log.debug('watch: %s' % pprint.pformat(to_watch))

        self._log.debug('checking %s things' % len(to_watch))
        for name, thing in to_watch:
            state = thing['handle'].poll()
            if state == None:
                self._log.debug('%-40s: ok' % name)
            else:
                raise RuntimeError ('%s died - shutting down' % name)

        return True # always idle


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        self._log.info("Agent finalizes")
        self._prof.prof('stop', uid=self._pilot_id)

        # tell other sub-agents get lost
        self.publish('command', {'cmd' : 'shutdown',
                                 'arg' : '%s finalization' % self.agent_name})

        # burn the bridges, burn EVERYTHING
        for name,sa in self._sub_agents.items():
            try:
                self._log.info("closing sub-agent %s", sa)
                sa['handle'].stop()
            except Exception as e:
                self._log.exception('ignore failing sub-agent terminate')

        for name,c in self._components.items():
            try:
                self._log.info("closing component %s", c)
                c['handle'].stop()
            except Exception as e:
                self._log.exception('ignore failing component terminate')

        for name,w in self._workers.items():
            try:
                self._log.info("closing worker %s", w)
                w['handle'].stop()
            except Exception as e:
                self._log.exception('ignore failing worker terminate')

        # communicate finalization to parent agent
        # -- if we have any parent...
        if self.agent_name != 'agent_0':
            self.publish('command', {'cmd' : 'final',
                                     'arg' : self.agent_name})

        self._log.info("Agent finalized")


    # --------------------------------------------------------------------------
    #
    def start_sub_agents(self):
        """
        For the list of sub_agents, get a launch command and launch that
        agent instance on the respective node.  We pass it to the seconds
        bootstrap level, there is no need to pass the first one again.
        """

        from ... import pilot as rp

        self._log.debug('start_sub_agents')

        sa_list = self._sub_cfg.get('sub_agents', [])

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
                        name   = self._cfg['agent_launch_method'],
                        cfg    = self._cfg,
                        logger = self._log)

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

        self._log.debug('start_sub_agents done')

    # --------------------------------------------------------------------------
    #
    def start_components(self):
        """
        For all componants defined on this agent instance, create the required
        number of those.  Keep a handle around for shutting them down later.
        """

        from ... import pilot as rp

        self._log.debug("start_components")

        # We use a static map from component names to class types for now --
        # a factory might be more appropriate (FIXME)
        cmap = {
            "AgentStagingInputComponent"  : rp.agent.Input,
            "AgentSchedulingComponent"    : rp.agent.Scheduler,
            "AgentExecutingComponent"     : rp.agent.Executing,
            "AgentStagingOutputComponent" : rp.agent.Output,
            }
        for cname, cnum in self._sub_cfg.get('components',{}).iteritems():
            for i in range(cnum):
                # each component gets its own copy of the config
                ccfg = copy.deepcopy(self._cfg)
                ccfg['number'] = i
                comp = cmap[cname].create(ccfg)
                comp.start()
                self._components[comp.childname] = {'handle' : comp,
                                                    'alive'  : False}
                self._log.info('created component %s (%s): %s', cname, cnum, comp.cname)

        # we also create *one* instance of every 'worker' type -- which are the
        # heartbeat and update worker.  To ensure this, we only create workers
        # in agent_0.
        # FIXME: make this configurable, both number and placement
        if self.agent_name == 'agent_0':
            wmap = {
                rpc.AGENT_UPDATE_WORKER    : rp.worker.Update,
                rpc.AGENT_HEARTBEAT_WORKER : rp.worker.Heartbeat
                }
            for wname in wmap:
                self._log.info('create worker %s', wname)
                wcfg   = copy.deepcopy(self._cfg)
                worker = wmap[wname].create(wcfg)
                worker.start()
                self._workers[worker.childname] = {'handle' : worker,
                                                   'alive'  : False}

        self._log.debug("start_components done")


    # --------------------------------------------------------------------------
    #
    def idle_cb(self):
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
            return self.check_units()

        except Exception as e:
            # exception in the main loop is fatal
            self._log.exception("ERROR in agent main loop: %s" % e)
            sys.exit(1)


    # --------------------------------------------------------------------------
    #
    def check_units(self):

        # Check if there are compute units waiting for input staging
        # and log that we pulled it.
        #
        # FIXME: Unfortunately, 'find_and_modify' is not bulkable, so we have
        # to use 'find'.  To avoid finding the same units over and over again,
        # we update the state *before* running the next find -- so we do it
        # right here...  No idea how to avoid that roundtrip...
        # This also blocks us from using multiple ingest threads, or from doing
        # late binding by unit pull :/
        cu_cursor = self._cu.find(spec  = {"pilot"   : self._pilot_id,
                                           'state'   : rps.AGENT_STAGING_INPUT_PENDING,
                                           'control' : 'umgr'})
        if not cu_cursor.count():
            # no units whatsoever...
            self._log.info("units pulled:    0")
            return False

        # update the unit states to avoid pulling them again next time.
        cu_list = list(cu_cursor)
        cu_uids = [cu['_id'] for cu in cu_list]

        self._cu.update(multi    = True,
                        spec     = {"_id"   : {"$in"     : cu_uids}},
                        document = {"$set"  : {"control" : 'agent'}})

        self._log.info("units pulled: %4d"   % len(cu_list))
        self._prof.prof('get', msg="bulk size: %d" % len(cu_list), uid=self._pilot_id)
        for cu in cu_list:
            self._prof.prof('get', msg="bulk size: %d" % len(cu_list), uid=cu['_id'])

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).  We don't publish nor profile as advance,
        # since that happened already on the module side when the state was set.
        self.advance(cu_list, publish=False, push=True, prof=False)

        # indicate that we did some work (if we did...)
        return True


# ------------------------------------------------------------------------------


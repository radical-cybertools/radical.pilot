
__copyright__ = 'Copyright 2014-2016, http://radical.rutgers.edu'
__license__   = 'MIT'

import copy
import os
import pprint
import stat
import time

import radical.utils       as ru

from ..   import utils     as rpu
from ..   import states    as rps
from ..   import constants as rpc
from ..db import DBSession

from .resource_manager import ResourceManager


# ------------------------------------------------------------------------------
#
class Agent_0(rpu.Worker):

    '''
    This is the main agent.  It starts sub-agents and watches them.  If any of
    the sub-agents die, it will shut down the other sub-agents and itself.

    This class inherits the rpu.Worker, so that it can use its communication
    bridges and callback mechanisms.  Specifically, it will pull the DB for
    new tasks to be exexuted and forwards them to the agent's component
    network (see `work()`).  It will also watch the DB for any commands to be
    forwarded (pilot termination, task cancelation, etc), and will take care
    of heartbeat messages to be sent to the client module.  To do all this, it
    initializes a DB connection in `initialize()`.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid     = 'agent.0'
        self._cfg     = cfg
        self._pid     = cfg.pid
        self._pmgr    = cfg.pmgr
        self._pwd     = cfg.pilot_sandbox
        self._session = session
        self._log     = ru.Logger(self._uid, ns='radical.pilot')

        self._starttime   = time.time()
        self._final_cause = None

        # this is the earliest point to sync bootstrap and agent profiles
        self._prof = ru.Profiler(ns='radical.pilot', name=self._uid)
        self._prof.prof('hostname', uid=cfg.pid, msg=ru.get_hostname())

        # run an inline registry service to share runtime config with other
        # agents and components
        reg_uid = 'radical.pilot.reg.%s' % self._uid
        self._reg_service = ru.zmq.Registry(uid=reg_uid)
        self._reg_service.start()

        # let all components know where to look for the registry
        self._cfg['reg_addr'] = self._reg_service.addr

        # connect to MongoDB for state push/pull
        self._connect_db()

        # configure ResourceManager before component startup, as components need
        # ResourceManager information for function (scheduler, executor)
        self._configure_rm()

        # ensure that app communication channels are visible to workload
        self._configure_app_comm()

        # expose heartbeat channel to sub-agents, bridges and components,
        # and start those
        self._cmgr = rpu.ComponentManager(self._cfg)
        self._cfg.heartbeat = self._cmgr.cfg.heartbeat

        self._cmgr.start_bridges()
        self._cmgr.start_components()

        # start any services if they are requested
        self._start_services()

        # create the sub-agent configs and start the sub agents
        self._write_sa_configs()
        self._start_sub_agents()   # TODO: move to cmgr?

        # at this point the session is up and connected, and it should have
        # brought up all communication bridges and components.  We are
        # ready to rumble!
        rpu.Worker.__init__(self, self._cfg, session)

        self.register_subscriber(rpc.CONTROL_PUBSUB, self._check_control)

        # run our own slow-paced heartbeat monitor to watch pmgr heartbeats
        # FIXME: we need to get pmgr freq
        freq = 60
        tint = freq / 3
        tout = freq * 10
        self._hb = ru.Heartbeat(uid=self._uid,
                                timeout=tout,
                                interval=tint,
                                beat_cb=self._hb_check,  # no own heartbeat(pmgr pulls)
                                term_cb=self._hb_term_cb,
                                log=self._log)
        self._hb.start()

        # register pmgr heartbeat
        self._log.info('hb init for %s', self._pmgr)
        self._hb.beat(uid=self._pmgr)


    # --------------------------------------------------------------------------
    #
    def _hb_check(self):

        self._log.debug('hb check')


    # --------------------------------------------------------------------------
    #
    def _hb_term_cb(self, msg=None):

        self._cmgr.close()
        self._log.warn('hb termination: %s', msg)

        return None


    # --------------------------------------------------------------------------
    #
    def _connect_db(self):

        # Check for the RADICAL_PILOT_DB_HOSTPORT env var, which will hold
        # the address of the tunnelized DB endpoint. If it exists, we
        # overrule the agent config with it.
        hostport = os.environ.get('RADICAL_PILOT_DB_HOSTPORT')
        if hostport:
            host, port = hostport.split(':', 1)
            dburl      = ru.Url(self._cfg.dburl)
            dburl.host = host
            dburl.port = port
            self._cfg.dburl = str(dburl)

        self._dbs = DBSession(sid=self._cfg.sid, dburl=self._cfg.dburl,
                              cfg=self._cfg, log=self._log)

    # --------------------------------------------------------------------------
    #
    def _configure_rm(self):

        # Create ResourceManager which will give us the set of agent_nodes to
        # use for sub-agent startup.  Add the remaining ResourceManager
        # information to the config, for the benefit of the scheduler).

        self._rm = ResourceManager.create(name=self._cfg.resource_manager,
                                          cfg=self._cfg, log=self._log,
                                          prof=self._prof)

        self._log.debug(pprint.pformat(self._rm.info))


    # --------------------------------------------------------------------------
    #
    def _configure_app_comm(self):

        # if the pilot description contains a request for application comm
        # channels, merge those into the agent config
        #
        # FIXME: this needs to start the app_comm bridges
        app_comm = self._cfg.get('app_comm')
        if app_comm:
            if isinstance(app_comm, list):
                app_comm = {ac: {'bulk_size': 0,
                                 'stall_hwm': 1,
                                 'log_level': 'error'} for ac in app_comm}
            for ac in app_comm:
                if ac in self._cfg['bridges']:
                    raise ValueError('reserved app_comm name %s' % ac)
                self._cfg['bridges'][ac] = app_comm[ac]


        # some of the bridge addresses also need to be exposed to the workload
        if app_comm:
            if 'task_environment' not in self._cfg:
                self._cfg['task_environment'] = dict()
            for ac in app_comm:
                if ac not in self._cfg['bridges']:
                    raise RuntimeError('missing app_comm %s' % ac)
                self._cfg['task_environment']['RP_%s_IN' % ac.upper()] = \
                        self._cfg['bridges'][ac]['addr_in']
                self._cfg['task_environment']['RP_%s_OUT' % ac.upper()] = \
                        self._cfg['bridges'][ac]['addr_out']


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # registers the staging_input_queue as this is what we want to push
        # tasks to
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        # register the command callback which pulls the DB for commands
        self.register_timed_cb(self._agent_command_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # register idle callback to pull for tasks
        self.register_timed_cb(self._check_tasks_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # sub-agents are started, components are started, bridges are up: we are
        # ready to roll!  Update pilot state.
        pilot = {'type'             : 'pilot',
                 'uid'              : self._pid,
                 'state'            : rps.PMGR_ACTIVE,
                 'resource_details' : {
                     # 'lm_info'      : self._rm.lm_info.get('version_info'),
                     # 'lm_detail'    : self._rm.lm_info.get('lm_detail'),
                     'rm_info'      : self._rm.info},
                 '$set'             : ['resource_details']}
        self.advance(pilot, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def work(self):

        # all work is done in the registered callbacks
        time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def stage_output(self):

        if  os.path.isfile('./staging_output.txt'):

            if not os.path.isfile('./staging_output.tgz'):

                cmd = 'tar zcvf staging_output.tgz $(cat staging_output.txt)'
                out, err, ret = ru.sh_callout(cmd, shell=True)

                if ret:
                    self._log.debug('out: %s', out)
                    self._log.debug('err: %s', err)
                    self._log.error('output tarring failed: %s', cmd)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # tar up output staging data
        self._log.debug('stage output parent')
        self.stage_output()

        # tear things down in reverse order
        self._hb.stop()
        self._cmgr.close()

        if self._rm:
            self._rm.stop()

        self._reg_service.stop()

        if   self._final_cause == 'timeout'  : state = rps.DONE
        elif self._final_cause == 'cancel'   : state = rps.CANCELED
        elif self._final_cause == 'sys.exit' : state = rps.CANCELED
        else                                 : state = rps.FAILED

        # NOTE: we do not push the final pilot state, as that is done by the
        #       bootstrapper *after* this pilot *actually* finished.
        with ru.ru_open('./killme.signal', 'w') as fout:
            fout.write('%s\n' % state)

        # we don't rely on the existence / viability of the update worker at
        # that point.
        self._log.debug('update db state: %s: %s', state, self._final_cause)
        self._log.info('rusage: %s', rpu.get_rusage())

        out, err, log = '', '', ''

        try   : out   = ru.ru_open('./agent.0.out', 'r').read(1024)
        except: pass
        try   : err   = ru.ru_open('./agent.0.err', 'r').read(1024)
        except: pass
        try   : log   = ru.ru_open('./agent.0.log', 'r').read(1024)
        except: pass

        ret = self._dbs._c.update({'type' : 'pilot',
                                   'uid'  : self._pid},
                                  {'$set' : {'stdout' : rpu.tail(out),
                                             'stderr' : rpu.tail(err),
                                             'logfile': rpu.tail(log),
                                             'state'  : state},
                                   '$push': {'states' : state}
                                  })
        self._log.debug('update ret: %s', ret)


    # --------------------------------------------------------------------
    #
    def _write_sa_configs(self):

        # we have all information needed by the subagents -- write the
        # sub-agent config files.

        # write deep-copies of the config for each sub-agent (sans from agent.0)
        for sa in self._cfg.get('agents', {}):

            assert(sa != 'agent.0'), 'expect subagent, not agent.0'

            # use our own config sans agents/components/bridges as a basis for
            # the sub-agent config.
            tmp_cfg = copy.deepcopy(self._cfg)
            tmp_cfg['agents']     = dict()
            tmp_cfg['components'] = dict()
            tmp_cfg['bridges']    = dict()

            # merge sub_agent layout into the config
            ru.dict_merge(tmp_cfg, self._cfg['agents'][sa], ru.OVERWRITE)

            tmp_cfg['uid']   = sa
            tmp_cfg['aid']   = sa
            tmp_cfg['owner'] = 'agent.0'

            ru.write_json(tmp_cfg, './%s.cfg' % sa)


    # --------------------------------------------------------------------------
    #
    def _start_services(self):
        '''
        If a `./services` file exist, reserve a compute node and run that file
        there as bash script.
        '''

        if not os.path.isfile('./services'):
            return

        # launch the `./services` script on the service node reserved by the RM.
        nodes = self._rm.info.service_node_list
        assert(nodes)

        bs_name = "%s/bootstrap_2.sh"     % self._pwd
        ls_name = "%s/services_launch.sh" % self._pwd
        ex_name = "%s/services_exec.sh"   % self._pwd

        threads = self._rm.info.cores_per_node * \
                  self._rm.info.threads_per_core

        service_task = {
            'uid'              : 'rp.services',
            'task_sandbox_path': self._pwd,
            'description'      : {'cpu_processes' : 1,
                                  'cpu_threads'   : threads,
                                  'gpu_processes' : 0,
                                  'gpu_threads'   : 0,
                                  'executable'    : '/bin/sh',
                                  'arguments'     : [bs_name, 'services']},
            'slots': {'ranks'  : [{'node_name'    : nodes[0]['node_name'],
                                   'node_id'      : nodes[0]['node_id'],
                                   'core_map'     : [[0]],
                                   'gpu_map'      : [],
                                   'lfs'          : 0,
                                   'mem'          : 0}]}
        }

        launcher = self._rm.find_launcher(service_task)
        if not launcher:
            raise RuntimeError('no launch method found for sub agent')

        tmp  = '#!/bin/sh\n\n'
        cmds = launcher.get_launcher_env()
        for cmd in cmds:
            tmp += '%s || exit 1\n' % cmd
        cmds = launcher.get_launch_cmd(service_task, ex_name)
        tmp += '%s\nexit $?\n\n' % '\n'.join(cmds)

        with ru.ru_open(ls_name, 'w') as fout:
            fout.write(tmp)

        tmp  = '#!/bin/sh\n\n'
        tmp += '. ./env/service.env\n'
        tmp += '/bin/sh -l ./services\n\n'

        with ru.ru_open(ex_name, 'w') as fout:
            fout.write(tmp)


        # make sure scripts are executable
        st = os.stat(ls_name)
        st = os.stat(ex_name)
        os.chmod(ls_name, st.st_mode | stat.S_IEXEC)
        os.chmod(ex_name, st.st_mode | stat.S_IEXEC)

        # spawn the sub-agent
        cmdline = './%s' % ls_name

        self._log.info ('create services: %s' % cmdline)
        ru.sh_callout_bg(cmdline, stdout='services.out', stderr='services.err')

        self._log.debug('services started done')


    # --------------------------------------------------------------------------
    #
    def _start_sub_agents(self):
        '''
        For the list of sub_agents, get a launch command and launch that
        agent instance on the respective node.  We pass it to the seconds
        bootstrap level, there is no need to pass the first one again.
        '''

        # FIXME: reroute to agent daemonizer

        if not self._cfg.get('agents'):
            return

        assert (len(self._rm.info.agent_node_list) >= len(self._cfg['agents']))

        self._log.debug('start_sub_agents')

        # store the current environment as the sub-agents will use the same
        ru.env_prep(os.environ, script_path='./env/agent.env')

        # the configs are written, and the sub-agents can be started.  To know
        # how to do that we create the agent launch method, have it creating
        # the respective command lines per agent instance, and run via
        # popen.
        #

        threads = self._rm.info.cores_per_node * \
                  self._rm.info.threads_per_core

        for idx, sa in enumerate(self._cfg['agents']):

            target = self._cfg['agents'][sa]['target']

            if target not in ['local', 'node']:

                raise ValueError('agent target unknown (%s)' % target)

            if target == 'local':

                # start agent locally
                cmdline = '/bin/sh -l %s/bootstrap_2.sh %s' % (self._pwd, sa)


            else:  # target == 'node':

                node = self._rm.info.agent_node_list[idx]
                # start agent remotely, use launch method
                # NOTE:  there is some implicit assumption that we can use
                #        the 'agent_node' string as 'agent_string:0' and
                #        obtain a well format slot...
                # FIXME: it is actually tricky to translate the agent_node
                #        into a viable 'slots' structure, as that is
                #        usually done by the schedulers.  So we leave that
                #        out for the moment, which will make this unable to
                #        work with a number of launch methods.  Can the
                #        offset computation be moved to the ResourceManager?
                bs_name       = '%s/bootstrap_2.sh' % (self._pwd)
                launch_script = '%s/%s.launch.sh'   % (self._pwd, sa)
                exec_script   = '%s/%s.exec.sh'     % (self._pwd, sa)

                agent_task = {
                    'uid'              : sa,
                    'task_sandbox_path': self._pwd,
                    'description'      : {'cpu_processes' : 1,
                                          'cpu_threads'   : threads,
                                          'gpu_processes' : 0,
                                          'gpu_threads'   : 0,
                                          'executable'    : '/bin/sh',
                                          'arguments'     : [bs_name, sa]},
                    'slots': {'ranks'  : [{'node_name'    : node['node_name'],
                                           'node_id'      : node['node_id'],
                                           'core_map'     : [[0]],
                                           'gpu_map'      : [],
                                           'lfs'          : 0,
                                           'mem'          : 0}]}
                }

                # find a launcher to use
                launcher = self._rm.find_launcher(agent_task)
                if not launcher:
                    raise RuntimeError('no launch method found for sub agent')


                tmp  = '#!/bin/sh\n\n'
                cmds = launcher.get_launcher_env()
                for cmd in cmds:
                    tmp += '%s || exit 1\n' % cmd
                cmds = launcher.get_launch_cmds(agent_task, exec_script)
                tmp += '%s\nexit $?\n\n' % '\n'.join(cmds)

                with ru.ru_open(launch_script, 'w') as fout:
                    fout.write(tmp)


                tmp  = '#!/bin/sh\n\n'
                tmp += '. ./env/agent.env\n'
                tmp += '/bin/sh -l ./bootstrap_2.sh %s\n\n' % sa

                with ru.ru_open(exec_script, 'w') as fout:
                    fout.write(tmp)

                # make sure scripts are executable
                st = os.stat(launch_script)
                st = os.stat(exec_script)
                os.chmod(launch_script, st.st_mode | stat.S_IEXEC)
                os.chmod(exec_script,   st.st_mode | stat.S_IEXEC)

                # spawn the sub-agent
                cmdline = launch_script

            self._log.info ('create sub-agent %s: %s' % (sa, cmdline))
            ru.sh_callout_bg(cmdline, stdout='%s.out' % sa,
                                      stderr='%s.err' % sa)

            # FIXME: register heartbeats?

        self._log.debug('start_sub_agents done')


    # --------------------------------------------------------------------------
    #
    def _agent_command_cb(self):

        if not self._check_commands(): return False
        if not self._check_rpc     (): return False
        if not self._check_state   (): return False

        return True


    # --------------------------------------------------------------------------
    #
    def _check_commands(self):

        # Check if there's a command waiting
        # FIXME: this pull should be done by the update worker, and commands
        #        should then be communicated over the command pubsub
        # FIXME: commands go to pmgr, tmgr, session docs
        # FIXME: check if pull/wipe are atomic
        # FIXME: long runnign commands can time out on hb
        retdoc = self._dbs._c.find_and_modify(
                    query ={'uid' : self._pid},
                    fields=['cmds'],                    # get  new commands
                    update={'$set': {'cmds': list()}})  # wipe old commands

        if not retdoc:
            return True

        for spec in retdoc.get('cmds', []):

            cmd = spec['cmd']
            arg = spec['arg']

            self._log.debug('pilot command: %s: %s', cmd, arg)
            self._prof.prof('cmd', msg="%s : %s" %  (cmd, arg), uid=self._pid)

            if cmd == 'heartbeat' and arg['pmgr'] == self._pmgr:
                self._hb.beat(uid=self._pmgr)

            elif cmd == 'cancel_pilot':
                self._log.info('cancel pilot cmd')
                self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'terminate',
                                                  'arg' : None})
                self._final_cause = 'cancel'
                self.stop()

                return False  # we are done

            elif cmd == 'cancel_tasks':
                self._log.info('cancel_tasks cmd')
                self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_tasks',
                                                  'arg' : arg})
            else:
                self._log.warn('could not interpret cmd "%s" - ignore', cmd)

        return True


    # --------------------------------------------------------------------------
    #
    def _check_rpc(self):
        '''
        check if the DB has any RPC request for this pilot.  If so, then forward
        that request as `rpc_req` command on the CONTROL channel, and listen for
        an `rpc_res` command on the same channel, for the same rpc id.  Once
        that response is received (from whatever component handled that
        command), send the response back to the databse for the callee to pick
        up.
        '''

        # FIXME: implement a timeout, and/or a registry of rpc clients

        retdoc = self._dbs._c.find_and_modify(
                    query ={'uid' : self._pid},
                    fields=['rpc_req'],
                    update={'$set': {'rpc_req': None}})

        if not retdoc:
            # no rpc request found
            return True

        rpc_req = retdoc.get('rpc_req')
        if rpc_req is None:
            # document has no rpc request
            return True

        self._log.debug('rpc req: %s', rpc_req)

        # RPCs are synchronous right now - we send the RPC on the command
        # channel, hope that some component picks it up and replies, and then
        # return that reply.  The reply is received via a temporary callback
        # defined here, which will receive all CONTROL messages until the right
        # rpc response comes along.
        def rpc_cb(topic, msg):

            rpc_id  = rpc_req['uid']

            cmd     = msg['cmd']
            rpc_res = msg['arg']

            if cmd != 'rpc_res':
                # not an rpc responese
                return True

            if rpc_res['uid'] != rpc_id:
                # not the right rpc response
                return True

            # send the response to the DB
            self._dbs._c.update({'type'  : 'pilot',
                                 'uid'   : self._pid},
                                {'$set'  : {'rpc_res': rpc_res}})

            # work is done - unregister this temporary cb (rpc_cb)
            return False


        self.register_subscriber(rpc.CONTROL_PUBSUB, rpc_cb)

        # ready to receive and proxy rpc response -- forward rpc request on
        # control channel
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'rpc_req',
                                          'arg' :  rpc_req})

        return True  # keeb cb registered (self._check_rpc)


    # --------------------------------------------------------------------------
    #
    def _check_control(self, _, msg):
        '''
        Check for commands on the control pubsub, mainly waiting for RPC
        requests to handle.  We handle two types of RPC requests: `hello` for
        testing, and `prepare_env` for environment preparation requests.
        '''

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd != 'rpc_req':
            # not an rpc request
            return True

        req = arg['rpc']
        if req not in ['hello', 'prepare_env']:
            # we don't handle that request
            return True

        ret     = None
        rpc_res = {'uid': arg['uid']}
        try:
            if req == 'hello'   :
                ret = 'hello %s' % ' '.join(arg['arg'])

            elif req == 'prepare_env':
                env_name = arg['arg']['env_name']
                env_spec = arg['arg']['env_spec']
                ret      = self._prepare_env(env_name, env_spec)

        except Exception as e:
            # request failed for some reason - indicate error
            rpc_res['err'] = repr(e)
            rpc_res['ret'] = None
            self._log.exception('control cmd failed')

        else:
            # request succeeded - respond with return value
            rpc_res['err'] = None
            rpc_res['ret'] = ret

        # publish the response (success or failure)
        self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'rpc_res',
                                          'arg':  rpc_res})
        return True


    # --------------------------------------------------------------------------
    #
    def _check_state(self):

        # Make sure that we haven't exceeded the runtime - otherwise terminate.
        if self._cfg.runtime:

            if time.time() >= self._starttime +  (int(self._cfg.runtime) * 60):

                self._log.info('runtime limit (%ss).', self._cfg.runtime * 60)
                self._final_cause = 'timeout'
                self.stop()
                return False  # we are done

        return True


    # --------------------------------------------------------------------------
    #
    def _check_tasks_cb(self):

        # Check for tasks waiting for input staging and log pull.
        #
        # FIXME: Unfortunately, 'find_and_modify' is not bulkable, so we have
        #        to use 'find'.  To avoid finding the same tasks over and over
        #        again, we update the 'control' field *before* running the next
        #        find -- so we do it right here.
        #        This also blocks us from using multiple ingest threads, or from
        #        doing late binding by task pull :/
        task_cursor = self._dbs._c.find({'type'    : 'task',
                                         'pilot'   : self._pid,
                                         'control' : 'agent_pending'})
        if not task_cursor.count():
            self._log.info('tasks pulled:    0')
            return True

        # update the tasks to avoid pulling them again next time.
        task_list = list(task_cursor)
        task_uids = [task['uid'] for task in task_list]

        self._dbs._c.update({'type'  : 'task',
                             'uid'   : {'$in'     : task_uids}},
                            {'$set'  : {'control' : 'agent'}},
                            multi=True)

        self._log.info("tasks pulled: %4d", len(task_list))
        self._prof.prof('get', msg='bulk: %d' % len(task_list), uid=self._pid)

        for task in task_list:

            # make sure the tasks obtain env settings (if needed)
            if 'task_environment' in self._cfg:
                if not task['description'].get('environment'):
                    task['description']['environment'] = dict()
                for k,v in self._cfg['task_environment'].items():
                    task['description']['environment'][k] = v

            # we need to make sure to have the correct state:
            task['state'] = rps._task_state_collapse(task['states'])
            self._prof.prof('get', uid=task['uid'])

            # FIXME: raise or fail task!
            if task['state'] != rps.AGENT_STAGING_INPUT_PENDING:
                self._log.error('invalid state: %s', (pprint.pformat(task)))

            task['control'] = 'agent'

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).  We don't publish nor profile as advance,
        # since that happened already on the module side when the state was set.
        self.advance(task_list, publish=False, push=True)

        return True


    # --------------------------------------------------------------------------
    #
    def _prepare_env(self, env_name, env_spec):

        etype = env_spec['type']
        evers = env_spec['version']
        emods = env_spec.get('setup')    or []
        pre   = env_spec.get('pre_exec') or []

        pre_exec = '-P ". env/bs0_pre_0.sh" '
        for cmd in pre:
            pre_exec += '-P "%s" ' % cmd

        if emods: mods = '-m "%s"' % ','.join(emods)
        else    : mods = ''

        assert(etype == 'virtualenv')
        assert(evers)

        rp_cse = ru.which('radical-pilot-create-static-ve')
        ve_cmd = '/bin/bash %s -d -p %s/env/rp_named_env.%s -v %s %s %s ' \
                 '| tee -a env.log 2>&1' \
               % (rp_cse, self._pwd, env_name, evers, mods, pre_exec)

        self._log.debug('env cmd: %s', ve_cmd)
        out, err, ret = ru.sh_callout(ve_cmd, shell=True)
        self._log.debug('    out: %s', out)
        self._log.debug('    err: %s', err)

        if ret:
            raise RuntimeError('prepare_env failed: \n%s\n%s\n' % (out, err))

        # prepare the env to be loaded in task exec scripts
        ve_path = '%s/env/rp_named_env.%s' % (self._pwd, env_name)
        with ru.ru_open('%s.sh' % ve_path, 'w') as fout:
            fout.write('\n. %s/bin/activate\n\n' % ve_path)

        # publish the venv creation to the scheduler
        self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'register_named_env',
                                          'arg': {'env_name': env_name}})
        return out


# ------------------------------------------------------------------------------


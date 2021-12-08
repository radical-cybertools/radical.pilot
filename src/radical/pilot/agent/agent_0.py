
__copyright__ = 'Copyright 2014-2016, http://radical.rutgers.edu'
__license__   = 'MIT'

import copy
import os
import pprint
import stat
import time
import pprint
import threading           as mt
import subprocess          as sp
import multiprocessing     as mp

import radical.utils       as ru

from ..   import utils     as rpu
from ..   import states    as rps
from ..   import constants as rpc

from .resource_manager import ResourceManager


# ------------------------------------------------------------------------------
#
class Agent_0(rpu.Worker):

    '''
    This is the main agent.  It starts sub-agents and watches them.  If any of
    the sub-agents die, it will shut down the other sub-agents and itself.

    This class inherits the rpu.Worker, so that it can use its communication
    bridges and callback mechanisms.  Specifically, it will pull the tasks from
    the proxy comm channels and forwards them to the agent's component network
    (see `work()`).  It will also watch the proxy pubsub for any commands to be
    enacted or forwarded (pilot termination, task cancelation, etc), and will
    take care of heartbeat messages to be sent to the client.
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
        self._sid     = self._session.uid
        self._log     = session._log

        self._starttime   = time.time()
        self._final_cause = None

        # pick up proxy config from session
        self._cfg.proxy = self._session._cfg.proxy

        rpu.Worker.__init__(self, self._cfg, session)

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

        # connect to proxy communication channels, maybe
        self._connect_proxy()

        # start any services if they are requested
        self._start_services()

        # create the sub-agent configs and start the sub agents
        self._write_sa_configs()
        self._start_sub_agents()   # TODO: move to cmgr?

        # handle control messages
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._check_control)

        # run our own slow-paced heartbeat monitor to watch pmgr heartbeats
        # FIXME: we need to get pmgr freq
        freq = 100
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

        # register the control callback
        self.register_subscriber(rpc.PROXY_CONTROL_PUBSUB,
                                 self._proxy_control_cb)

        # proxy state updates
        self.register_publisher(rpc.PROXY_STATE_PUBSUB)
        self.register_subscriber(rpc.STATE_PUBSUB, self._proxy_state_cb)

        # regularly check for lifetime limit
        self.register_timed_cb(self._check_lifetime, timer=10)

        # as long as we are alive, we also want to keep the proxy alive
        self._session._run_proxy_hb()



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
    def _connect_proxy(self):

        # write config files for proxy channels
        for p in self._cfg.proxy:
            ru.write_json('%s.cfg' % p, self._cfg.proxy[p])

        # listen for new tasks from the client
        self.register_input(rps.AGENT_STAGING_INPUT_PENDING,
                            rpc.PROXY_TASK_QUEUE,
                            qname=self._pid,
                            cb=self._proxy_input_cb)

        # and forward to agent input staging
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        # listen for completed tasks to foward to client
        self.register_input(rps.TMGR_STAGING_OUTPUT_PENDING,
                            rpc.AGENT_COLLECTING_QUEUE,
                            qname='default',
                            cb=self._proxy_output_cb)

        # and register output
        self.register_output(rps.TMGR_STAGING_OUTPUT_PENDING,
                             rpc.PROXY_TASK_QUEUE)

        # FIXME: register pubsubs


    # --------------------------------------------------------------------------
    #
    def _proxy_input_cb(self, msg):

        self._log.debug('proxy input cb: %s', len(msg))

        to_advance = list()

        for task in msg:

            # make sure the tasks obtain env settings (if needed)
            if 'task_environment' in self._cfg:

                if not task['description'].get('environment'):
                    task['description']['environment'] = dict()

                for k,v in self._cfg['task_environment'].items():
                    # FIXME: this might overwrite user specified env
                    task['description']['environment'][k] = v

            # FIXME: raise or fail task!
            if task['state'] != rps.AGENT_STAGING_INPUT_PENDING:
                self._log.error('invalid state: %s:%s:%s', task['uid'],
                        task['state'], task.get('states'))
                continue

            to_advance.append(task)

        # now we really own the tasks and can start working on them (ie. push
        # them into the pipeline).  We don't publish nor profile as advance,
        # since the state transition happened already on the client side when
        # the state was set.
        self.advance(to_advance, publish=False, push=True)


    # --------------------------------------------------------------------------
    #
    def _proxy_output_cb(self, msg):

        # we just forward the tasks to the task proxy queue
        self._log.debug('=== proxy output cb: %s', len(msg))
        self.advance(msg, publish=False, push=True, qname=self._sid)


    # --------------------------------------------------------------------------
    #
    def _client_ctrl_cb(self, topic, msg):

        self._log.debug('ctl sub cb: %s %s', topic, msg)


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

        # sub-agents are started, components are started, bridges are up: we are
        # ready to roll!  Send state update
        rm_info = self._rm.info
        n_nodes = len(rm_info['node_list'])

        pilot = {'type'     : 'pilot',
                 'uid'      : self._pid,
                 'state'    : rps.PMGR_ACTIVE,
                 'resources': {'rm_info': rm_info,
                               'cpu'    : rm_info['cores_per_node'] * n_nodes,
                               'gpu'    : rm_info['gpus_per_node']  * n_nodes}}

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

        self._log.info('rusage: %s', rpu.get_rusage())

        out, err, log = '', '', ''

        try   : out = open('./agent.0.out', 'r').read(1024)
        except: pass
        try   : err = open('./agent.0.err', 'r').read(1024)
        except: pass
        try   : log = open('./agent.0.log', 'r').read(1024)
        except: pass

        self._reg_service.stop()

        if   self._final_cause == 'timeout'  : state = rps.DONE
        elif self._final_cause == 'cancel'   : state = rps.CANCELED
        elif self._final_cause == 'sys.exit' : state = rps.CANCELED
        else                                 : state = rps.FAILED

        # NOTE: we do not push the final pilot state, as that is done by the
        #       bootstrapper *after* this pilot *actually* finished.
        with ru.ru_open('./killme.signal', 'w') as fout:
            fout.write('%s\n' % state)

        pilot = {'type'   : 'pilot',
                 'uid'    : self._pid,
                 'stdout' : out,
                 'stderr' : err,
                 'logfile': log,
                 'state'  : state}

        self._log.debug('push final state update')
        self._log.debug('update state: %s: %s', state, self._final_cause)
        self.publish(rpc.PROXY_STATE_PUBSUB,
                     topic=rpc.STATE_PUBSUB, msg=[pilot])


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

            target  = self._cfg['agents'][sa]['target']
            cmdline = None

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
    def _check_lifetime(self):

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
    def _proxy_state_cb(self, topic, msg):

        # no need to check - blindly forward all messages to the proxy
        self.publish(rpc.PROXY_STATE_PUBSUB, topic=topic, msg=msg)


    # --------------------------------------------------------------------------
    #
    def _proxy_control_cb(self, topic, msg):

        self._log.debug('proxy control: %s', msg)

        cmd = msg['cmd']
        arg = msg['arg']

        self._log.debug('pilot command: %s: %s', cmd, arg)
        self._prof.prof('cmd', msg="%s : %s" %  (cmd, arg), uid=self._pid)


        if cmd == 'pmgr_heartbeat' and arg['pmgr'] == self._pmgr:

            self._hb.beat(uid=self._pmgr)
            return True


        if cmd == 'prep_env':

            env_spec = arg
            for env_id in env_spec:
                # ensure we have a hb period
                self._hb.beat(uid=self._pmgr)
                self._prepare_env(env_id, env_spec[env_id])
            return True


        if cmd == 'cancel_pilots':

            if self._pid not in arg.get('uids'):
                self._log.debug('ignore cancel %s', msg)
                return True

            self._log.info('cancel pilot cmd')
            self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'terminate',
                                              'arg' : None})
            self._final_cause = 'cancel'
            self.stop()

            return False  # we are done


        # all other messages (such as cancel_tasks) are forwarded to the agent
        # control pubsub, to be picked up by the respective target components
        self._log.debug('fwd control msg %s', msg)
        self.publish(rpc.CONTROL_PUBSUB, msg)

        return True


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
            ret = None

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


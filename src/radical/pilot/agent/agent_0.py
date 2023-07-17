
__copyright__ = 'Copyright 2014-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import copy
import os
import stat
import time
import pprint

import threading           as mt

import radical.utils       as ru

from ..   import utils     as rpu
from ..   import states    as rps
from ..   import constants as rpc
from ..   import TaskDescription
from ..   import Session
from ..   import TaskDescription, AGENT_SERVICE

from .resource_manager import ResourceManager


# ------------------------------------------------------------------------------
#
class Agent_0(rpu.Worker):

    '''
    This is the main agent.  It starts sub-agents and watches them.  If any of
    the sub-agents die, it will shut down the other sub-agents and itself.

    This class inherits the rpu.Worker, so that it can use its communication
    bridges and callback mechanisms.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        cfg = ru.Config(path='./agent_0.cfg')

        self._uid     = cfg.uid
        self._pid     = cfg.pid
        self._sid     = cfg.sid
        self._owner   = cfg.owner
        self._pmgr    = cfg.pmgr
        self._pwd     = cfg.pilot_sandbox

        self._session = Session(uid=cfg.sid, cfg=cfg, _role=Session._AGENT_0)
        self._rcfg    = self._session._rcfg

        # init the worker / component base classes, connects registry
        rpu.Worker.__init__(self, cfg, self._session)

        self._starttime   = time.time()
        self._final_cause = None

        # keep some state about service startups
        self._service_uids_launched = list()
        self._service_uids_running  = list()
        self._services_setup        = mt.Event()

        # this is the earliest point to sync bootstrap and agent profiles
        self._prof.prof('hostname', uid=cfg.pid, msg=ru.get_hostname())

        # configure ResourceManager before component startup, as components need
        # ResourceManager information for function (scheduler, executor)
        self._configure_rm()

        # ensure that app communication channels are visible to workload
        self._configure_app_comm()

        # start any services if they are requested
        self._start_services()

        # create the sub-agent configs and start the sub agents
        self._write_sa_configs()
        self._start_sub_agents()   # TODO: move to cmgr?

        # regularly check for lifetime limit
        self.register_timed_cb(self._check_lifetime, timer=10)


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
        self._log.debug('proxy output cb: %s', len(msg))
        self.advance(msg, publish=False, push=True, qname=self._sid)


    # --------------------------------------------------------------------------
    #
    def _client_ctrl_cb(self, topic, msg):

        self._log.debug('ctl sub cb: %s %s', topic, msg)
        ## FIXME?


    # --------------------------------------------------------------------------
    #
    def _configure_rm(self):

        # Create ResourceManager which will give us the set of agent_nodes to
        # use for sub-agent startup.  Add the remaining ResourceManager
        # information to the config, for the benefit of the scheduler).

        self._cfg.reg_addr = self._session.reg_addr
        self._rm = ResourceManager.create(name=self._rcfg.resource_manager,
                                          cfg=self._cfg, rcfg=self._rcfg,
                                          log=self._log, prof=self._prof)

        self._log.debug(pprint.pformat(self._rm.info))


    # --------------------------------------------------------------------------
    #
    def _configure_app_comm(self):

        # if the pilot description contains a request for application comm
        # channels, merge those into the agent config
        #
        # FIXME: this needs to start the app_comm bridges
        app_comm = self._rcfg.get('app_comm')
        if app_comm:

            # bridge addresses also need to be exposed to the workload
            if 'task_environment' not in self._rcfg:
                self._rcfg['task_environment'] = dict()

            if isinstance(app_comm, list):
                app_comm = {ac: {'bulk_size': 0,
                                 'stall_hwm': 1,
                                 'log_level': 'error'} for ac in app_comm}
            for ac in app_comm:

                if ac in self._reg['bridges']:
                    raise ValueError('reserved app_comm name %s' % ac)

                self._reg['bridges.%s' % ac] = app_comm[ac]

                AC = ac.upper()

                self._rcfg['task_environment']['RP_%s_IN'  % AC] = ac['addr_in']
                self._rcfg['task_environment']['RP_%s_OUT' % AC] = ac['addr_out']

        # some of the bridge addresses also need to be exposed to the workload
        if app_comm:
            if 'task_environment' not in self._cfg:
                self._cfg['task_environment'] = dict()
            for ac in app_comm:
                if ac not in self._reg['bridges']:
                    raise RuntimeError('missing app_comm %s' % ac)
                self._cfg['task_environment']['RP_%s_IN' % ac.upper()] = \
                        self._reg['bridges.%s.ac' % ac]['addr_in']
                self._cfg['task_environment']['RP_%s_OUT' % ac.upper()] = \
                        self._reg['bridges.%s.addr_out' % ac]



    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # handle pilot commands
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)

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

        # before we run any tasks, prepare a named_env `rp` for tasks which use
        # the pilot's own environment, such as raptors
        env_spec = {'type'    : os.environ['RP_VENV_TYPE'],
                    'path'    : os.environ['RP_VENV_PATH'],
                    'pre_exec': ['export PYTHONPATH=%s'
                                 %  os.environ.get('PYTHONPATH', ''),
                                 'export PATH=%s'
                                 %  os.environ.get('PATH', '')]
                   }
        self._prepare_env('rp', env_spec)

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

        self.advance(pilot, publish=True, push=False, fwd=True)


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

        self._log.info('rusage: %s', rpu.get_rusage())

        out, err, log = '', '', ''

        try   : out = open('./agent_0.out', 'r').read(1024)
        except: pass
        try   : err = open('./agent_0.err', 'r').read(1024)
        except: pass
        try   : log = open('./agent_0.log', 'r').read(1024)
        except: pass

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
                 'state'  : state,
                 'forward': True}

        self._log.debug('push final state update')
        self._log.debug('update state: %s: %s', state, self._final_cause)
        self.publish(rpc.STATE_PUBSUB,
                     topic=rpc.STATE_PUBSUB, msg=[pilot])

        # tear things down in reverse order
        self._rm.stop()
        self._session.close()


    # --------------------------------------------------------------------
    #
    def _write_sa_configs(self):

        # we have all information needed by the subagents -- write the
        # sub-agent config files.

        # write deep-copies of the config for each sub-agent (sans from agent_0)
        for sa in self._rcfg.get('agents', {}):

            assert (sa != 'agent_0'), 'expect subagent, not agent_0'

            # use our own config sans agents/components/bridges as a basis for
            # the sub-agent config.
            tmp_cfg = copy.deepcopy(self._session._cfg)
            tmp_cfg['agents']     = dict()
            tmp_cfg['components'] = dict()
            tmp_cfg['bridges']    = dict()

            # merge sub_agent layout into the config
            ru.dict_merge(tmp_cfg, self._cfg['agents'][sa], ru.OVERWRITE)

            tmp_cfg['uid']   = sa
            tmp_cfg['aid']   = sa
            tmp_cfg['owner'] = 'agent_0'


    # --------------------------------------------------------------------------
    #
    def _start_services(self):

        service_descriptions = self._cfg.services
        if not service_descriptions:
            return
        self._log.info('starting agent services')

        services = list()
        for service_desc in service_descriptions:

            td      = TaskDescription(service_desc)
            td.mode = AGENT_SERVICE
            # ensure that the description is viable
            td.verify()

            cfg = self._cfg
            tid = ru.generate_id('service.%(item_counter)04d',
                                 ru.ID_CUSTOM, ns=self._cfg.sid)
            task = dict()
            task['origin']            = 'agent'
            task['description']       = td.as_dict()
            task['state']             = rps.AGENT_STAGING_INPUT_PENDING
            task['status']            = 'NEW'
            task['type']              = 'service_task'
            task['uid']               = tid
            task['pilot_sandbox']     = cfg.pilot_sandbox
            task['task_sandbox']      = cfg.pilot_sandbox + task['uid'] + '/'
            task['task_sandbox_path'] = cfg.pilot_sandbox + task['uid'] + '/'
            task['session_sandbox']   = cfg.session_sandbox
            task['resource_sandbox']  = cfg.resource_sandbox
            task['pilot']             = cfg.pid
            task['resources']         = {'cpu': td.ranks * td.cores_per_rank,
                                         'gpu': td.ranks * td.gpus_per_rank}

            self._service_uids_launched.append(tid)
            services.append(task)

        self.advance(services, publish=False, push=True)

        # Waiting 2mins for all services to launch
        if not self._services_setup.wait(timeout=60 * 2):
            raise RuntimeError('Unable to start services')

        self._log.info('all agent services started')


    # --------------------------------------------------------------------------
    #
    def _service_state_cb(self, topic, msg):  # pylint: disable=unused-argument

        cmd   = msg['cmd']
        tasks = msg['arg']

        if cmd != 'update':
            return

        for service in ru.as_list(tasks):

            if service['uid'] not in self._service_uids_launched or \
                    service['uid'] in self._service_uids_running:
                continue

            self._log.debug('service state update %s: %s',
                            service['uid'], service['state'])
            if service['state'] != rps.AGENT_EXECUTING:
                continue

            self._service_uids_running.append(service['uid'])
            self._log.debug('service %s started (%s / %s)', service['uid'],
                            len(self._service_uids_running),
                            len(self._service_uids_launched))

            if len(self._service_uids_launched) == \
                    len(self._service_uids_running):
                self._services_setup.set()


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
                    'uid'               : sa,
                    'task_sandbox_path' : self._pwd,
                    'description'       : TaskDescription({
                        'uid'           : sa,
                        'ranks'         : 1,
                        'cores_per_rank': self._rm.info.cores_per_node,
                        'executable'    : '/bin/sh',
                        'arguments'     : [bs_name, sa]
                    }).as_dict(),
                    'slots': {'ranks'   : [{'node_name': node['node_name'],
                                            'node_id'  : node['node_id'],
                                            'core_map' : [[0]],
                                            'gpu_map'  : [],
                                            'lfs'      : 0,
                                            'mem'      : 0}]}
                }

                # find a launcher to use
                launcher = self._rm.find_launcher(agent_task)
                if not launcher:
                    raise RuntimeError('no launch method found for sub agent')

                # FIXME: set RP environment (as in Popen Executor)

                tmp  = '#!/bin/sh\n\n'
                tmp += 'export RP_PILOT_SANDBOX="%s"\n\n' % self._pwd
                cmds = launcher.get_launcher_env()
                for cmd in cmds:
                    tmp += '%s || exit 1\n' % cmd

                cmds = launcher.get_launch_cmds(agent_task, exec_script)
                tmp += '%s\nexit $?\n\n' % cmds

                with ru.ru_open(launch_script, 'w') as fout:
                    fout.write(tmp)


                tmp  = '#!/bin/sh\n\n'
                tmp += '. ./env/agent.env\n'
                tmp += '/bin/sh -l ./bootstrap_2.sh %s\n\n' % sa

                with ru.ru_open(exec_script, 'w') as fout:
                    fout.write(tmp)

                # make sure scripts are executable
                st_l = os.stat(launch_script)
                st_e = os.stat(exec_script)
                os.chmod(launch_script, st_l.st_mode | stat.S_IEXEC)
                os.chmod(exec_script,   st_e.st_mode | stat.S_IEXEC)

                # spawn the sub-agent
                cmdline = launch_script

            self._log.info ('create sub-agent %s: %s', sa, cmdline)
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
    def _control_cb(self, _, msg):
        '''
        Check for commands on the control pubsub, mainly waiting for RPC
        requests to handle.
        '''

        self._log.debug('control: %s', msg)

        cmd = msg['cmd']
        arg = msg['arg']

        self._log.debug('pilot command: %s: %s', cmd, arg)
        self._prof.prof('cmd', msg="%s : %s" %  (cmd, arg), uid=self._pid)


        if cmd == 'pmgr_heartbeat' and arg['pmgr'] == self._pmgr:

            self._session._hb.beat(uid=self._pmgr)
            return True


        elif cmd == 'prep_env':

            env_spec = arg
            for env_id in env_spec:
                self._prepare_env(env_id, env_spec[env_id])
            return True


        elif cmd == 'cancel_pilots':

            if self._pid not in arg.get('uids'):
                self._log.debug('ignore cancel %s', msg)

            self._log.info('cancel pilot cmd')
            self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'terminate',
                                              'arg' : None})
            self._final_cause = 'cancel'
            self.stop()

            # work is done - unregister this cb
            return False


        elif cmd == 'rpc_req':

            req = arg['rpc']
            if req not in ['hello', 'prepare_env']:

                # we don't handle that request
                return True

            rpc_res = {'uid': arg['uid']}

            try:
                if req == 'hello'   :
                    out = 'hello %s' % ' '.join(arg['arg'])

                elif req == 'prepare_env':
                    env_name = arg['arg']['env_name']
                    env_spec = arg['arg']['env_spec']
                    out      = self._prepare_env(env_name, env_spec)

                else:
                    # unknown command
                    self._log.info('ignore rpc command: %s', req)
                    return True

                # request succeeded - respond with return value
                rpc_res['err'] = None
                rpc_res['out'] = out
                rpc_res['ret'] = 0

            except Exception as e:
                # request failed for some reason - indicate error
                rpc_res['err'] = repr(e)
                rpc_res['out'] = None
                rpc_res['ret'] = 1
                self._log.exception('control cmd failed')

            # publish the response (success or failure)
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'rpc_res',
                                              'arg':  rpc_res})

        return True


    # --------------------------------------------------------------------------
    #
    def _prepare_env(self, env_name, env_spec):

        self._log.debug('env_spec: %s', env_spec)

        etype = env_spec.get('type', 'venv')
        evers = env_spec.get('version')
        path  = env_spec.get('path')
        emods = env_spec.get('setup')    or []
        pre   = env_spec.get('pre_exec') or []
        out   = None

        pre_exec = '-P ". env/bs0_pre_0.sh" '
        for cmd in pre:
            pre_exec += '-P "%s" ' % cmd

        if emods: mods = '-m "%s"' % ','.join(emods)
        else    : mods = ''

      # assert etype == 'virtualenv'
      # assert evers

        # only create a new VE if path is not set or if it does not exist
        if path:
            path = path.rstrip('/')

        ve_local_path = '%s/env/rp_named_env.%s' % (self._pwd, env_name)
        if path: ve_path = path
        else   : ve_path = ve_local_path

        if evers:
            evers = '-v %s' % evers
        else:
            evers = ''

        rp_cse = ru.which('radical-pilot-create-static-ve')
        ve_cmd = '/bin/bash %s -d -p %s -t %s ' % (rp_cse, ve_path, etype) + \
                 '%s %s %s '                    % (evers, mods, pre_exec)  + \
                 '-T %s.env > env.log 2>&1'     % ve_local_path

        # FIXME: we should export all sandboxes etc. to the prep_env.
        os.environ['RP_RESOURCE_SANDBOX'] = '../../'

        self._log.debug('env cmd: %s', ve_cmd)
        out, err, ret = ru.sh_callout(ve_cmd, shell=True)
        self._log.debug('    out: %s', out)
        self._log.debug('    err: %s', err)

        if ret:
            raise RuntimeError('prepare_env failed: \n%s\n%s\n' % (out, err))

        # if the ve lives outside of the pilot sandbox, link it
        if path:
            os.symlink(path, ve_local_path)

        self._log.debug('ve_path: %s', ve_path)

        # prepare the env to be loaded in task exec scripts
        with ru.ru_open('%s.sh' % ve_local_path, 'w') as fout:
            fout.write('\n. %s/bin/activate\n\n' % ve_path)

        # publish the venv creation to the scheduler
        self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'register_named_env',
                                          'arg': {'env_name': env_name}})
        return out


# ------------------------------------------------------------------------------


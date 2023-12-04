
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
from ..   import Session
from ..   import TaskDescription, AGENT_SERVICE


# ------------------------------------------------------------------------------
#
class Agent_0(rpu.AgentComponent):

    '''
    This is the main agent.  It starts sub-agents and watches them.  If any of
    the sub-agents die, it will shut down the other sub-agents and itself.

    This class inherits the rpu.AgentComponent, so that it can use its
    communication bridges and callback mechanisms.
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

        self._rm      = self._session.get_rm()

        # init the worker / component base classes, connects registry
        super().__init__(cfg, self._session)

        self._starttime   = time.time()
        self._final_cause = None

        # keep some state about service startups
        self._service_uids_launched = list()
        self._service_uids_running  = list()
        self._services_setup        = mt.Event()

        # this is the earliest point to sync bootstrap and agent profiles
        self._prof.prof('hostname', uid=cfg.pid, msg=ru.get_hostname())

        # ensure that app communication channels are visible to workload
        self._configure_app_comm()

        # start the sub agents
        self._start_sub_agents()

        # regularly check for lifetime limit
        self.register_timed_cb(self._check_lifetime, timer=10)


    # --------------------------------------------------------------------------
    #
    def _proxy_input_cb(self, msg):

        self._log.debug_8('proxy input cb: %s', len(msg))

        to_advance = list()

        for task in msg:

            # make sure the tasks obtain env settings (if needed)
            if 'task_environment' in self.session.rcfg:

                if not task['description'].get('environment'):
                    task['description']['environment'] = dict()

                for k,v in self.session.rcfg.task_environment.items():
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
    def _configure_app_comm(self):

        # if the pilot description contains a request for application comm
        # channels, merge those into the agent config
        #
        # FIXME: this needs to start the app_comm bridges
        app_comm = self.session.rcfg.get('app_comm')
        if app_comm:

            # bridge addresses also need to be exposed to the workload
            if 'task_environment' not in self.session.rcfg:
                self.session.rcfg['task_environment'] = dict()

            if isinstance(app_comm, list):
                app_comm = {ac: {'bulk_size': 0,
                                 'stall_hwm': 1,
                                 'log_level': 'error'} for ac in app_comm}
            for ac in app_comm:

                if ac in self._reg['bridges']:
                    raise ValueError('reserved app_comm name %s' % ac)

                self._reg['bridges.%s' % ac] = app_comm[ac]

                AC = ac.upper()

                self.session.rcfg.task_environment['RP_%s_IN'  % AC] = ac['addr_in']
                self.session.rcfg.task_environment['RP_%s_OUT' % AC] = ac['addr_out']


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # listen for new tasks from the client
        self.register_input(rps.AGENT_STAGING_INPUT_PENDING,
                            rpc.PROXY_TASK_QUEUE,
                            qname=self._pid,
                            cb=self._proxy_input_cb)

        # and forward to agent input staging
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        # listen for completed tasks to forward to client
        self.register_input(rps.TMGR_STAGING_OUTPUT_PENDING,
                            rpc.AGENT_COLLECTING_QUEUE,
                            cb=self._proxy_output_cb)

        # and register output
        self.register_output(rps.TMGR_STAGING_OUTPUT_PENDING,
                             rpc.PROXY_TASK_QUEUE)

        self.register_rpc_handler('prepare_env', self._prepare_env,
                                                 rpc_addr=self._pid)

        # before we run any tasks, prepare a named_env `rp` for tasks which use
        # the pilot's own environment, such as raptors
        env_spec = {'type'    : os.environ['RP_VENV_TYPE'],
                    'path'    : os.environ['RP_VENV_PATH'],
                    'pre_exec': ['export PYTHONPATH=%s'
                                 %  os.environ.get('PYTHONPATH', ''),
                                 'export PATH=%s'
                                 %  os.environ.get('PATH', '')]
                   }
        self.rpc('prepare_env', env_name='rp', env_spec=env_spec,
                                rpc_addr=self._pid)

        # start any services if they are requested
        self._start_services()

        # sub-agents are started, components are started, bridges are up: we are
        # ready to roll!  Send state update
        rm_info = self._rm.info
        n_nodes = len(rm_info['node_list'])

        self._log.debug('advance to PMGR_ACTIVE')

        pilot = {'$all'     : True,              # pass full info to client side
                 'type'     : 'pilot',
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
                 'state'  : state}

        self._log.debug('push final state update')
        self._log.debug('update state: %s: %s', state, self._final_cause)
        self.advance(pilot, publish=True, push=False)

        # tear things down in reverse order
        self._rm.stop()
        self._session.close()


    # --------------------------------------------------------------------------
    #
    def _start_services(self):

        if not self.session.cfg.services:
            return

        self._log.info('starting agent services')

        services      = []
        services_data = {}

        for sd in self.session.cfg.services:

            td      = TaskDescription(sd)
            td.mode = AGENT_SERVICE
            # ensure that the description is viable
            td.verify()

            tid = ru.generate_id('service.%(item_counter)04d',
                                 ru.ID_CUSTOM, ns=self.session.uid)
            task = dict()
            task['uid']               = tid
            task['type']              = 'service_task'
            task['origin']            = 'agent'
            task['pilot']             = self.session.cfg.pid
            task['description']       = td.as_dict()
            task['state']             = rps.AGENT_STAGING_INPUT_PENDING
            task['pilot_sandbox']     = self.session.cfg.pilot_sandbox
            task['session_sandbox']   = self.session.cfg.session_sandbox
            task['resource_sandbox']  = self.session.cfg.resource_sandbox
            task['resources']         = {'cpu': td.ranks * td.cores_per_rank,
                                         'gpu': td.ranks * td.gpus_per_rank}

            task_sandbox = self.session.cfg.pilot_sandbox + tid + '/'
            task['task_sandbox']      = task_sandbox
            task['task_sandbox_path'] = task_sandbox

            # TODO: use `type='service_task'` in RADICAL-Analytics

            # TaskDescription.metadata will contain service related data:
            # "name" (unique), "startup_file"

            self._service_uids_launched.append(tid)
            services.append(task)

            services_data[tid] = {}
            if td.metadata.get('startup_file'):
                n = td.metadata.get('name')
                services_data[tid]['name'] = 'service.%s' % n if n else tid
                services_data[tid]['startup_file'] = td.metadata['startup_file']

        self.advance(services, publish=False, push=True)

        self.register_timed_cb(cb=self._services_startup_cb,
                               cb_data=services_data,
                               timer=2)

        # waiting for all services to start (max waiting time 2 mins)
        if not self._services_setup.wait(timeout=120):
            raise RuntimeError('Unable to start services')

        self.unregister_timed_cb(self._services_startup_cb)

        self._log.info('all agent services started')

    def _services_startup_cb(self, cb_data):

        for tid in list(cb_data):

            service_up   = False
            startup_file = cb_data[tid].get('startup_file')

            if not startup_file:
                service_up = True
                # FIXME: at this point we assume that since "startup_file" is
                #        not provided, then we don't wait - this will be
                #        replaced with another callback (BaseComponent.advance will
                #        publish control command "service_up" for service tasks)

            elif os.path.isfile(startup_file):
                # if file exists then service is up (general approach)
                service_up = True

                # collect data from the startup file: at this point we look
                # for URLs only
                service_urls = {}
                with ru.ru_open(startup_file, 'r') as fin:
                    for line in fin.readlines():
                        if '://' not in line:
                            continue
                        parts = line.split()
                        if len(parts) == 1:
                            idx, url = '', parts[0]
                        elif '://' in parts[1]:
                            idx, url = parts[0], parts[1]
                        else:
                            continue
                        service_urls[idx] = url

                if service_urls:
                    for idx, url in service_urls.items():
                        key = cb_data[tid]['name']
                        if idx:
                            key += '.%s' % idx
                        key += '.url'
                        self.session._reg[key] = url

            if service_up:
                self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'service_up',
                                                  'arg': {'uid': tid}})
                del cb_data[tid]

        return True

    # --------------------------------------------------------------------------
    #
    def _start_sub_agents(self):
        '''
        For the list of sub_agents, get a launch command and launch that
        agent instance on the respective node.  We pass it to the seconds
        bootstrap level, there is no need to pass the first one again.
        '''

        # FIXME: reroute to agent daemonizer

        if not self.session.cfg.get('agents'):
            return

        n_agents      = len(self.session.cfg['agents'])
        n_agent_nodes = len(self._rm.info.agent_node_list)

        assert n_agent_nodes >= n_agents


        self._log.debug('start_sub_agents')

        # store the current environment as the sub-agents will use the same
        # (it will be called within "bootstrap_2.sh")
        ru.env_prep(os.environ, script_path='./env/agent.env')

        # the configs are written, and the sub-agents can be started.  To know
        # how to do that we create the agent launch method, have it creating
        # the respective command lines per agent instance, and run via popen.

        bs_name = '%s/bootstrap_2.sh'

        for idx, sa in enumerate(self.session.cfg['agents']):

            target  = self.session.cfg['agents'][sa]['target']
            bs_args = [self._sid, self.session.cfg.reg_addr, sa]

            if target not in ['local', 'node']:

                raise ValueError('agent target unknown (%s)' % target)

            if target == 'local':

                # start agent locally
                bs_path = bs_name % self._pwd
                cmdline = '/bin/sh -l %s' % ' '.join([bs_path] + bs_args)

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

                launch_script = '%s/%s.launch.sh'   % (self._pwd, sa)
                exec_script   = '%s/%s.exec.sh'     % (self._pwd, sa)

                node_cores = [cid for cid, cstate in enumerate(node['cores'])
                              if cstate == rpc.FREE]

                agent_task = {
                    'uid'               : sa,
                    'task_sandbox_path' : self._pwd,
                    'description'       : TaskDescription({
                        'uid'           : sa,
                        'ranks'         : 1,
                        'cores_per_rank': self._rm.info.cores_per_node,
                        'executable'    : '/bin/sh',
                        'arguments'     : [bs_name % self._pwd] + bs_args
                    }).as_dict(),
                    'slots': {'ranks'   : [{'node_name': node['node_name'],
                                            'node_id'  : node['node_id'],
                                            'core_map' : [node_cores],
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
                tmp += '/bin/sh -l %s\n\n' % ' '.join([bs_name % '.'] + bs_args)
                with ru.ru_open(exec_script, 'w') as fout:
                    fout.write(tmp)

                # make sure scripts are executable
                st_l = os.stat(launch_script)
                st_e = os.stat(exec_script)
                os.chmod(launch_script, st_l.st_mode | stat.S_IEXEC)
                os.chmod(exec_script,   st_e.st_mode | stat.S_IEXEC)

                # spawn the sub-agent
                cmdline = launch_script

            self._log.info('create sub-agent %s: %s', sa, cmdline)
            ru.sh_callout_bg(cmdline, stdout='%s.out' % sa,
                                      stderr='%s.err' % sa,
                                      cwd=self._pwd)

        self._log.debug('start_sub_agents done')


    # --------------------------------------------------------------------------
    #
    def _check_lifetime(self):

        # Make sure that we haven't exceeded the runtime - otherwise terminate.
        if self.session.cfg.runtime:

            if time.time() >= self._starttime + \
                                           (int(self.session.cfg.runtime) * 60):

                self._log.info('runtime limit (%ss).',
                               self.session.cfg.runtime * 60)
                self._final_cause = 'timeout'
                self.stop()
                return False  # we are done

        return True


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):
        '''
        Check for commands on the control pubsub, mainly waiting for RPC
        requests to handle.
        '''

        self._log.debug_1('control msg %s: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg.get('arg')

        self._log.debug('pilot command: %s: %s', cmd, arg)
        self._prof.prof('cmd', msg="%s : %s" %  (cmd, arg), uid=self._pid)

        if cmd == 'pmgr_heartbeat' and arg['pmgr'] == self._pmgr:
            self._session._hb.beat(uid=self._pmgr)
            return True

        elif cmd == 'cancel_pilots':
            return self._ctrl_cancel_pilots(msg)

        elif cmd == 'service_up':
            return self._ctrl_service_up(msg)


    # --------------------------------------------------------------------------
    #
    def _ctrl_cancel_pilots(self, msg):

        arg = msg['arg']

        if self._pid not in arg.get('uids'):
            self._log.debug('ignore cancel %s', msg)

        self._log.info('cancel pilot cmd')
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'terminate',
                                          'arg' : None})
        self._final_cause = 'cancel'
        self.stop()

        # work is done - unregister this cb
        return False


    # --------------------------------------------------------------------------
    #
    def _ctrl_service_up(self, msg):

        uid = msg['arg']['uid']

        # This message signals that an agent service instance is up and running.
        # We expect to find the service UID in args and can then unblock the
        # service startup wait for that uid

        if uid not in self._service_uids_launched:
            # we do not know this service instance
            self._log.warn('ignore service startup signal for %s', uid)
            return True

        if uid in self._service_uids_running:
            self._log.warn('duplicated service startup signal for %s', uid)
            return True

        self._service_uids_running.append(uid)
        self._log.debug('service %s started (%s / %s)', uid,
                        len(self._service_uids_running),
                        len(self._service_uids_launched))

        # signal main thread when all services are up
        if len(self._service_uids_launched) == \
           len(self._service_uids_running):
            self._services_setup.set()

        return True


    # --------------------------------------------------------------------------
    #
    def _prepare_env(self, env_name, env_spec):

        self._log.debug('env_spec %s: %s', env_name, env_spec)

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


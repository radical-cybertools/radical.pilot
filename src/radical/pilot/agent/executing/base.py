
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import stat
import time

import threading          as mt

import radical.utils      as ru

from ... import states    as rps
from ... import agent     as rpa
from ... import constants as rpc
from ... import utils     as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's spawner types
EXECUTING_NAME_POPEN   = 'POPEN'
EXECUTING_NAME_FLUX    = 'FLUX'
EXECUTING_NAME_NOOP    = 'NOOP'
EXECUTING_NAME_DRAGON  = 'DRAGON'


# ------------------------------------------------------------------------------
#
class AgentExecutingComponent(rpu.AgentComponent):
    '''
    Manage the creation of Task processes, and watch them until they are
    completed (one way or the other).  The spawner thus moves the task from
    PendingExecution to Executing, and then to a final state (or PendingStageOut
    of course).
    '''
    _shell     = ru.which('bash') or '/bin/sh'
    _header    = '#!%s\n' % _shell
    _separator = '\n# ' + '-' * 78 + '\n'


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Spawner
    #
    @classmethod
    def create(cls, cfg, session):

        # Make sure that we are the base-class!
        if cls != AgentExecutingComponent:
            raise TypeError('Factory only available to base class!')

        name = session.rcfg.agent_spawner

        from .popen    import Popen
        from .flux     import Flux
        from .noop     import NOOP
        from .dragon   import Dragon

        impl = {
            EXECUTING_NAME_POPEN : Popen,
            EXECUTING_NAME_FLUX  : Flux,
            EXECUTING_NAME_NOOP  : NOOP,
            EXECUTING_NAME_DRAGON: Dragon,
        }

        if name not in impl:
            raise ValueError('AgentExecutingComponent %s unknown' % name)

        return impl[name](cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        rm_name  = self.session.rcfg.resource_manager
        self._rm = rpa.ResourceManager.create(rm_name,
                                              self.session.cfg,
                                              self.session.rcfg,
                                              self._log, self._prof)

        self._pwd      = os.path.realpath(os.getcwd())
        self.sid       = self.session.uid
        self.pid       = self.session.cfg.pid
        self.resource  = self.session.cfg.resource
        self.rsbox     = self.session.cfg.resource_sandbox
        self.ssbox     = self.session.cfg.session_sandbox
        self.psbox     = self.session.cfg.pilot_sandbox
        self.gtod      = '$RP_PILOT_SANDBOX/gtod'
        self.prof      = '$RP_PILOT_SANDBOX/prof'
        self.rp_ctrl   = ru.which('radical-pilot-control')

        assert self.rp_ctrl, 'radical-pilot-control not found'

        # if so configured, let the tasks know what to use as tmp dir
        self.tmpdir    = os.environ.get('TMPDIR', '/tmp') + '/' + self.sid
        ru.rec_makedir(self.tmpdir)

        self._task_tmp = self.session.rcfg.get('task_tmp', self.tmpdir)

        if self.psbox.startswith(self.ssbox):
            self.psbox = '$RP_SESSION_SANDBOX%s'  % self.psbox[len(self.ssbox):]
        if self.ssbox.startswith(self.rsbox):
            self.ssbox = '$RP_RESOURCE_SANDBOX%s' % self.ssbox[len(self.rsbox):]
        if self.ssbox.endswith(self.sid):
            self.ssbox = '%s$RP_SESSION_ID/'      % self.ssbox[:-len(self.sid)]

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher(rpc.AGENT_UNSCHEDULE_PUBSUB)

        self._to_tasks  = list()
        self._to_lock   = mt.Lock()
        self._to_thread = mt.Thread(target=self._to_watcher)
        self._to_thread.daemon = True
        self._to_thread.start()


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        raise NotImplementedError('work is not implemented')


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        # FIXME RPC: already handled in the component base class
        if cmd == 'cancel_tasks':

            self._log.info('cancel_tasks command (%s)', arg)
            for tid in arg['uids']:
                task = self.get_task(tid)
                if task:
                    self.cancel_task(task)

        elif cmd == 'task_startup_done':

            self._log.info('task_startup_done command (%s)', arg)
            task = self.get_task(arg['uid'])
            if task:
                # if execution timeout is 0., then we will reset cancellation
                cancel_time = task['description'].get('timeout', 0.)
                if cancel_time:
                    cancel_time += time.time()
                with self._to_lock:
                    self._to_tasks.append([task, cancel_time, True])


    # --------------------------------------------------------------------------
    #
    def get_task(self, tid):

        raise NotImplementedError('get_task is not implemented')


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):

        raise NotImplementedError('cancel_task is not implemented')


    # --------------------------------------------------------------------------
    #
    def _to_watcher(self):
        '''
        watch the set of tasks for which timeouts are defined.  If the timeout
        passes and the tasks are still active, kill the task via
        `self._cancel_task(task)`.  That has to be implemented by al executors.
        '''

        # tasks to watch for timeout
        to_tasks = dict()

        while not self._term.is_set():

            # collect new tasks to watch
            with self._to_lock:

                for task, cancel_time, has_started in self._to_tasks:
                    self._log.debug('to_watcher: %s, cancel_time=%s, has_started=%s',
                                    task['uid'], cancel_time, has_started)
                    tid = task['uid']
                    if has_started or tid not in to_tasks:
                        to_tasks[task['uid']] = [task, cancel_time]

                self._to_tasks = list()

            # avoid busy wait
            if not to_tasks:
                time.sleep(1)
                continue

            # list of pairs <task, timeout> sorted by timeout, smallest first
            to_list = sorted(to_tasks.values(), key=lambda x: x[1])
            # cancel all tasks which have timed out
            for task, cancel_time in to_list:
                now = time.time()
                if now > cancel_time:
                    if cancel_time:
                        self._log.warning('task %s timed out after %.2f seconds',
                                          task['uid'], now - cancel_time)
                        self._prof.prof('task_timeout', uid=task['uid'])
                        self.cancel_task(task=task)
                    del to_tasks[task['uid']]
                else:
                    break


    # --------------------------------------------------------------------------
    #
    def handle_timeout(self, task):

        startup_to = task['description'].get('startup_timeout', 0.)
        exec_to    = task['description'].get('timeout',         0.)

        if startup_to or exec_to:

            self._log.debug('handle_timeout: %s, startup_to=%s, exec_to=%s',
                            task['uid'], startup_to, exec_to)

            with self._to_lock:
                cancel_time = time.time() + (startup_to or exec_to)
                has_started = not bool(startup_to)
                self._to_tasks.append([task, cancel_time, has_started])


    # --------------------------------------------------------------------------
    #
    def advance_tasks(self, tasks, state, publish, push, ts=None):
        '''
        sort tasks into different buckets, depending on their origin.
        That origin will determine where tasks which completed execution
        and end up here will be routed to:

          - client: state update to update worker
          - raptor: state update to `STATE_PUBSUB`
          - agent : state update to `STATE_PUBSUB`

        a fallback is not in place to enforce the specification of the
        `origin` attributes for tasks.
        '''


        buckets = {'client': list(),
                   'raptor': list(),
                   'agent' : list()}

        for task in ru.as_list(tasks):
            buckets[task['origin']].append(task)

        # we want any task which has a `raptor_id` set to show up in raptor's
        # result callbacks
        if state != rps.AGENT_EXECUTING:
            for task in ru.as_list(tasks):
                if task['description'].get('raptor_id'):
                    if task not in buckets['raptor']:
                        buckets['raptor'].append(task)

        if buckets['client']:
            self.advance(buckets['client'], state=state,
                                            publish=publish, push=push, ts=ts)

        if buckets['raptor']:
            self.advance(buckets['raptor'], state=state,
                                            publish=publish, push=False, ts=ts)
            self.publish(rpc.STATE_PUBSUB, {'cmd': 'raptor_state_update',
                                            'arg': buckets['raptor']})

        if buckets['agent']:
            self.advance(buckets['agent'], state=state,
                                            publish=publish, push=False, ts=ts)


    # --------------------------------------------------------------------------
    #
    # methods to prepare task exec scripts
    #
    def _create_exec_script(self, launcher, task):

        tid  = task['uid']
        td   = task['description']
        sbox = task['task_sandbox_path']

        env_sbox      = '$RP_TASK_SANDBOX'
        exec_script   = '%s.exec.sh' % tid
        exec_path     = '%s/%s' % (env_sbox, exec_script)
        exec_fullpath = '%s/%s' % (sbox, exec_script)

        # make sure the sandbox exists
        self._prof.prof('task_mkdir', uid=tid)
        ru.rec_makedir(sbox)
        self._prof.prof('task_mkdir_done', uid=tid)

        # the exec shell script runs the same set of commands for all ranks.
        # However, if the ranks need different GPU's assigned, or if either pre-
        # or post-exec directives contain per-rank dictionaries, then we switch
        # per-rank in the script for all sections between pre- and post-exec.

        n_ranks = td['ranks']
        slots   = task.setdefault('slots', {})

        self._extend_pre_exec(td, slots)

        tmp  = ''
        tmp += self._header
        tmp += self._separator
        tmp += self._get_rp_env(task)
        tmp += self._get_rp_funcs()
        tmp += self._separator
        tmp += '# rank ID\n'
        tmp += self._get_rank_ids(n_ranks, launcher)

        if td.get('startup_timeout'):
            tmp += self._separator
            tmp += '# startup completed\n'
            tmp += 'test "$RP_RANK" == "0" && $RP_CTRL '\
                   '$RP_SESSION_ID task_startup_done uid=$RP_TASK_ID\n'

        tmp += self._separator
        tmp += self._get_prof('exec_start')

        tmp += self._get_task_env(task, launcher)

        tmp += self._separator
        tmp += '# pre-exec commands\n'
        tmp += self._get_prof('exec_pre')
        tmp += self._get_prep_exec(task, n_ranks, sig='pre_exec')

        tmp += self._separator
        tmp += '# output file detection (i)\n'
        tmp += "ls | sort | grep -ve '^%s\\.' > %s/%s.files\n" \
                % (tid, env_sbox, tid)

        tmp += self._separator
        tmp += '# execute rank\n'
        tmp += self._get_prof('rank_start')
        tmp += self._get_exec(task, launcher)
        tmp += self._get_prof('rank_stop',
                              msg='RP_EXEC_PID=$RP_EXEC_PID:'
                                  'RP_RANK_PID=$RP_RANK_PID')

        tmp += self._separator
        tmp += '# output file detection (ii)\n'
        tmp += 'ls | sort | comm -23 - ' \
               "%s/%s.files | grep -ve '^%s\\.' > %s/%s.ofiles\n" \
                       % (env_sbox, tid, tid, env_sbox, tid)

        tmp += self._separator
        tmp += '# post-exec commands\n'
        tmp += self._get_prof('exec_post')
        tmp += self._get_prep_exec(task, n_ranks, sig='post_exec')

        tmp += self._separator
        tmp += self._get_prof('exec_stop')
        tmp += 'exit $RP_RET\n'

        fh = os.open(path=exec_fullpath,
                     mode=0o755,
                     flags=(
                             os.O_WRONLY|   # access mode: write only
                             os.O_CREAT |   # create if not exists
                             os.O_TRUNC ))  # truncate the file to zero

        os.write(fh, tmp.encode('utf-8'))
        os.close(fh)

      # with ru.ru_open(exec_fullpath, 'w') as fout:
      #     fout.write(tmp)

      # # make sure scripts are executable
      # st_e = os.stat(exec_fullpath)
      # os.chmod(exec_fullpath, st_e.st_mode | stat.S_IEXEC)

      # # need to set `DEBUG_5` or higher to get slot debug logs
      # if self._log._debug_level >= 5:
      #     ru.write_json(slots, '%s/%s.sl' % (sbox, tid))

        return exec_path, exec_fullpath


    # --------------------------------------------------------------------------
    #
    def _get_task_env(self, task, launcher):

        ret = ''
        td  = task['description']

        # named_env's are prepared by the launcher
        if td['named_env']:
            ret += '\n# named environment\n'
            ret += '. %s\n' % launcher.get_task_named_env(td['named_env'])

        # also add any env vars requested in the task description
        if td['environment']:
            ret += '\n# task env settings\n'
            for key, val in td['environment'].items():
                ret += 'export %s="%s"\n' % (key, val)

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rank_ids(self, n_ranks, launcher):

        ret  = ''
        ret += 'export RP_RANKS=%s\n' % n_ranks
        ret += launcher.get_rank_cmd()

        if n_ranks > 1:

            # make sure that RP_RANK is known (otherwise task fails silently)
            if 'export RP_RANK=' not in ret:
                raise RuntimeError('launch method %s does not export RP_RANK'
                                   % launcher.name)

        # also define a method to sync all ranks on certain events
        ret += '\nrp_sync_ranks() {\n'
        ret += '    sig=$1\n'
        ret += '    echo $RP_RANK >> $sig.sig\n'
        ret += '    while test $(cat $sig.sig | wc -l) -lt $RP_RANKS; do\n'
        ret += '        sleep 1\n'
        ret += '    done\n'
        ret += '}\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def _extend_pre_exec(self, td, slots=None):

        # FIXME: this assumes that the rank has a `gpu_maps` and `core_maps`
        #        with exactly one entry, corresponding to the rank process to be
        #        started.

        # FIXME: need to distinguish between logical and physical IDs

        if td['threading_type'] == rpc.OpenMP:
            # for future updates: if task slots are heterogeneous in terms of
            #                     number of threads, then the following string
            #                     should be converted into dictionary (per rank)
            num_threads = td.get('cores_per_rank', 1)
            td['pre_exec'].append('export OMP_NUM_THREADS=%d' % num_threads)

        if td['gpus_per_rank'] and td['gpu_type'] == rpc.CUDA and slots:
            # equivalent to the 'physical' value for original `cvd_id_mode`
            rank_env = {}
            for rank_id,slot in enumerate(slots):
                rank_env[str(rank_id)] = \
                    'export CUDA_VISIBLE_DEVICES=%s' % \
                    ','.join([str(g['index']) for g in slot['gpus']])
            td['pre_exec'].append(rank_env)

        # pre-defined `pre_exec` per platform configuration
        tmp = self.session.rcfg.get('task_pre_exec')
        if tmp:
            td['pre_exec'].extend(ru.as_list(tmp))


    # --------------------------------------------------------------------------
    #
    def _get_prep_exec(self, task, n_ranks, sig):

        ret = ''
        td  = task['description']

        if sig not in td:
            return ret

        entries         = ru.as_list(td[sig])
        switch_per_rank = any([isinstance(x, dict) for x in entries])
        cmd_template    = '%s || rp_error %s\n'

        sync_ranks_cmd = ''
        if sig == 'pre_exec' and td['pre_exec_sync']:
            sync_ranks_cmd = 'rp_sync_ranks %s\n' % sig

        if not switch_per_rank:
            return ''.join([cmd_template % (x, sig) for x in entries]) + \
                   sync_ranks_cmd

        ret += 'case "$RP_RANK" in\n'
        for rank_id in range(n_ranks):

            ret += '    %d)\n' % rank_id

            for entry in entries:

                if isinstance(entry, str):
                    entry = {str(rank_id): entry}

                for cmd in ru.as_list(entry.get(str(rank_id))):
                    ret += '        ' + cmd_template % (cmd, sig)

            ret += '        ;;\n'

        ret += 'esac\n' + sync_ranks_cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_exec(self, task, launcher):

        # FIXME: core pinning goes here

        ret  = '%s &\n' % launcher.get_exec(task)

        # collect PIDs for exec-script and executable
        ret += '\nRP_EXEC_PID=$$\nRP_RANK_PID=$!\n\n'
        ret += 'wait $RP_RANK_PID\n'

        # set output
        ret += 'RP_RET=$?\n'

        return ret



    # --------------------------------------------------------------------------
    #
    # methods to prepare task launch scripts
    #
    def _create_launch_script(self, launcher, task, exec_path):

        tid  = task['uid']
        sbox = task['task_sandbox_path']

        if not launcher:
            raise RuntimeError('no launcher found for task %s' % tid)

        self._log.debug('Launching task with %s', launcher.name)

        launch_script   = '%s.launch.sh' % tid
        launch_path     = '$RP_TASK_SANDBOX/%s' % launch_script
        launch_fullpath = '%s/%s' % (sbox, launch_script)

        ru.rec_makedir(sbox)

        with ru.ru_open(launch_fullpath, 'w') as fout:

            tmp  = ''
            tmp += self._header
            tmp += self._separator
            tmp += self._get_rp_env(task)
            tmp += self._get_rp_funcs()
            tmp += self._separator
            tmp += self._get_prof('launch_start')

            tmp += self._separator
            tmp += '# change to task sandbox\n'
            tmp += 'cd $RP_TASK_SANDBOX\n'

            tmp += self._separator
            tmp += '# prepare launcher env\n'
            tmp += self._get_launch_env(launcher)

            tmp += self._separator
            tmp += '# pre-launch commands\n'
            tmp += self._get_prof('launch_pre')
            tmp += self._get_prep_launch(task, sig='pre_launch')

            tmp += self._separator
            tmp += '# launch commands\n'
            tmp += self._get_prof('launch_submit')
            tmp += self._get_launch(task, launcher, exec_path)
            tmp += self._get_prof('launch_collect',
                                  msg='RP_LAUNCH_PID=$RP_LAUNCH_PID')

            tmp += self._separator
            tmp += '# post-launch commands\n'
            tmp += self._get_prof('launch_post')
            tmp += self._get_prep_launch(task, sig='post_launch')

            tmp += self._separator
            tmp += self._get_prof('launch_stop')
            tmp += 'exit $RP_RET\n'

            tmp += self._separator
            tmp += '\n'

            fout.write(tmp)


        st_l = os.stat(launch_fullpath)
        os.chmod(launch_fullpath, st_l.st_mode | stat.S_IEXEC)

        return launch_path, launch_fullpath


    # --------------------------------------------------------------------------
    #
    def _get_launch(self, task, launcher, exec_path):

        ret  = '( \\\n'

        for cmd in ru.as_list(launcher.get_launch_cmds(task, exec_path)):
            ret += '  %s \\\n' % cmd

        ret += ') 1> %s \\\n  2> %s\n' % (task['stdout_file_short'],
                                          task['stderr_file_short'])
        # collect PID for launch-script
        ret += 'RP_RET=$?\n'
        ret += 'RP_LAUNCH_PID=$$\n'

        return ret


    # --------------------------------------------------------------------------
    #
    # launcher
    #
    def _get_launch_env(self, launcher):

        ret  = ''

        for cmd in launcher.get_launcher_env():
            ret += '%s || rp_error launcher_env\n' % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_prep_launch(self, task, sig):

        ret = ''
        td = task['description']

        if sig not in td:
            return ret

        for cmd in ru.as_list(task['description'][sig]):
            ret += '%s || rp_error %s\n' % (cmd, sig)

        return ret


    # --------------------------------------------------------------------------
    #
    # methods to prepare task launch and exec scripts
    #
    def _get_rp_env(self, task):

        tid  = task['uid']
        td   = task['description']
        name = task.get('name') or tid
        sbox = os.path.realpath(task['task_sandbox_path'])

        if sbox.startswith(self._pwd):
            sbox = '$RP_PILOT_SANDBOX%s' % sbox[len(self._pwd):]

        gpr = td['gpus_per_rank']
        if int(gpr) == gpr: gpr = '%d' % gpr
        else              : gpr = '%f' % gpr

        ctrl_pub_addr = self._reg['bridges.control_pubsub']['addr_pub']
        ctrl_sub_addr = self._reg['bridges.control_pubsub']['addr_pub']

        ret  = '\n'
        ret += 'export RP_TASK_ID="%s"\n'           % tid
        ret += 'export RP_TASK_NAME="%s"\n'         % name
        ret += 'export RP_PILOT_ID="%s"\n'          % self.pid
        ret += 'export RP_SESSION_ID="%s"\n'        % self.sid
        ret += 'export RP_RESOURCE="%s"\n'          % self.resource
        ret += 'export RP_RESOURCE_SANDBOX="%s"\n'  % self.rsbox
        ret += 'export RP_SESSION_SANDBOX="%s"\n'   % self.ssbox
        ret += 'export RP_PILOT_SANDBOX="%s"\n'     % self.psbox
        ret += 'export RP_TASK_SANDBOX="%s"\n'      % sbox
        ret += 'export RP_REGISTRY_ADDRESS="%s"\n'  % self.session.reg_addr
        ret += 'export RP_CONTROL_PUB_ADDRESS=%s\n' % ctrl_pub_addr
        ret += 'export RP_CONTROL_SUB_ADDRESS=%s\n' % ctrl_sub_addr
        ret += 'export RP_CORES_PER_RANK=%d\n'      % td['cores_per_rank']
        ret += 'export RP_GPUS_PER_RANK=%s\n'       % gpr

        services = td.get('services') or list()
        for service in services:
            info = self._reg['services.%s' % service] or ''
            sid  = service.replace('.', '_').upper()
            ret += 'export RP_INFO_%s="%s"\n' % (sid, str(info))

        # FIXME AM
      # ret += 'export RP_LFS="%s"\n'              % self.lfs
        ret += 'export RP_GTOD="%s"\n'             % self.gtod
        ret += 'export RP_PROF="%s"\n'             % self.prof
        ret += 'export RP_CTRL="%s"\n'             % self.rp_ctrl

        if self._prof.enabled:
            ret += 'export RP_PROF_TGT="%s/%s.prof"\n' % (sbox, tid)
        else:
            ret += 'unset  RP_PROF_TGT\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rp_funcs(self):

        # define helper functions
        ret  = '\nrp_error() {\n'
        ret += '    echo "$1 failed" 1>&2\n'
        ret += '    exit 1\n'
        ret += '}\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_prof(self, event, msg=''):

        return '$RP_PROF %s "%s"\n' % (event, msg)



# ------------------------------------------------------------------------------


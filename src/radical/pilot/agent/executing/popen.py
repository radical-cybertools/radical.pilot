# pylint: disable=unused-argument

__copyright__ = 'Copyright 2013-2016, http://radical.rutgers.edu'
__license__   = 'MIT'


import os
import stat
import time
import queue
import atexit
import pprint
import signal
import threading as mt
import subprocess

import radical.utils as ru

from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
# ensure tasks are killed on termination
_pids = list()


def _kill():
    for pid in _pids:
        if not isinstance(pid, int):
            # skip test mocks
            continue
        try   : os.killpg(pid, signal.SIGTERM)
        except: pass


atexit.register(_kill)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
class Popen(AgentExecutingComponent):

    _header    = '#!/bin/sh\n'
    _separator = '\n# ' + '-' * 78 + '\n'


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

      # session._log.debug('popen init start')
        AgentExecutingComponent.__init__(self, cfg, session)

        self._proc_term = mt.Event()
      # session._log.debug('popen init stop')


    # --------------------------------------------------------------------------
    #
    def initialize(self):

      # self._log.debug('popen initialize start')
        AgentExecutingComponent.initialize(self)

        self._cancel_lock     = mt.RLock()
        self._tasks_to_cancel = list()
        self._tasks_to_watch  = list()
        self._watch_queue     = queue.Queue()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._watcher = mt.Thread(target=self._watch)
      # self._watcher.daemon = True
        self._watcher.start()

      # self._log.debug('popen initialize stop')

    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # FIXME: should be moved to base class `AgentExecutingComponent`?
        self._proc_term.set()

    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_tasks':

            self._log.info('cancel_tasks command (%s)' % arg)
            with self._cancel_lock:
                self._tasks_to_cancel.extend(arg['uids'])

        return True

    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:

            try:
                self._handle_task(task)

            except Exception:
                # append the startup error to the tasks stderr.  This is
                # not completely correct (as this text is not produced
                # by the task), but it seems the most intuitive way to
                # communicate that error to the application/user.
                self._log.exception("error running Task")
                if task['stderr'] is None:
                    task['stderr'] = ''
                task['stderr'] += '\nPilot cannot start task:\n'
                task['stderr'] += '\n'.join(ru.get_exception_trace())

                # can't rely on the executor base to free the task resources
                self._prof.prof('unschedule_start', uid=task['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                task['control'] = 'tmgr_pending'
                task['$all']    = True
                self.advance(task, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task):

        # before we start handling the task, check if it should run in a named
        # env.  If so, inject the activation of that env in the task's pre_exec
        # directives.
        tid  = task['uid']
        td   = task['description']
        sbox = task['task_sandbox_path']

        # prepare stdout/stderr
        task['stdout'] = ''
        task['stderr'] = ''

        stdout_file    = td.get('stdout') or '%s.out' % (tid)
        stderr_file    = td.get('stderr') or '%s.err' % (tid)

        if stdout_file[0] != '/':
            task['stdout_file']       = '%s/%s' % (sbox, stdout_file)
            task['stdout_file_short'] = '$RP_TASK_SANDBOX/%s' % (stdout_file)
        else:
            task['stdout_file']       = stdout_file
            task['stdout_file_short'] = stdout_file

        if stderr_file[0] != '/':
            task['stderr_file']       = '%s/%s' % (sbox, stderr_file)
            task['stderr_file_short'] = '$RP_TASK_SANDBOX/%s' % (stderr_file)
        else:
            task['stderr_file']       = stderr_file
            task['stderr_file_short'] = stderr_file

        # create two shell scripts: a launcher script (task.launch.sh) which
        # sets the launcher environment, performs pre_launch commands, and then
        # launches the second script which executes the task.
        #
        # The second script (`task.exec.sh`) is instantiated once per task rank.
        # It first resets the environment created by the launcher, then prepares
        # the environment for the tasks.  Next it runs the `pre_exec` directives
        # for all ranks, then the individual `pre_rank` directives are executed,
        # and then, after all ranks are synchronized, finally the task ranks
        # begin to run.
        #
        # The scripts thus show the following structure:
        #
        # Launcher Script (`task.000000.launch.sh`):
        # ----------------------------------------------------------------------
        # #!/bin/sh
        #
        # # `pre_launch` commands
        # date > data.input
        #
        # # launcher specific environment setup
        # module load openmpi
        #
        # # launch the task script
        # mpirun -n 4 ./task.000000.exec.sh
        # ----------------------------------------------------------------------
        #
        # Task Script (`task.000000.exec.sh`)
        # ----------------------------------------------------------------------
        # #!/bin/sh
        #
        # # clean launch environment
        # module unload mpi
        #
        # # task environment setup (`pre_exec`)
        # module load gromacs
        #
        # # rank specific setup (`pre_rank`)
        # touch task.000000.ranks
        # if test "$MPI_RANK" = 0; then
        #   export CUDA_VISIBLE_DEVICES=0
        #   export OENMP_NUM_THREADS=2
        #   export RANK_0_VAR=foo
        #   echo 0 >> task.000000.ranks
        # fi
        #
        # if test "$MPI_RANK" = 1; then
        #   export CUDA_VISIBLE_DEVICES=1
        #   export OENMP_NUM_THREADS=4
        #   export RANK_1_VAR=bar
        #   echo 1 >> task.000000.ranks
        # fi
        #
        # # synchronize ranks
        # while $(cat task.000000.ranks | wc -l) != $MPI_RANKS; do
        #   sleep 1
        # done
        #
        # # run the task
        # mdrun -i ... -o ... -foo ... 1> task.000000.$MPI_RANK.out \
        #                              2> task.000000.$MPI_RANK.err
        #
        # # now do the very same stuff for the `post_rank` and `post_exec`
        # # directives
        # ...
        #
        # ----------------------------------------------------------------------
        #
        # NOTE: MongoDB only accepts string keys, and thus the rank IDs in
        #       pre_rank and post_rank dictionaries are rendered as strings.
        #       This should be changed to more intuitive integers once MongoDB
        #       is phased out.
        #
        # prep stdout/err so that we can append w/o checking for None
        task['stdout'] = ''
        task['stderr'] = ''

        launcher = self._rm.find_launcher(task)

        if not launcher:
            raise RuntimeError('no launcher found for task %s' % tid)

        self._log.debug('Launching task with %s', launcher.name)

        launch_script = '%s.launch.sh'        % tid
        exec_script   = '%s.exec.sh'          % tid
        exec_path     = '$RP_TASK_SANDBOX/%s' % exec_script

        ru.rec_makedir(sbox)

        with ru.ru_open('%s/%s' % (sbox, launch_script), 'w') as fout:

            tmp  = ''
            tmp += self._header
            tmp += self._separator
            tmp += self._get_rp_env(task)
            tmp += self._separator
            tmp += self._get_prof('launch_start', tid)

            tmp += self._separator
            tmp += '# change to task sandbox\n'
            tmp += 'cd $RP_TASK_SANDBOX\n'

            tmp += self._separator
            tmp += '# prepare launcher env\n'
            tmp += self._get_launch_env(task, launcher)

            tmp += self._separator
            tmp += '# pre-launch commands\n'
            tmp += self._get_prof('launch_pre', tid)
            tmp += self._get_pre_launch(task, launcher)

            tmp += self._separator
            tmp += '# launch commands\n'
            tmp += self._get_prof('launch_submit', tid)
            tmp += '%s\n' % self._get_launch_cmds(task, launcher, exec_path)
            tmp += 'RP_RET=$?\n'
            tmp += self._get_prof('launch_collect', tid)

            tmp += self._separator
            tmp += '# post-launch commands\n'
            tmp += self._get_prof('launch_post', tid)
            tmp += self._get_post_launch(task, launcher)

            tmp += self._separator
            tmp += self._get_prof('launch_stop', tid)
            tmp += 'exit $RP_RET\n'

            tmp += self._separator
            tmp += '\n'

            fout.write(tmp)


        ranks   = task['slots']['ranks']
        n_ranks = len(ranks)

        with ru.ru_open('%s/%s' % (sbox, exec_script), 'w') as fout:

            tmp  = ''
            tmp += self._header
            tmp += self._separator
            tmp += '# rank ID\n'
            tmp += self._get_rank_ids(n_ranks, launcher)

            tmp += self._separator
            tmp += self._get_rp_env(task)
            tmp += self._separator
            tmp += self._get_prof('exec_start', tid)

            tmp += '# task environment\n'
            tmp += self._get_task_env(task, launcher)

            tmp += self._separator
            tmp += '# pre-exec commands\n'
            tmp += self._get_prof('exec_pre', tid)
            tmp += self._get_pre_exec(task)

            # pre_rank list is applied to rank 0, dict to the ranks listed
            pre_rank = td['pre_rank']
            if isinstance(pre_rank, list): pre_rank = {'0': pre_rank}

            if pre_rank:
                tmp += self._separator
                tmp += self._get_prof('rank_pre', tid)
                tmp += '# pre-rank commands\n'
                tmp += 'case "$RP_RANK" in\n'
                for rank_id, cmds in pre_rank.items():
                    rank_id = int(rank_id)
                    tmp += '    %d)\n' % rank_id
                    tmp += self._get_pre_rank(rank_id, cmds)
                    tmp += '        ;;\n'
                tmp += 'esac\n\n'

                tmp += self._get_rank_sync('pre_rank', n_ranks)

            tmp += self._separator
            tmp += '# execute ranks\n'
            tmp += self._get_prof('rank_start', tid)
            tmp += 'case "$RP_RANK" in\n'
            for rank_id, rank in enumerate(ranks):
                tmp += '    %d)\n' % rank_id
                tmp += self._get_rank_exec(task, rank_id, rank, launcher)
                tmp += '        ;;\n'
            tmp += 'esac\n'
            tmp += 'RP_RET=$?\n'
            tmp += self._get_prof('rank_stop', tid)

            # post_rank list is applied to rank 0, dict to the ranks listed
            post_rank = td['post_rank']
            if isinstance(post_rank, list): post_rank = {'0': post_rank}

            if post_rank:
                tmp += self._separator
                tmp += self._get_prof('rank_post', tid)
                tmp += self._get_rank_sync('post_rank', n_ranks)

                tmp += '\n# post-rank commands\n'
                tmp += 'case "$RP_RANK" in\n'
                for rank_id, cmds in post_rank.items():
                    rank_id = int(rank_id)
                    tmp += '    %d)\n' % rank_id
                    tmp += self._get_post_rank(rank_id, cmds)
                    tmp += '        ;;\n'
                tmp += 'esac\n\n'

            tmp += self._separator
            tmp += self._get_prof('exec_post', tid)
            tmp += '# post exec commands\n'
            tmp += self._get_post_exec(task)

            tmp += self._separator
            tmp += self._get_prof('exec_stop', tid)
            tmp += 'exit $RP_RET\n'

            tmp += self._separator
            tmp += '\n'

            fout.write(tmp)


      # # ensure that the named env exists
      # env = td.get('named_env')
      # if env:
      #     if not os.path.isdir('%s/%s' % (self._pwd, env)):
      #         raise ValueError('invalid named env %s for task %s'
      #                         % (env, task['uid']))
      #     pre = ru.as_list(td.get('pre_exec'))
      #     pre.insert(0, '. %s/%s/bin/activate' % (self._pwd, env))
      #     pre.insert(0, '. %s/deactivate'      % (self._pwd))
      #     td['pre_exec'] = pre

        # make sure scripts are executable
        st_l = os.stat('%s/%s' % (sbox, launch_script))
        st_e = os.stat('%s/%s' % (sbox, exec_script))
        os.chmod('%s/%s' % (sbox, launch_script), st_l.st_mode | stat.S_IEXEC)
        os.chmod('%s/%s' % (sbox, exec_script),   st_e.st_mode | stat.S_IEXEC)

        # make sure the sandbox exists
        slots_fname = '%s/%s.sl' % (sbox, tid)

        with ru.ru_open(slots_fname, 'w') as fout:
            fout.write('\n%s\n\n' % pprint.pformat(task['slots']))

        # make sure the sandbox exists
        self._prof.prof('exec_mkdir', uid=tid)
        ru.rec_makedir(sbox)
        self._prof.prof('exec_mkdir_done', uid=tid)

        # launch and exec script are done, get ready for execution.
        cmdline = '/bin/sh %s' % launch_script

        self._log.info('Launching task %s via %s in %s', tid, cmdline, sbox)

        _launch_out_h = ru.ru_open('%s/%s.launch.out' % (sbox, tid), 'w')

        self._prof.prof('exec_start', uid=tid)
        task['proc'] = subprocess.Popen(args       = cmdline,
                                        executable = None,
                                        stdin      = None,
                                        stdout     = _launch_out_h,
                                        stderr     = subprocess.STDOUT,
                                        close_fds  = True,
                                        shell      = True,
                                        cwd        = sbox)
        # decoupling from parent process group is disabled,
        # in case of enabling it, one of the following options should be added:
        #    `preexec_fn=os.setsid` OR `start_new_session=True`
        self._prof.prof('exec_ok', uid=tid)

        # store pid for last-effort termination
        _pids.append(task['proc'].pid)

        self._watch_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        try:
            while not self._proc_term.is_set():

                tasks = list()
                try:

                    # FIXME: we don't want to only wait for one Task -- then we
                    #        would pull Task state too frequently.  OTOH, we
                    #        also don't want to learn about tasks until all
                    #        slots are filled, because then we may not be able
                    #        to catch finishing tasks in time -- so there is
                    #        a fine balance here.  Balance means 100.
                    MAX_QUEUE_BULKSIZE = 100

                    while len(tasks) < MAX_QUEUE_BULKSIZE :
                        tasks.append (self._watch_queue.get_nowait())

                except queue.Empty:
                    # nothing found -- no problem, see if any tasks finished
                    pass

                # add all tasks we found to the watchlist
                for task in tasks :
                    self._tasks_to_watch.append (task)

                # check on the known tasks.
                action = self._check_running()

                if not action and not tasks :
                    # nothing happened at all!  Zzz for a bit.
                    # FIXME: make configurable
                    time.sleep(0.1)

        except Exception as e:
            self._log.exception('Error in ExecWorker watch loop (%s)' % e)
            # FIXME: this should signal the ExecWorker for shutdown...


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self):

        action = 0
        for task in self._tasks_to_watch:

            # poll subprocess object
            exit_code = task['proc'].poll()
            tid       = task['uid']

            if exit_code is None:
                # Process is still running

                if tid in self._tasks_to_cancel:

                    # FIXME: there is a race condition between the state poll
                    # above and the kill command below.  We probably should pull
                    # state after kill again?

                    self._prof.prof('exec_cancel_start', uid=tid)

                    # We got a request to cancel this task - send SIGTERM to the
                    # process group (which should include the actual launch
                    # method)
                  # task['proc'].kill()
                    action += 1
                    try:
                        os.killpg(task['proc'].pid, signal.SIGTERM)
                    except OSError:
                        # task is already gone, we ignore this
                        pass
                    task['proc'].wait()  # make sure proc is collected

                    with self._cancel_lock:
                        self._tasks_to_cancel.remove(tid)

                    self._prof.prof('exec_cancel_stop', uid=tid)

                    del(task['proc'])  # proc is not json serializable
                    self._prof.prof('unschedule_start', uid=tid)
                    self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)
                    self.advance(task, rps.CANCELED, publish=True, push=False)

                    # we don't need to watch canceled tasks
                    self._tasks_to_watch.remove(task)

            else:

                self._prof.prof('exec_stop', uid=tid)

                # make sure proc is collected
                task['proc'].wait()

                # we have a valid return code -- task is final
                action += 1
                self._log.info("Task %s has return code %s.", tid, exit_code)

                task['exit_code'] = exit_code

                # Free the Slots, Flee the Flots, Ree the Frots!
                self._tasks_to_watch.remove(task)
                del(task['proc'])  # proc is not json serializable
                self._prof.prof('unschedule_start', uid=tid)
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                if exit_code != 0:
                    # The task failed - fail after staging output
                    task['target_state'] = rps.FAILED

                else:
                    # The task finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there
                    # are no directives -- at the very least, we'll upload
                    # stdout/stderr
                    task['target_state'] = rps.DONE

                self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                                   publish=True, push=True)

        return action


    # --------------------------------------------------------------------------
    def _get_check(self, event):

        return '\\\n        || (echo "%s failed"; false) || exit 1\n' % event


    # --------------------------------------------------------------------------
    #
    # launcher
    #
    def _get_prof(self, event, tid, msg=''):

        return '$RP_PROF %s\n' % event


    # --------------------------------------------------------------------------
    #
    # launcher
    #
    def _get_launch_env(self, task, launcher):

        ret  = ''
        cmds = launcher.get_launcher_env()
        for cmd in cmds:
            ret += '%s %s' % (cmd, self._get_check('launcher env'))
        return ret


    # --------------------------------------------------------------------------
    #
    def _get_pre_launch(self, task, launcher):

        ret  = ''
        cmds = task['description']['pre_launch']
        for cmd in cmds:
            ret += '%s %s' % (cmd, self._get_check('pre_launch'))

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_launch_cmds(self, task, launcher, exec_path):

        ret  = '( \\\n'
        cmds = ru.as_list(launcher.get_launch_cmds(task, exec_path))
        for cmd in cmds:
            ret += '  %s \\\n' % cmd

        ret += ') 1> %s\n  2> %s %s' % (task['stdout_file_short'],
                                        task['stderr_file_short'],
                                        self._get_check('launch'))
        return ret


    # --------------------------------------------------------------------------
    #
    def _get_post_launch(self, task, launcher):

        ret  = ''
        cmds = task['description']['post_launch']
        for cmd in cmds:
            ret += '%s %s' % (cmd, self._get_check('post_launch'))

        return ret


    # --------------------------------------------------------------------------
    # exec
    #
    def _get_rp_env(self, task):

        tid  = task['uid']
        name = task.get('name') or tid
        sbox = os.path.realpath(task['task_sandbox_path'])

        if sbox.startswith(self._pwd):
            sbox = '$RP_PILOT_SANDBOX%s' % sbox[len(self._pwd):]

        ret  = ''
        ret += 'export RP_TASK_ID="%s"\n'          % tid
        ret += 'export RP_TASK_NAME="%s"\n'        % name
        ret += 'export RP_PILOT_ID="%s"\n'         % self._pid
        ret += 'export RP_SESSION_ID="%s"\n'       % self.sid
        ret += 'export RP_RESOURCE="%s"\n'         % self.resource
        ret += 'export RP_RESOURCE_SANDBOX="%s"\n' % self.rsbox
        ret += 'export RP_SESSION_SANDBOX="%s"\n'  % self.ssbox
        ret += 'export RP_PILOT_SANDBOX="%s"\n'    % self.psbox
        ret += 'export RP_TASK_SANDBOX="%s"\n'     % sbox
        # FIXME AM
      # ret += 'export RP_LFS="%s"\n'              % self.lfs
        ret += 'export RP_GTOD="%s"\n'             % self.gtod
        ret += 'export RP_PROF="%s"\n'             % self.prof

        if self._prof.enabled:
            ret += 'export RP_PROF_TGT="%s/%s.prof"\n' % (sbox, tid)
        else:
            ret += 'unset  RP_PROF_TGT'

        return ret


    # --------------------------------------------------------------------------
    # exec
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
            for key,val in td['environment'].items():
                ret += 'export %s="%s"\n' % (key, val)

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_pre_exec(self, task):

        ret  = ''

        cmds = task['description']['pre_exec']
        for cmd in cmds:
            ret += '%s %s' % (cmd, self._get_check('pre_exec'))

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rank_ids(self, n_ranks, launcher):

        ret  = ''
        ret += 'export RP_RANKS=%s\n' % n_ranks
        ret += launcher.get_rank_cmd()

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_post_exec(self, task):

        ret  = ''
        cmds = task['description']['post_exec']
        for cmd in cmds:
            ret += '%s %s' % (cmd, self._get_check('post_exec'))

        return ret


    # --------------------------------------------------------------------------
    #
    # rank
    #
    def _get_pre_rank(self, rank_id, cmds=None):

        ret = ''
        cmds = cmds or []
        for cmd in cmds:
            # FIXME: exit on error, but don't stall other ranks on sync
            ret += '        %s\n' % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rank_sync(self, sig, ranks):

        # FIXME: only sync if LM needs it (`if lm._ranks_need_sync`)

        if ranks == 1:
            return ''

        # FIXME: make sure that all ranks are alive
        ret  = '# sync ranks before %s commands\n' % sig
        ret += 'echo $RP_RANK >> %s.sig\n\n' % sig
        ret += 'while test $(cat %s.sig | wc -l) -lt $RP_RANKS; do\n' % sig
        ret += '    sleep 1\n'
        ret += 'done\n\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rank_exec(self, task, rank_id, rank, launcher):

        # FIXME: this assumes that the rank has a `gpu_maps` and `core_maps`
        #        with exactly one entry, corresponding to the rank process to be
        #        started.

        ret  = ''
        gmap = rank['gpu_map']

        # FIXME: need to distinguish between logical and physical IDs
        if gmap:
            # equivalent to the 'physical' value for original `cvd_id_mode`
            gpus = ','.join([str(gpu_set[0]) for gpu_set in gmap])
            ret += '        export CUDA_VISIBLE_DEVICES=%s\n' % gpus

        cmap = rank['core_map'][0]
        ret += '        export OMP_NUM_THREADS="%d"\n' % len(cmap)

        # FIXME: core pinning goes here


        cmds = ru.as_list(launcher.get_rank_exec(task, rank_id, rank))
        for cmd in cmds:
            ret += '        %s\n' % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_post_rank(self, rank_id, cmds=None):

        ret = ''
        cmds = cmds or []
        for cmd in cmds:
            ret += '        %s %s' % (cmd, self._get_check('post_rank'))

        return ret


# ------------------------------------------------------------------------------


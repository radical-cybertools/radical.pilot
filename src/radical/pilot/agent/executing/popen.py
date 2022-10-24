# pylint: disable=unused-argument

__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
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


# pylint: disable=unused-argument
def _kill(*args, **kwargs):

    for pid in _pids:

        # skip test mocks
        if not isinstance(pid, int):
            continue

        try   : os.killpg(pid, signal.SIGTERM)
        except: pass


atexit.register(_kill)
signal.signal(signal.SIGTERM, _kill)
signal.signal(signal.SIGINT,  _kill)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
class Popen(AgentExecutingComponent):

    _header    = '#!/bin/sh\n'
    _separator = '\n# ' + '-' * 78 + '\n'

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
                self._prof.prof('task_start', uid=task['uid'])
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

            finally:
                self._prof.prof('task_stop', uid=task['uid'])


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

        stdout_file    = td.get('stdout') or '%s.out' % tid
        stderr_file    = td.get('stderr') or '%s.err' % tid

        if stdout_file[0] != '/':
            task['stdout_file']       = '%s/%s' % (sbox, stdout_file)
            task['stdout_file_short'] = '$RP_TASK_SANDBOX/%s' % stdout_file
        else:
            task['stdout_file']       = stdout_file
            task['stdout_file_short'] = stdout_file

        if stderr_file[0] != '/':
            task['stderr_file']       = '%s/%s' % (sbox, stderr_file)
            task['stderr_file_short'] = '$RP_TASK_SANDBOX/%s' % stderr_file
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
        # for all ranks and per rank (if commands for particular rank are
        # defined) are executed, and then, after all ranks are synchronized,
        # finally the task ranks begin to run.
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
        # # rank specific setup
        # touch task.000000.ranks
        # if test "$MPI_RANK" = 0; then
        #   export CUDA_VISIBLE_DEVICES=0
        #   export OMP_NUM_THREADS=2
        #   export RANK_0_VAR=foo
        #   echo 0 >> task.000000.ranks
        # fi
        #
        # if test "$MPI_RANK" = 1; then
        #   export CUDA_VISIBLE_DEVICES=1
        #   export OMP_NUM_THREADS=4
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
        # # now do the very same stuff for the `post_exec` directive
        # ...
        #
        # ----------------------------------------------------------------------
        #
        # NOTE: MongoDB only accepts string keys, and thus the rank IDs in
        #       pre_exec and post_exec dictionaries are rendered as strings.
        #       This should be changed to more intuitive integers once MongoDB
        #       is phased out.
        #

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
            tmp += self._get_rp_funcs()
            tmp += self._separator
            tmp += self._get_prof('launch_start', tid)

            tmp += self._separator
            tmp += '# change to task sandbox\n'
            tmp += 'cd $RP_TASK_SANDBOX\n'

            tmp += self._separator
            tmp += '# prepare launcher env\n'
            tmp += self._get_launch_env(launcher)

            tmp += self._separator
            tmp += '# pre-launch commands\n'
            tmp += self._get_prof('launch_pre', tid)
            tmp += self._get_prep_launch(task, sig='pre_launch')

            tmp += self._separator
            tmp += '# launch commands\n'
            tmp += self._get_prof('launch_submit', tid)
            tmp += self._get_launch(task, launcher, exec_path)
            tmp += self._get_prof('launch_collect', tid)

            tmp += self._separator
            tmp += '# post-launch commands\n'
            tmp += self._get_prof('launch_post', tid)
            tmp += self._get_prep_launch(task, sig='post_launch')

            tmp += self._separator
            tmp += self._get_prof('launch_stop', tid)
            tmp += 'exit $RP_RET\n'

            tmp += self._separator
            tmp += '\n'

            fout.write(tmp)

        ranks   = task['slots']['ranks']
        n_ranks = len(ranks)

        # the exec shell script runs the same set of commands for all ranks.
        # However, if the ranks need different GPU's assigned, or if either pre-
        # or post-exec directives contain per-rank dictionaries, then we switch
        # per-rank in the script for all sections between pre- and post-exec.

        self._extend_pre_exec(td, ranks)

        with ru.ru_open('%s/%s' % (sbox, exec_script), 'w') as fout:

            tmp  = ''
            tmp += self._header
            tmp += self._separator
            tmp += self._get_rp_env(task)
            tmp += self._get_rp_funcs()
            tmp += self._separator
            tmp += '# rank ID\n'
            tmp += self._get_rank_ids(n_ranks, launcher)
            tmp += self._separator
            tmp += self._get_prof('exec_start', tid)

            tmp += self._get_task_env(task, launcher)

            tmp += self._separator
            tmp += '# pre-exec commands\n'
            tmp += self._get_prof('exec_pre', tid)
            tmp += self._get_prep_exec(task, n_ranks, sig='pre_exec')

            tmp += self._separator
            tmp += '# execute rank\n'
            tmp += self._get_prof('rank_start', tid)
            tmp += self._get_exec(task, launcher)
            tmp += self._get_prof('rank_stop', tid)

            tmp += self._separator
            tmp += '# post-exec commands\n'
            tmp += self._get_prof('exec_post', tid)
            tmp += self._get_prep_exec(task, n_ranks, sig='post_exec')

            tmp += self._separator
            tmp += self._get_prof('exec_stop', tid)
            tmp += 'exit $RP_RET\n'

            tmp += self._separator
            tmp += '\n'

            fout.write(tmp)


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
        self._prof.prof('task_mkdir', uid=tid)
        ru.rec_makedir(sbox)
        self._prof.prof('task_mkdir_done', uid=tid)

        # launch and exec script are done, get ready for execution.
        cmdline = '/bin/sh %s' % launch_script

        self._log.info('Launching task %s via %s in %s', tid, cmdline, sbox)

        _launch_out_h = ru.ru_open('%s/%s.launch.out' % (sbox, tid), 'w')

        self._prof.prof('task_run_start', uid=tid)
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
        self._prof.prof('task_run_ok', uid=tid)

        # store pid for last-effort termination
        _pids.append(task['proc'].pid)

        self._watch_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        try:
            while not self._term.is_set():

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

                    self._prof.prof('task_run_cancel_start', uid=tid)

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

                    self._prof.prof('task_run_cancel_stop', uid=tid)

                    del task['proc']  # proc is not json serializable
                    self._prof.prof('unschedule_start', uid=tid)
                    self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)
                    self.advance(task, rps.CANCELED, publish=True, push=False)

                    # we don't need to watch canceled tasks
                    self._tasks_to_watch.remove(task)

            else:

                self._prof.prof('task_run_stop', uid=tid)

                # make sure proc is collected
                task['proc'].wait()

                # we have a valid return code -- task is final
                action += 1
                self._log.info("Task %s has return code %s.", tid, exit_code)

                task['exit_code'] = exit_code

                # Free the Slots, Flee the Flots, Ree the Frots!
                self._tasks_to_watch.remove(task)
                del task['proc']  # proc is not json serializable
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
    #
    def _get_prof(self, event, tid, msg=''):

        return '$RP_PROF %s\n' % event


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
    def _get_rp_env(self, task):

        tid  = task['uid']
        name = task.get('name') or tid
        sbox = os.path.realpath(task['task_sandbox_path'])

        if sbox.startswith(self._pwd):
            sbox = '$RP_PILOT_SANDBOX%s' % sbox[len(self._pwd):]

        ret  = '\n'
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
            ret += 'unset  RP_PROF_TGT\n'

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
    def _get_launch(self, task, launcher, exec_path):

        ret  = '( \\\n'

        for cmd in ru.as_list(launcher.get_launch_cmds(task, exec_path)):
            ret += '  %s \\\n' % cmd

        ret += ') 1> %s \\\n  2> %s\n' % (task['stdout_file_short'],
                                          task['stderr_file_short'])
        ret += 'RP_RET=$?\n'

        return ret


    # --------------------------------------------------------------------------
    #
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
            ret += 'test -z "$RP_RANK" && echo "Cannot determine rank"\n'
            ret += 'test -z "$RP_RANK" && exit 1\n'

        else:
            ret += 'test -z "$RP_RANK" && export RP_RANK=0\n'

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
    def _extend_pre_exec(self, td, ranks):

        # FIXME: this assumes that the rank has a `gpu_maps` and `core_maps`
        #        with exactly one entry, corresponding to the rank process to be
        #        started.

        # FIXME: need to distinguish between logical and physical IDs

        if td['threading_type'] == rpc.OpenMP:
            # for future updates: if task ranks are heterogeneous in terms of
            #                     number of threads, then the following string
            #                     should be converted into dictionary (per rank)
            num_threads = td.get('cores_per_rank', 1)
            assert (num_threads == len(ranks[0]['core_map'][0]))
            td['pre_exec'].append('export OMP_NUM_THREADS=%d' % num_threads)

        if td['gpus_per_rank'] and td['gpu_type'] == rpc.CUDA:
            # equivalent to the 'physical' value for original `cvd_id_mode`
            td['pre_exec'].append(
                {str(rank_id): 'export CUDA_VISIBLE_DEVICES=%s' %
                               ','.join([str(gm[0]) for gm in rank['gpu_map']])
                 for rank_id, rank in enumerate(ranks)})


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

        ret  = ''

        for cmd in ru.as_list(launcher.get_exec(task)):
            ret += '%s\n' % cmd
        ret += 'RP_RET=$?\n'

        return ret


# ------------------------------------------------------------------------------


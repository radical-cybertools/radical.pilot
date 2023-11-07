# pylint: disable=unused-argument

__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import stat
import time
import queue
import atexit
import signal
import threading  as mt
import subprocess as sp

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

    # flags for the watcher queue
    TO_WATCH  = 0
    TO_CANCEL = 1

    # --------------------------------------------------------------------------
    #
    def initialize(self):

      # self._log.debug('popen initialize start')
        AgentExecutingComponent.initialize(self)

        self._watch_queue = queue.Queue()

        self._pid = self.session.cfg.pid

        # run watcher thread
        self._watcher = mt.Thread(target=self._watch)
      # self._watcher.daemon = True
        self._watcher.start()

      # self._log.debug('popen initialize stop')


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, uid):

        self._watch_queue.put([self.TO_CANCEL, uid])


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance_tasks(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:

            try:
                self._prof.prof('task_start', uid=task['uid'])
                self._handle_task(task)

            except Exception as e:
                self._log.exception("error running Task")
                task['exception']        = repr(e)
                task['exception_detail'] = '\n'.join(ru.get_exception_trace())

                # can't rely on the executor base to free the task resources
                self._prof.prof('unschedule_start', uid=task['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                task['control'] = 'tmgr_pending'
                task['$all']    = True
                self.advance_tasks(task, rps.FAILED, publish=True, push=False)


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
        # The scripts thus show the following approximate structure:
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

        # the exec shell script runs the same set of commands for all ranks.
        # However, if the ranks need different GPU's assigned, or if either pre-
        # or post-exec directives contain per-rank dictionaries, then we switch
        # per-rank in the script for all sections between pre- and post-exec.

        n_ranks = td['ranks']
        slots   = task.setdefault('slots', {})

        self._extend_pre_exec(td, slots.get('ranks'))

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
        self._prof.prof('task_mkdir', uid=tid)
        ru.rec_makedir(sbox)
        self._prof.prof('task_mkdir_done', uid=tid)

        # need to set `DEBUG_5` or higher to get slot debug logs
        if self._log._debug_level >= 5:
            ru.write_json('%s/%s.sl' % (sbox, tid), slots)

        # launch and exec script are done, get ready for execution.
        cmdline = '%s/%s' % (sbox, launch_script)

        self._log.info('Launching task %s via %s in %s', tid, cmdline, sbox)

        _launch_out_h = ru.ru_open('%s/%s.launch.out' % (sbox, tid), 'w')

        # `start_new_session=True` is default, which enables decoupling
        # from the parent process group (part of the task cancellation)
        _start_new_session = self.session.rcfg.new_session_per_task or False

        self._prof.prof('task_run_start', uid=tid)
        task['proc'] = sp.Popen(args              = cmdline,
                                executable        = None,
                                shell             = False,
                                stdin             = None,
                                stdout            = _launch_out_h,
                                stderr            = sp.STDOUT,
                                start_new_session = _start_new_session,
                                close_fds         = True,
                                cwd               = sbox)
        self._prof.prof('task_run_ok', uid=tid)

        # store pid for last-effort termination
        _pids.append(task['proc'].pid)

        # handle task timeout if needed
        self.handle_timeout(task)

        # watch task for completion
        self._watch_queue.put([self.TO_WATCH, task])


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        to_watch  = list()  # contains task dicts
        to_cancel = set()   # contains task IDs

        try:
            while not self._term.is_set():

                # FIXME: we don't want to only wait for one Task -- then we
                #        would pull Task state too frequently.  OTOH, we
                #        also don't want to learn about tasks until all
                #        slots are filled, because then we may not be able
                #        to catch finishing tasks in time -- so there is
                #        a fine balance here.  Balance means 100.
                MAX_QUEUE_BULKSIZE = 100
                count = 0

                try:
                    while count < MAX_QUEUE_BULKSIZE:

                        flag, thing = self._watch_queue.get_nowait()
                        count += 1

                        # NOTE: `thing` can be task id or task dict, depending
                        #       on the flag value
                        if   flag == self.TO_WATCH : to_watch.append(thing)
                        elif flag == self.TO_CANCEL: to_cancel.add(thing)
                        else: raise RuntimeError('unknown flag %s' % flag)

                except queue.Empty:
                    # nothing found -- no problem, see if any tasks finished
                    pass

                # check on the known tasks.
                action = self._check_running(to_watch, to_cancel)

                # FIXME: remove uids from lists after completion

                if not action and not count:
                    # nothing happened at all!  Zzz for a bit.
                    # FIXME: make configurable
                    time.sleep(0.1)

        except Exception as e:
            self._log.exception('Error in ExecWorker watch loop (%s)' % e)
            # FIXME: this should signal the ExecWorker for shutdown...


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self, to_watch, to_cancel):

        #
        action = False

        # `to_watch.remove()` in the loop requires copy to iterate over the list
        for task in list(to_watch):

            tid = task['uid']

            # poll subprocess object
            exit_code = task['proc'].poll()

            tasks_to_advance = list()
            tasks_to_cancel  = list()

            if exit_code is None:

                # process is still running - cancel if needed
                if tid in to_cancel:

                    self._log.debug('cancel %s', tid)

                    action = True
                    self._prof.prof('task_run_cancel_start', uid=tid)

                    # got a request to cancel this task - send SIGTERM to the
                    # process group (which should include the actual launch
                    # method)
                    try:
                        # kill the whole process group
                        pgrp = os.getpgid(task['proc'].pid)
                        os.killpg(pgrp, signal.SIGKILL)
                    except OSError:
                        # lost race: task is already gone, we ignore this
                        # FIXME: collect and move to DONE/FAILED
                        pass

                    task['proc'].wait()  # make sure proc is collected

                    to_cancel.remove(tid)
                    to_watch.remove(task)
                    del task['proc']  # proc is not json serializable

                    self._prof.prof('task_run_cancel_stop', uid=tid)

                    self._prof.prof('unschedule_start', uid=tid)
                    tasks_to_cancel.append(task)

            else:

                action = True
                self._prof.prof('task_run_stop', uid=tid)

                # make sure proc is collected
                task['proc'].wait()

                # we have a valid return code -- task is final
                self._log.info("Task %s has return code %s.", tid, exit_code)

                task['exit_code'] = exit_code

                # Free the Slots, Flee the Flots, Ree the Frots!
                to_watch.remove(task)
                if tid in to_cancel:
                    to_cancel.remove(tid)
                del task['proc']  # proc is not json serializable
                tasks_to_advance.append(task)

                self._prof.prof('unschedule_start', uid=tid)

                if exit_code != 0:
                    # task failed - fail after staging output
                    task['exception']        = 'RuntimeError("task failed")'
                    task['exception_detail'] = 'exit code: %s' % exit_code
                    task['target_state'    ] = rps.FAILED

                else:
                    # The task finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there
                    # are no directives -- at the very least, we'll upload
                    # stdout/stderr
                    task['target_state'] = rps.DONE

            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB,
                         tasks_to_cancel + tasks_to_advance)

            if tasks_to_cancel:
                self.advance(tasks_to_cancel, rps.CANCELED,
                                              publish=True, push=False)
            if tasks_to_advance:
                self.advance(tasks_to_advance, rps.AGENT_STAGING_OUTPUT_PENDING,
                                               publish=True, push=True)

        return action


    # --------------------------------------------------------------------------
    #
    # pylint: disable=unused-argument
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
        td   = task['description']
        name = task.get('name') or tid
        sbox = os.path.realpath(task['task_sandbox_path'])

        if sbox.startswith(self._pwd):
            sbox = '$RP_PILOT_SANDBOX%s' % sbox[len(self._pwd):]

        gpr = td['gpus_per_rank']
        if int(gpr) == gpr:
            gpr = '%d' % gpr
        else:
            gpr = '%f' % gpr

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
        ret += 'export RP_REGISTRY_ADDRESS="%s"\n' % self.session.reg_addr
        ret += 'export RP_CORES_PER_RANK=%d\n'     % td['cores_per_rank']
        ret += 'export RP_GPUS_PER_RANK=%s\n'      % gpr

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
            if 'export RP_RANK=' not in ret:
                raise RuntimeError('launch method does not export RP_RANK')

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
    def _extend_pre_exec(self, td, ranks=None):

        # FIXME: this assumes that the rank has a `gpu_maps` and `core_maps`
        #        with exactly one entry, corresponding to the rank process to be
        #        started.

        # FIXME: need to distinguish between logical and physical IDs

        if td['threading_type'] == rpc.OpenMP:
            # for future updates: if task ranks are heterogeneous in terms of
            #                     number of threads, then the following string
            #                     should be converted into dictionary (per rank)
            num_threads = td.get('cores_per_rank', 1)
            td['pre_exec'].append('export OMP_NUM_THREADS=%d' % num_threads)

        if td['gpus_per_rank'] and td['gpu_type'] == rpc.CUDA and ranks:
            # equivalent to the 'physical' value for original `cvd_id_mode`
            rank_id  = 0
            rank_env = {}
            for slot_ranks in ranks:
                for gpu_map in slot_ranks['gpu_map']:
                    rank_env[str(rank_id)] = \
                        'export CUDA_VISIBLE_DEVICES=%s' % \
                        ','.join([str(g) for g in gpu_map])
                    rank_id += 1
            td['pre_exec'].append(rank_env)

        # pre-defined `pre_exec` per platform configuration
        td['pre_exec'].extend(ru.as_list(self.session.rcfg.get('task_pre_exec')))


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


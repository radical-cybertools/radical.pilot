# pylint: disable=unused-argument

__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
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

    # flags for the watcher queue
    TO_WATCH  = 0
    TO_CANCEL = 1

    # --------------------------------------------------------------------------
    #
    def initialize(self):

      # self._log.debug('popen initialize start')
        super().initialize()

        self._watch_queue = queue.Queue()

        # run watcher thread
        self._watcher = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()


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

        launcher = self._rm.find_launcher(task)

        exec_path  , _ = self._create_exec_script(launcher, task)
        _, launch_path = self._create_launch_script(launcher, task, exec_path)

        tid  = task['uid']
        sbox = task['task_sandbox_path']

        # launch and exec script are done, get ready for execution.
        self._log.info('Launching task %s via %s in %s', tid, launch_path, sbox)

        _launch_out_h = ru.ru_open('%s/%s.launch.out' % (sbox, tid), 'w')


        # `start_new_session=True` is default, which enables decoupling
        # from the parent process group (part of the task cancellation)
        _start_new_session = self.session.rcfg.new_session_per_task or False

        self._prof.prof('task_run_start', uid=tid)
        task['proc'] = sp.Popen(args              = launch_path,
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


# ------------------------------------------------------------------------------


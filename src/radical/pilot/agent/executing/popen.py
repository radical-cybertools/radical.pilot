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

    # --------------------------------------------------------------------------
    #
    def initialize(self):

      # self._log.debug('popen initialize start')
        super().initialize()

        self._tasks       = dict()
        self._check_lock  = mt.Lock()
        self._watch_queue = queue.Queue()

        # run watcher thread
        self._watcher = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def get_task(self, tid):

        return self._tasks.get(tid)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):

        # was the task even started?
        tid  = task['uid']
        proc = task.get('proc')
        if not proc:
            # task was not started, nothing to do
            # FIXME: this is a race, the executor might already own the task
            self._log.debug('task %s was not started', tid)
            return

        # check if the task is, maybe, already done
        exit_code = proc.poll()
        if exit_code is not None:
            # task is done, nothing to do
            self._log.debug('task %s is already done', tid)
            return

        # remove from tasks dictionary, thus "watcher" will not pick it up
        with self._check_lock:
            if tid not in self._tasks:
                return
            try:
                del self._tasks[tid]
            except KeyError:
                pass

        # task is still running -- cancel it
        self._log.debug('cancel %s', tid)
        self._prof.prof('task_run_cancel_start', uid=tid)

        launcher = self._rm.get_launcher(task['launcher_name'])
        launcher.cancel_task(task, proc.pid)

        proc.wait()  # make sure proc is collected

        try:
            # might race with task collection
            del task['proc']  # proc is not json serializable
        except KeyError:
            pass

        task['exit_code']    = None
        task['target_state'] = rps.CANCELED

        self._prof.prof('task_run_cancel_stop', uid=tid)
        self._prof.prof('unschedule_start', uid=tid)
        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

        self.advance([task], rps.AGENT_STAGING_OUTPUT_PENDING,
                             publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance_tasks(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:

            try:
                self._prof.prof('task_start', uid=task['uid'])
                self._tasks.update({task['uid']: task})
                self._handle_task(task)

            except Exception as e:
                self._log.exception("error running Task")
                task['exception']        = repr(e)
                task['exception_detail'] = '\n'.join(ru.get_exception_trace())

                # can't rely on the executor base to free the task resources
                self._prof.prof('unschedule_start', uid=task['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

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

        launcher, lname = self._rm.find_launcher(task)

        if not launcher:
            raise RuntimeError('no launcher found for %s' % task)

        task['launcher_name'] = lname

        exec_path  , _ = self._create_exec_script(launcher, task)
        _, launch_path = self._create_launch_script(launcher, task, exec_path)

        task['exec_path']   = exec_path
        task['launch_path'] = launch_path

        self._launch_task(task)


    # --------------------------------------------------------------------------
    #
    def _launch_task(self, task):

        tid  = task['uid']
        sbox = task['task_sandbox_path']

        launch_path = task['launch_path']

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
        self._watch_queue.put(task)

        # now that the task cancellation cb would succeed, let's make sure that
        # no cancellation request sneaked in before the task got started
        if self.is_canceled(task) is True:
            self.cancel_task(task)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        to_watch = list()  # contains task dicts

        try:
            while not self._term.is_set():

                # FIXME: we don't want to only wait for one Task -- then we
                #        would pull Task state too frequently.  OTOH, we
                #        also don't want to learn about tasks until all
                #        slots are filled, because then we may not be able
                #        to catch finishing tasks in time -- so there is
                #        a fine balance here.  Fine balance means 100.
                MAX_QUEUE_BULKSIZE = 100
                count = 0

                try:
                    while count < MAX_QUEUE_BULKSIZE:
                        to_watch.append(self._watch_queue.get_nowait())
                        count += 1

                except queue.Empty:
                    pass

                # check on the known tasks.
                self._check_running(to_watch)

                if not count:
                    # no new tasks, no new state -- sleep a bit
                    time.sleep(0.05)

        except Exception as e:
            self._log.exception('Error in ExecWorker watch loop (%s)' % e)
            # FIXME: this should signal the ExecWorker for shutdown...


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self, to_watch):

        tasks_to_advance = list()

        # `to_watch.remove()` in the loop requires copy to iterate over the list
        for task in list(to_watch):

            task_proc = task.get('proc')
            if task_proc is None:
                to_watch.remove(task)
                continue

            tid = task['uid']

            # poll subprocess object
            exit_code = task_proc.poll()
            if exit_code is not None:

                self._prof.prof('task_run_stop', uid=tid)

                # make sure proc is collected
                task_proc.wait()

                # we have a valid return code -- task is final
                self._log.info("Task %s has return code %s.", tid, exit_code)

                # Free the Slots, Flee the Flots, Ree the Frots!
                to_watch.remove(task)

                try:
                    # might race with task cancellation
                    del task['proc']  # proc is not json serializable
                except KeyError:
                    pass

                with self._check_lock:
                    if tid not in self._tasks:
                        # task was canceled before, nothing to do
                        continue
                    try:
                        del self._tasks[tid]
                    except KeyError:
                        pass

                tasks_to_advance.append(task)

                self._prof.prof('unschedule_start', uid=tid)

                if exit_code == 0:
                    # The task finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there
                    # are no directives -- at the very least, we'll upload
                    # stdout/stderr
                    task['exit_code']    = exit_code
                    task['target_state'] = rps.DONE

                else:
                    # task failed (we still run staging output)
                    task['exit_code']        = exit_code
                    task['exception']        = 'RuntimeError("task failed")'
                    task['exception_detail'] = 'exit code: %s' % exit_code
                    task['target_state']     = rps.FAILED

        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, tasks_to_advance)

        if tasks_to_advance:
            self.advance(tasks_to_advance, rps.AGENT_STAGING_OUTPUT_PENDING,
                                           publish=True, push=True)


# ------------------------------------------------------------------------------


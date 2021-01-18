# pylint: disable=subprocess-popen-preexec-fn
# FIXME: review pylint directive - https://github.com/PyCQA/pylint/pull/2087
#        (https://docs.python.org/3/library/subprocess.html#popen-constructor)

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import stat
import time
import queue
import atexit
import pprint
import signal
import tempfile
import threading as mt
import traceback
import subprocess

import radical.utils as ru

from ...  import agent     as rpa
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
# ensure tasks are killed on termination
_pids = list()


def _kill():
    for pid in _pids:
        try   : os.killpg(pid, signal.SIGTERM)
        except: pass


atexit.register(_kill)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
class Popen(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._watcher   = None
        self._terminate = mt.Event()

        AgentExecutingComponent.__init__ (self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        self._cancel_lock     = ru.RLock()
        self._tasks_to_cancel = list()
        self._tasks_to_watch  = list()
        self._watch_queue     = queue.Queue ()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._watcher = mt.Thread(target=self._watch)
      # self._watcher.daemon = True
        self._watcher.start()

        # The AgentExecutingComponent needs the LaunchMethod to construct
        # commands.
        self._task_launcher = rpa.LaunchMethod.create(
                name    = self._cfg.get('task_launch_method'),
                cfg     = self._cfg,
                session = self._session)

        self._mpi_launcher = rpa.LaunchMethod.create(
                name    = self._cfg.get('mpi_launch_method'),
                cfg     = self._cfg,
                session = self._session)

        self.gtod   = "%s/gtod" % self._pwd
        self.tmpdir = tempfile.gettempdir()


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_tasks':

            self._log.info("cancel_tasks command (%s)" % arg)
            with self._cancel_lock:
                self._tasks_to_cancel.extend(arg['uids'])

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:
            self._handle_task(task)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task):

        try:
            descr = task['description']

            # ensure that the named env exists
            env = descr.get('named_env')
            if env:
                if not os.path.isdir('%s/%s' % (self._pwd, env)):
                    raise ValueError('invalid named env %s for task %s'
                                    % (env, task['uid']))
                pre = ru.as_list(descr.get('pre_exec'))
                pre.insert(0, '. %s/%s/bin/activate' % (self._pwd, env))
                pre.insert(0, '. %s/deactivate'      % (self._pwd))
                descr['pre_exec'] = pre


            # prep stdout/err so that we can append w/o checking for None
            task['stdout'] = ''
            task['stderr'] = ''

            cpt = descr['cpu_process_type']
          # gpt = descr['gpu_process_type']  # FIXME: use

            # FIXME: this switch is insufficient for mixed tasks (MPI/OpenMP)
            if cpt == 'MPI': launcher = self._mpi_launcher
            else           : launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (process type = %s)" % cpt)

            self._log.debug("Launching task with %s (%s).",
                            launcher.name, launcher.launch_command)

            # Start a new subprocess to launch the task
            self.spawn(launcher=launcher, task=task)

        except Exception as e:
            # append the startup error to the tasks stderr.  This is
            # not completely correct (as this text is not produced
            # by the task), but it seems the most intuitive way to
            # communicate that error to the application/user.
            self._log.exception("error running Task")
            if task.get('stderr') is None:
                task['stderr'] = ''
            task['stderr'] += "\nPilot cannot start task:\n%s\n%s" \
                            % (str(e), traceback.format_exc())

            # Free the Slots, Flee the Flots, Ree the Frots!
            self._prof.prof('unschedule_start', uid=task['uid'])
            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

            self.advance(task, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, task):

        descr   = task['description']
        sandbox = task['task_sandbox_path']

        # make sure the sandbox exists
        self._prof.prof('exec_mkdir', uid=task['uid'])
        rpu.rec_makedir(sandbox)
        self._prof.prof('exec_mkdir_done', uid=task['uid'])

        launch_script_name = '%s/%s.sh' % (sandbox, task['uid'])
        slots_fname        = '%s/%s.sl' % (sandbox, task['uid'])

        self._log.debug("Created launch_script: %s", launch_script_name)

        # prep stdout/err so that we can append w/o checking for None
        task['stdout'] = ''
        task['stderr'] = ''

        with open(slots_fname, "w") as launch_script:
            launch_script.write('\n%s\n\n' % pprint.pformat(task['slots']))

        with open(launch_script_name, "w") as launch_script:
            launch_script.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            env_string = ''
          # env_string += '. %s/env.orig\n'                % self._pwd
            env_string += 'export RADICAL_BASE="%s"\n'     % self._pwd
            env_string += 'export RP_SESSION_ID="%s"\n'    % self._cfg['sid']
            env_string += 'export RP_PILOT_ID="%s"\n'      % self._cfg['pid']
            env_string += 'export RP_AGENT_ID="%s"\n'      % self._cfg['aid']
            env_string += 'export RP_SPAWNER_ID="%s"\n'    % self.uid
            env_string += 'export RP_TASK_ID="%s"\n'       % task['uid']
            env_string += 'export RP_TASK_NAME="%s"\n'     % task['description'].get('name')
            env_string += 'export RP_GTOD="%s"\n'          % self.gtod
            env_string += 'export RP_TMP="%s"\n'           % self._task_tmp
            env_string += 'export RP_PILOT_SANDBOX="%s"\n' % self._pwd
            env_string += 'export RP_PILOT_STAGING="%s"\n' % self._pwd
            if self._prof.enabled:
                env_string += 'export RP_PROF="%s/%s.prof"\n' % (sandbox, task['uid'])

            else:
                env_string += 'unset  RP_PROF\n'

            if 'RP_APP_TUNNEL' in os.environ:
                env_string += 'export RP_APP_TUNNEL="%s"\n' % os.environ['RP_APP_TUNNEL']

            env_string += '''
prof(){
    if test -z "$RP_PROF"
    then
        return
    fi
    event=$1
    msg=$2
    now=$($RP_GTOD)
    echo "$now,$event,task_script,MainThread,$RP_TASK_ID,AGENT_EXECUTING,$msg" >> $RP_PROF
}
'''

            # FIXME: this should be set by an LaunchMethod filter or something (GPU)
            env_string += 'export OMP_NUM_THREADS="%s"\n' % descr['cpu_threads']

            # The actual command line, constructed per launch-method
            try:
                launch_command, hop_cmd = launcher.construct_command(task, launch_script_name)

                if hop_cmd : cmdline = hop_cmd
                else       : cmdline = launch_script_name

            except Exception as e:
                msg = "Error in spawner (%s)" % e
                self._log.exception(msg)
                raise RuntimeError (msg) from e

            # also add any env vars requested in the task description
            if descr['environment']:
                for key,val in descr['environment'].items():
                    env_string += 'export "%s=%s"\n' % (key, val)

            launch_script.write('\n# Environment variables\n%s\n' % env_string)
            launch_script.write('prof task_start\n')
            launch_script.write('\n# Change to task sandbox\ncd %s\n' % sandbox)

            # FIXME: task_pre_exec should be LM specific
            if self._cfg.get('task_pre_exec'):
                for val in self._cfg['task_pre_exec']:
                    launch_script.write("%s\n"  % val)

            if descr['pre_exec']:
                fail = ' (echo "pre_exec failed"; false) || exit'
                pre  = ''
                for elem in descr['pre_exec']:
                    pre += "%s || %s\n" % (elem, fail)
                # Note: extra spaces below are for visual alignment
                launch_script.write("\n# Pre-exec commands\n")
                launch_script.write('prof task_pre_start\n')
                launch_script.write(pre)
                launch_script.write('prof task_pre_stop\n')

            launch_script.write("\n# The command to run\n")
            launch_script.write('prof task_exec_start\n')
            launch_script.write('%s\n' % launch_command)
            launch_script.write('RETVAL=$?\n')
            launch_script.write('prof task_exec_stop\n')

            # After the universe dies the infrared death, there will be nothing
            if descr['post_exec']:
                fail = ' (echo "post_exec failed"; false) || exit'
                post = ''
                for elem in descr['post_exec']:
                    post += "%s || %s\n" % (elem, fail)
                launch_script.write("\n# Post-exec commands\n")
                launch_script.write('prof task_post_start\n')
                launch_script.write('%s\n' % post)
                launch_script.write('prof task_post_stop "$ret=RETVAL"\n')

            launch_script.write("\n# Exit the script with the return code from the command\n")
            launch_script.write("prof task_stop\n")
            launch_script.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)

        # prepare stdout/stderr
        stdout_file = descr.get('stdout') or '%s.out' % task['uid']
        stderr_file = descr.get('stderr') or '%s.err' % task['uid']

        task['stdout_file'] = os.path.join(sandbox, stdout_file)
        task['stderr_file'] = os.path.join(sandbox, stderr_file)

        _stdout_file_h = open(task['stdout_file'], 'a')
        _stderr_file_h = open(task['stderr_file'], 'a')

        self._log.info("Launching task %s via %s in %s", task['uid'], cmdline, sandbox)

        self._prof.prof('exec_start', uid=task['uid'])
        task['proc'] = subprocess.Popen(args       = cmdline,
                                      executable = None,
                                      stdin      = None,
                                      stdout     = _stdout_file_h,
                                      stderr     = _stderr_file_h,
                                      preexec_fn = os.setsid,
                                      close_fds  = True,
                                      shell      = True,
                                      cwd        = sandbox)
        self._prof.prof('exec_ok', uid=task['uid'])

        # store pid for last-effort termination
        _pids.append(task['proc'].pid)

        self._watch_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        try:
            while not self._terminate.is_set():

                tasks = list()
                try:
                    # we don't want to only wait for one Task -- then we would
                    # pull Task state too frequently.  OTOH, we also don't want to
                    # learn about tasks until all slots are filled, because then
                    # we may not be able to catch finishing tasks in time -- so
                    # there is a fine balance here.  Balance means 100 (FIXME).
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
            self._log.exception("Error in ExecWorker watch loop (%s)" % e)
            # FIXME: this should signal the ExecWorker for shutdown...


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self):

        action = 0
        for task in self._tasks_to_watch:

            # poll subprocess object
            exit_code = task['proc'].poll()
            uid       = task['uid']

            if exit_code is None:
                # Process is still running

                if task['uid'] in self._tasks_to_cancel:

                    # FIXME: there is a race condition between the state poll
                    # above and the kill command below.  We probably should pull
                    # state after kill again?

                    self._prof.prof('exec_cancel_start', uid=uid)

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
                        self._tasks_to_cancel.remove(uid)

                    self._prof.prof('exec_cancel_stop', uid=uid)

                    del(task['proc'])  # proc is not json serializable
                    self._prof.prof('unschedule_start', uid=task['uid'])
                    self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)
                    self.advance(task, rps.CANCELED, publish=True, push=False)

                    # we don't need to watch canceled tasks
                    self._tasks_to_watch.remove(task)

            else:

                self._prof.prof('exec_stop', uid=uid)

                # make sure proc is collected
                task['proc'].wait()

                # we have a valid return code -- task is final
                action += 1
                self._log.info("Task %s has return code %s.", uid, exit_code)

                task['exit_code'] = exit_code

                # Free the Slots, Flee the Flots, Ree the Frots!
                self._tasks_to_watch.remove(task)
                del(task['proc'])  # proc is not json serializable
                self._prof.prof('unschedule_start', uid=task['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                if exit_code != 0:
                    # The task failed - fail after staging output
                    task['target_state'] = rps.FAILED

                else:
                    # The task finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there are no
                    # directives -- at the very least, we'll upload stdout/stderr
                    task['target_state'] = rps.DONE

                self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        return action


# ------------------------------------------------------------------------------


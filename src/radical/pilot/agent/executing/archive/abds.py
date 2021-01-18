
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import stat
import time
import queue
import tempfile
import threading
import traceback
import subprocess

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class ABDS(AgentExecutingComponent):

    # The name is rong based on the abstraction, but for the moment I do not
    # have any other ideas

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        from .... import pilot as rp

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
        self._watch_queue     = queue.Queue()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._terminate = threading.Event()
        self._watcher   = threading.Thread(target=self._watch, name="Watcher")
        self._watcher.daemon = True
        self._watcher.start ()

        # The AgentExecutingComponent needs the LaunchMethod to construct
        # commands.
        self._task_launcher = rp.agent.LaunchMethod.create(
                name    = self._cfg['task_launch_method'],
                cfg     = self._cfg,
                session = self._session)

        self._mpi_launcher = rp.agent.LaunchMethod.create(
                name    = self._cfg['mpi_launch_method'],
                cfg     = self._cfg,
                session = self._session)

        self._task_environment = self._populate_task_environment()

        self.gtod   = "%s/gtod" % self._pwd
        self.tmpdir = tempfile.gettempdir()

        # if we need to transplant any original env into the Task, we dig the
        # respective keys from the dump made by bootstrap_0.sh
        self._env_task_export = dict()
        if self._cfg.get('export_to_task'):
            with open('env.orig', 'r') as f:
                for line in f.readlines():
                    if '=' in line:
                        k,v = line.split('=', 1)
                        key = k.strip()
                        val = v.strip()
                        if key in self._cfg['export_to_task']:
                            self._env_task_export[key] = val


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # terminate watcher thread
        self._terminate.set()
        if self._watcher:
            self._watcher.join()


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_tasks':

            self._log.info("cancel_tasks command (%s)" % arg)
            with self._cancel_lock:
                self._tasks_to_cancel.extend(arg['uids'])

        return True


    # --------------------------------------------------------------------------
    #
    def _populate_task_environment(self):
        """Derive the environment for the task's from our own environment."""

        # Get the environment of the agent
        new_env = copy.deepcopy(os.environ)

        #
        # Mimic what virtualenv's "deactivate" would do
        #
        old_path = new_env.pop('_OLD_VIRTUAL_PATH', None)
        if old_path:
            new_env['PATH'] = old_path

        old_ppath = new_env.pop('_OLD_VIRTUAL_PYTHONPATH', None)
        if old_ppath:
            new_env['PYTHONPATH'] = old_ppath

        old_home = new_env.pop('_OLD_VIRTUAL_PYTHONHOME', None)
        if old_home:
            new_env['PYTHON_HOME'] = old_home

        old_ps = new_env.pop('_OLD_VIRTUAL_PS1', None)
        if old_ps:
            new_env['PS1'] = old_ps

        new_env.pop('VIRTUAL_ENV', None)

        # Remove the configured set of environment variables from the
        # environment that we pass to Popen.
        for e in list(new_env.keys()):
            env_removables = list()
            if self._mpi_launcher : env_removables += self._mpi_launcher.env_removables
            if self._task_launcher: env_removables += self._task_launcher.env_removables
            for r in  env_removables:
                if e.startswith(r):
                    new_env.pop(e, None)

        return new_env


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)

        for task in tasks:
            self._handle_task(task)

        self.advance(tasks, rps.AGENT_EXECUTING_PENDING, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task):

        try:
            if task['description']['mpi']:
                launcher = self._mpi_launcher
            else :
                launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (mpi=%s)" % task['description']['mpi'])

            self._log.debug("Launching task with %s (%s).", launcher.name, launcher.launch_command)

            assert(task['slots']) # FIXME: no assert, but check
            self._prof.prof('exec', msg='task launch', uid=task['uid'])

            # Start a new subprocess to launch the task
            self.spawn(launcher=launcher, task=task)

        except Exception as e:
            # append the startup error to the tasks stderr.  This is
            # not completely correct (as this text is not produced
            # by the task), but it seems the most intuitive way to
            # communicate that error to the application/user.
            self._log.exception("error running Task")
            task['stderr'] += "\nPilot cannot start task:\n%s\n%s" \
                            % (str(e), traceback.format_exc())

            # Free the Slots, Flee the Flots, Ree the Frots!
            if task['slots']:
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

            self.advance(task, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, task):

        self._prof.prof('spawn', msg='task spawn', uid=task['uid'])

        sandbox = task['task_sandbox_path']

        # make sure the sandbox exists
        rpu.rec_makedir(sandbox)

        # prep stdout/err so that we can append w/o checking for None
        task['stdout']  = ''
        task['stderr']  = ''

        launch_script_name = '%s/%s.sh' % (sandbox, task['uid'])

        with open(launch_script_name, "w") as launch_script:
            launch_script.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            env_string  = ''
            if task['description']['environment']:
                for key,val in task['description']['environment'].items():
                    env_string += 'export %s="%s"\n' % (key, val)
            env_string += 'export RP_SESSION_ID="%s"\n'    % self._cfg['sid']
            env_string += 'export RP_PILOT_ID="%s"\n'      % self._cfg['pid']
            env_string += 'export RP_AGENT_ID="%s"\n'      % self._cfg['aid']
            env_string += 'export RP_SPAWNER_ID="%s"\n'    % self.uid
            env_string += 'export RP_TASK_ID="%s"\n'       % task['uid']
            env_string += 'export RP_TASK_NAME="%s"\n'     % task['description'].get('name')
            env_string += 'export RP_GTOD="%s"\n'          % self.gtod
            env_string += 'export RP_PILOT_STAGING="%s"\n' % self._pwd
            if self._prof.enabled:
                env_string += 'export RP_PROF="%s/%s.prof"\n' % (sandbox, task['uid'])

            # also add any env vars requested for export by the resource config
            for k,v in self._env_task_export.items():
                env_string += "export %s=%s\n" % (k,v)

            env_string += '''
prof(){
    if test -z "$RP_PROF"
    then
        return
    fi
    event=$1
    now=$($RP_GTOD)
    echo "$now,$event,task_script,MainThread,$RP_TASK_ID,AGENT_EXECUTING," >> $RP_PROF
}
'''

            # also add any env vars requested in the task description
            if task['description']['environment']:
                for key,val in task['description']['environment'].items():
                    env_string += 'export %s=%s\n' % (key, val)

            launch_script.write('\n# Environment variables\n%s\n' % env_string)

            launch_script.write('prof task_start\n')
            launch_script.write('\n# Change to task sandbox\ncd %s\n' % sandbox)

            # Before the Big Bang there was nothing
            if task['description']['pre_exec']:
                pre_exec_string = ''
                if isinstance(task['description']['pre_exec'], list):
                    for elem in task['description']['pre_exec']:
                        pre_exec_string += "%s\n" % elem
                else:
                    pre_exec_string += "%s\n" % task['description']['pre_exec']
                # Note: extra spaces below are for visual alignment
                launch_script.write("\n# Pre-exec commands\n")
                launch_script.write('prof task_pre_start\n')
                launch_script.write(pre_exec_string)
                launch_script.write('prof task_pre_stop\n')

            # YARN pre execution folder permission change
            launch_script.write('\n## Changing Working Directory permissions for YARN\n')
            launch_script.write('old_perm="`stat -c %a .`"\n')
            launch_script.write('chmod -R 777 .\n')

            # The actual command line, constructed per launch-method
            try:
                self._log.debug("Launch Script Name %s",launch_script_name)
                launch_command, hop_cmd = launcher.construct_command(task, launch_script_name)
                self._log.debug("Launch Command %s from %s", launch_command, launcher.name)

                if hop_cmd : cmdline = hop_cmd
                else       : cmdline = launch_script_name

            except Exception as e:
                msg = "Error in spawner (%s)" % e
                self._log.exception(msg)
                raise RuntimeError(msg)

            launch_script.write("\n# The command to run\n")
            launch_script.write('prof task_exec_start\n')
            launch_script.write("%s\n" % launch_command)
            launch_script.write("RETVAL=$?\n")
            launch_script.write("\ncat Ystdout\n")
            launch_script.write('prof task_exec_stop\n')

            # After the universe dies the infrared death, there will be nothing
            if task['description']['post_exec']:
                post_exec_string = ''
                if isinstance(task['description']['post_exec'], list):
                    for elem in task['description']['post_exec']:
                        post_exec_string += "%s\n" % elem
                else:
                    post_exec_string += "%s\n" % task['description']['post_exec']
                launch_script.write("\n# Post-exec commands\n")
                launch_script.write('prof task_post_start\n')
                launch_script.write('%s\n' % post_exec_string)
                launch_script.write('prof task_post_stop\n')

            # YARN pre execution folder permission change
            launch_script.write('\n## Changing Working Directory permissions for YARN\n')
            launch_script.write('chmod $old_perm .\n')

            launch_script.write("\n# Exit the script with the return code from the command\n")
            launch_script.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)
        self._prof.prof('control', msg='launch script constructed', uid=task['uid'])

        # prepare stdout/stderr
        stdout_file = task['description'].get('stdout') or '%s.out' % task['uid']
        stderr_file = task['description'].get('stderr') or '%s.err' % task['uid']

        task['stdout_file'] = os.path.join(sandbox, stdout_file)
        task['stderr_file'] = os.path.join(sandbox, stderr_file)

        _stdout_file_h = open(task['stdout_file'], "w+")
        _stderr_file_h = open(task['stderr_file'], "w+")
        self._prof.prof('control', msg='stdout and stderr files created', uid=task['uid'])

        self._log.info("Launching task %s via %s in %s", task['uid'], cmdline, sandbox)

        self._prof.prof('spawn', msg='spawning passed to popen', uid=task['uid'])

        task['proc'] = subprocess.Popen(args               = cmdline,
                                        bufsize            = 0,
                                        executable         = None,
                                        stdin              = None,
                                        stdout             = _stdout_file_h,
                                        stderr             = _stderr_file_h,
                                        preexec_fn         = None,
                                        close_fds          = True,
                                        shell              = True,
                                        cwd                = sandbox,
                                        env                = self._task_environment,
                                        universal_newlines = False,
                                        startupinfo        = None,
                                        creationflags      = 0)

        self._prof.prof('spawn', msg='spawning passed to popen', uid=task['uid'])
        self._watch_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        try:
            self._prof.prof('run', uid=self._pid)

            while not self._terminate.is_set():

                tasks = list()

                try:

                    # we don't want to only wait for one Task -- then we would
                    # pull Task state too frequently.  OTOH, we also don't want to
                    # learn about tasks until all slots are filled, because then
                    # we may not be able to catch finishing tasks in time -- so
                    # there is a fine balance here.  Balance means 100 (FIXME).
                  # self._prof.prof('pull')
                    MAX_QUEUE_BULKSIZE = 100
                    while len(tasks) < MAX_QUEUE_BULKSIZE :
                        tasks.append (self._watch_queue.get_nowait())

                except queue.Empty:

                    # nothing found -- no problem, see if any tasks finished
                    pass

                # add all tasks we found to the watchlist
                for task in tasks :

                    self._prof.prof('passed', msg="ExecWatcher picked up task",
                                              uid=task['uid'])
                    self._tasks_to_watch.append (task)

                # check on the known tasks.
                action = self._check_running()

                if not action and not tasks :
                    # nothing happened at all!  Zzz for a bit.
                    time.sleep(self._cfg['db_poll_sleeptime'])

        except Exception as e:
            self._log.exception("Error in ExecWorker watch loop (%s)" % e)
            # FIXME: this should signal the ExecWorker for shutdown...

        self._prof.prof('stop', uid=self._pid)
        self._prof.flush()


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self):

        action = 0

        for task in self._tasks_to_watch:

            sandbox = '%s/%s' % (self._pwd, task['uid'])

            # ------------------------------------------------------------------
            # This code snippet reads the YARN application report file and if
            # the application is RUNNING it update the state of the Task with
            # the right time stamp. In any other case it works as it was.
            logfile = '%s/%s' % (sandbox, '/YarnApplicationReport.log')
            if task['state'] == rps.AGENT_EXECUTING_PENDING \
                    and os.path.isfile(logfile):

                yarnreport = open(logfile,'r')
                report_contents = yarnreport.readlines()
                yarnreport.close()

                for report_line in report_contents:
                    if report_line.find('RUNNING') != -1:
                        self._log.debug(report_contents)
                        line = report_line.split(',')
                        ts   = (int(line[3].split('=')[1])/1000)
                        action += 1
                        proc = task['proc']
                        self._log.debug('Proc Print {0}'.format(proc))
                        del(task['proc'])  # proc is not json serializable
                        self.advance(task, rps.AGENT_EXECUTING, publish=True,
                                     push=False,ts=ts)
                        task['proc'] = proc

                        # FIXME: Ioannis, what is this supposed to do?  I wanted
                        # to update the state of the task but keep it in the
                        # watching queue. I am not sure it is needed anymore.
                        index = self._tasks_to_watch.index(task)
                        self._tasks_to_watch[index] = task

            else :
                # poll subprocess object
                exit_code = task['proc'].poll()

                if exit_code is None:
                    # Process is still running

                    if task['uid'] in self._tasks_to_cancel:

                        # FIXME: there is a race condition between the state poll
                        # above and the kill command below.  We probably should pull
                        # state after kill again?

                        # We got a request to cancel this task
                        action += 1
                        task['proc'].kill()
                        task['proc'].wait() # make sure proc is collected

                        with self._cancel_lock:
                            self._tasks_to_cancel.remove(task['uid'])

                        self._prof.prof('final', msg="execution canceled",
                                uid=task['uid'])

                        self._tasks_to_watch.remove(task)

                        del(task['proc'])  # proc is not json serializable
                        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)
                        self.advance(task, rps.CANCELED, publish=True, push=False)

                else:
                    self._prof.prof('exec', msg='execution complete', uid=task['uid'])


                    # make sure proc is collected
                    task['proc'].wait()

                    # we have a valid return code -- task is final
                    action += 1
                    self._log.info("Task %s has return code %s.", task['uid'], exit_code)

                    task['exit_code'] = exit_code

                    # Free the Slots, Flee the Flots, Ree the Frots!
                    self._tasks_to_watch.remove(task)
                    del(task['proc'])  # proc is not json serializable
                    self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                    if exit_code != 0:
                        # The task failed - fail after staging output
                        self._prof.prof('final', msg="execution failed",
                                uid=task['uid'])
                        task['target_state'] = rps.FAILED

                    else:
                        # The task finished cleanly, see if we need to deal with
                        # output data.  We always move to stageout, even if there are no
                        # directives -- at the very least, we'll upload stdout/stderr
                        self._prof.prof('final', msg="execution succeeded",
                                uid=task['uid'])
                        task['target_state'] = rps.DONE

                    self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        return action



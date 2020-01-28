
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

        self._cancel_lock    = ru.RLock()
        self._cus_to_cancel  = list()
        self._cus_to_watch   = list()
        self._watch_queue    = queue.Queue()

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

        self._cu_environment = self._populate_cu_environment()

        self.gtod   = "%s/gtod" % self._pwd
        self.tmpdir = tempfile.gettempdir()

        # if we need to transplant any original env into the CU, we dig the
        # respective keys from the dump made by bootstrap_0.sh
        self._env_cu_export = dict()
        if self._cfg.get('export_to_cu'):
            with open('env.orig', 'r') as f:
                for line in f.readlines():
                    if '=' in line:
                        k,v = line.split('=', 1)
                        key = k.strip()
                        val = v.strip()
                        if key in self._cfg['export_to_cu']:
                            self._env_cu_export[key] = val


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

        if cmd == 'cancel_units':

            self._log.info("cancel_units command (%s)" % arg)
            with self._cancel_lock:
                self._cus_to_cancel.extend(arg['uids'])

        return True


    # --------------------------------------------------------------------------
    #
    def _populate_cu_environment(self):
        """Derive the environment for the cu's from our own environment."""

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
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_SCHEDULING, publish=True, push=False)

        for unit in units:
            self._handle_unit(unit)

        self.advance(units, rps.AGENT_EXECUTING_PENDING, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, cu):

        try:
            if cu['description']['mpi']:
                launcher = self._mpi_launcher
            else :
                launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (mpi=%s)" % cu['description']['mpi'])

            self._log.debug("Launching unit with %s (%s).", launcher.name, launcher.launch_command)

            assert(cu['slots']) # FIXME: no assert, but check
            self._prof.prof('exec', msg='unit launch', uid=cu['uid'])

            # Start a new subprocess to launch the unit
            self.spawn(launcher=launcher, cu=cu)

        except Exception as e:
            # append the startup error to the units stderr.  This is
            # not completely correct (as this text is not produced
            # by the unit), but it seems the most intuitive way to
            # communicate that error to the application/user.
            self._log.exception("error running CU")
            cu['stderr'] += "\nPilot cannot start compute unit:\n%s\n%s" \
                            % (str(e), traceback.format_exc())

            # Free the Slots, Flee the Flots, Ree the Frots!
            if cu['slots']:
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

            self.advance(cu, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        self._prof.prof('spawn', msg='unit spawn', uid=cu['uid'])

        sandbox = cu['unit_sandbox_path']

        # make sure the sandbox exists
        rpu.rec_makedir(sandbox)

        # prep stdout/err so that we can append w/o checking for None
        cu['stdout']  = ''
        cu['stderr']  = ''

        launch_script_name = '%s/%s.sh' % (sandbox, cu['uid'])

        with open(launch_script_name, "w") as launch_script:
            launch_script.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            env_string  = ''
            if cu['description']['environment']:
                for key,val in cu['description']['environment'].items():
                    env_string += 'export %s="%s"\n' % (key, val)
            env_string += 'export RP_SESSION_ID="%s"\n'   % self._cfg['sid']
            env_string += 'export RP_PILOT_ID="%s"\n'     % self._cfg['pid']
            env_string += 'export RP_AGENT_ID="%s"\n'     % self._cfg['aid']
            env_string += 'export RP_SPAWNER_ID="%s"\n'   % self.uid
            env_string += 'export RP_UNIT_ID="%s"\n'      % cu['uid']
            env_string += 'export RP_UNIT_NAME="%s"\n'    % cu['description'].get('name')
            env_string += 'export RP_GTOD="%s"\n'         % self.gtod
            env_string += 'export RP_PILOT_STAGING="%s/staging_area"\n' \
                                                          % self._pwd
            if self._prof.enabled:
                env_string += 'export RP_PROF="%s/%s.prof"\n' % (sandbox, cu['uid'])

            # also add any env vars requested for export by the resource config
            for k,v in self._env_cu_export.items():
                env_string += "export %s=%s\n" % (k,v)

            env_string += '''
prof(){
    if test -z "$RP_PROF"
    then
        return
    fi
    event=$1
    now=$($RP_GTOD)
    echo "$now,$event,unit_script,MainThread,$RP_UNIT_ID,AGENT_EXECUTING," >> $RP_PROF
}
'''

            # also add any env vars requested in the unit description
            if cu['description']['environment']:
                for key,val in cu['description']['environment'].items():
                    env_string += 'export %s=%s\n' % (key, val)

            launch_script.write('\n# Environment variables\n%s\n' % env_string)

            launch_script.write('prof cu_start\n')
            launch_script.write('\n# Change to unit sandbox\ncd %s\n' % sandbox)
            launch_script.write('prof cu_cd_done\n')

            # Before the Big Bang there was nothing
            if cu['description']['pre_exec']:
                pre_exec_string = ''
                if isinstance(cu['description']['pre_exec'], list):
                    for elem in cu['description']['pre_exec']:
                        pre_exec_string += "%s\n" % elem
                else:
                    pre_exec_string += "%s\n" % cu['description']['pre_exec']
                # Note: extra spaces below are for visual alignment
                launch_script.write("\n# Pre-exec commands\n")
                launch_script.write('prof cu_pre_start\n')
                launch_script.write(pre_exec_string)
                launch_script.write('prof cu_pre_stop\n')

            # YARN pre execution folder permission change
            launch_script.write('\n## Changing Working Directory permissions for YARN\n')
            launch_script.write('old_perm="`stat -c %a .`"\n')
            launch_script.write('chmod -R 777 .\n')

            # The actual command line, constructed per launch-method
            try:
                self._log.debug("Launch Script Name %s",launch_script_name)
                launch_command, hop_cmd = launcher.construct_command(cu, launch_script_name)
                self._log.debug("Launch Command %s from %s", launch_command, launcher.name)

                if hop_cmd : cmdline = hop_cmd
                else       : cmdline = launch_script_name

            except Exception as e:
                msg = "Error in spawner (%s)" % e
                self._log.exception(msg)
                raise RuntimeError(msg)

            launch_script.write("\n# The command to run\n")
            launch_script.write('prof cu_exec_start\n')
            launch_script.write("%s\n" % launch_command)
            launch_script.write("RETVAL=$?\n")
            launch_script.write("\ncat Ystdout\n")
            launch_script.write('prof cu_exec_stop\n')

            # After the universe dies the infrared death, there will be nothing
            if cu['description']['post_exec']:
                post_exec_string = ''
                if isinstance(cu['description']['post_exec'], list):
                    for elem in cu['description']['post_exec']:
                        post_exec_string += "%s\n" % elem
                else:
                    post_exec_string += "%s\n" % cu['description']['post_exec']
                launch_script.write("\n# Post-exec commands\n")
                launch_script.write('prof cu_post_start\n')
                launch_script.write('%s\n' % post_exec_string)
                launch_script.write('prof cu_post_stop\n')

            # YARN pre execution folder permission change
            launch_script.write('\n## Changing Working Directory permissions for YARN\n')
            launch_script.write('chmod $old_perm .\n')

            launch_script.write("\n# Exit the script with the return code from the command\n")
            launch_script.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)
        self._prof.prof('control', msg='launch script constructed', uid=cu['uid'])

        # prepare stdout/stderr
        stdout_file = cu['description'].get('stdout') or 'STDOUT'
        stderr_file = cu['description'].get('stderr') or 'STDERR'

        cu['stdout_file'] = os.path.join(sandbox, stdout_file)
        cu['stderr_file'] = os.path.join(sandbox, stderr_file)

        _stdout_file_h = open(cu['stdout_file'], "w+")
        _stderr_file_h = open(cu['stderr_file'], "w+")
        self._prof.prof('control', msg='stdout and stderr files created', uid=cu['uid'])

        self._log.info("Launching unit %s via %s in %s", cu['uid'], cmdline, sandbox)

        self._prof.prof('spawn', msg='spawning passed to popen', uid=cu['uid'])

        cu['proc'] = subprocess.Popen(args               = cmdline,
                                      bufsize            = 0,
                                      executable         = None,
                                      stdin              = None,
                                      stdout             = _stdout_file_h,
                                      stderr             = _stderr_file_h,
                                      preexec_fn         = None,
                                      close_fds          = True,
                                      shell              = True,
                                      cwd                = sandbox,
                                      env                = self._cu_environment,
                                      universal_newlines = False,
                                      startupinfo        = None,
                                      creationflags      = 0)

        self._prof.prof('spawn', msg='spawning passed to popen', uid=cu['uid'])
        self._watch_queue.put(cu)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        try:
            cuid = self.uid.replace('Component', 'Watcher')
            self._prof.prof('run', uid=self._pid)

            while not self._terminate.is_set():

                cus = list()

                try:

                    # we don't want to only wait for one CU -- then we would
                    # pull CU state too frequently.  OTOH, we also don't want to
                    # learn about CUs until all slots are filled, because then
                    # we may not be able to catch finishing CUs in time -- so
                    # there is a fine balance here.  Balance means 100 (FIXME).
                  # self._prof.prof('pull')
                    MAX_QUEUE_BULKSIZE = 100
                    while len(cus) < MAX_QUEUE_BULKSIZE :
                        cus.append (self._watch_queue.get_nowait())

                except queue.Empty:

                    # nothing found -- no problem, see if any CUs finished
                    pass

                # add all cus we found to the watchlist
                for cu in cus :

                    self._prof.prof('passed', msg="ExecWatcher picked up unit", uid=cu['uid'])
                    self._cus_to_watch.append (cu)

                # check on the known cus.
                action = self._check_running()

                if not action and not cus :
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

        for cu in self._cus_to_watch:

            sandbox = '%s/%s' % (self._pwd, cu['uid'])

            #-------------------------------------------------------------------
            # This code snippet reads the YARN application report file and if
            # the application is RUNNING it update the state of the CU with the
            # right time stamp. In any other case it works as it was.
            logfile = '%s/%s' % (sandbox, '/YarnApplicationReport.log')
            if cu['state']==rps.AGENT_EXECUTING_PENDING \
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
                        proc = cu['proc']
                        self._log.debug('Proc Print {0}'.format(proc))
                        del(cu['proc'])  # proc is not json serializable
                        self.advance(cu, rps.AGENT_EXECUTING, publish=True,
                                     push=False,ts=ts)
                        cu['proc']    = proc

                        # FIXME: Ioannis, what is this supposed to do?
                        # I wanted to update the state of the cu but keep it in the watching
                        # queue. I am not sure it is needed anymore.
                        index = self._cus_to_watch.index(cu)
                        self._cus_to_watch[index]=cu

            else :
                # poll subprocess object
                exit_code = cu['proc'].poll()
                now       = time.time()

                if exit_code is None:
                    # Process is still running

                    if cu['uid'] in self._cus_to_cancel:

                        # FIXME: there is a race condition between the state poll
                        # above and the kill command below.  We probably should pull
                        # state after kill again?

                        # We got a request to cancel this cu
                        action += 1
                        cu['proc'].kill()
                        cu['proc'].wait() # make sure proc is collected

                        with self._cancel_lock:
                            self._cus_to_cancel.remove(cu['uid'])

                        self._prof.prof('final', msg="execution canceled", uid=cu['uid'])

                        self._cus_to_watch.remove(cu)

                        del(cu['proc'])  # proc is not json serializable
                        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)
                        self.advance(cu, rps.CANCELED, publish=True, push=False)

                else:
                    self._prof.prof('exec', msg='execution complete', uid=cu['uid'])


                    # make sure proc is collected
                    cu['proc'].wait()

                    # we have a valid return code -- unit is final
                    action += 1
                    self._log.info("Unit %s has return code %s.", cu['uid'], exit_code)

                    cu['exit_code'] = exit_code

                    # Free the Slots, Flee the Flots, Ree the Frots!
                    self._cus_to_watch.remove(cu)
                    del(cu['proc'])  # proc is not json serializable
                    self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

                    if exit_code != 0:
                        # The unit failed - fail after staging output
                        self._prof.prof('final', msg="execution failed", uid=cu['uid'])
                        cu['target_state'] = rps.FAILED

                    else:
                        # The unit finished cleanly, see if we need to deal with
                        # output data.  We always move to stageout, even if there are no
                        # directives -- at the very least, we'll upload stdout/stderr
                        self._prof.prof('final', msg="execution succeeded", uid=cu['uid'])
                        cu['target_state'] = rps.DONE

                    self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        return action



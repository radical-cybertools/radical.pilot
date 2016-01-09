
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import stat
import time
import Queue
import tempfile
import threading
import traceback
import subprocess

import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ==============================================================================
#
class Popen(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        AgentExecutingComponent.__init__ (self, cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

      # self.declare_input (rps.AGENT_EXECUTING_PENDING, rpc.AGENT_EXECUTING_QUEUE)
      # self.declare_worker(rps.AGENT_EXECUTING_PENDING, self.work)

        self.declare_input (rps.EXECUTING_PENDING, rpc.AGENT_EXECUTING_QUEUE)
        self.declare_worker(rps.EXECUTING_PENDING, self.work)

        self.declare_output(rps.AGENT_STAGING_OUTPUT_PENDING, rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.declare_publisher ('unschedule', rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.declare_publisher ('state',      rpc.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rpc.AGENT_COMMAND_PUBSUB)
        self.declare_subscriber('command', rpc.AGENT_COMMAND_PUBSUB, self.command_cb)

        self._cancel_lock    = threading.RLock()
        self._cus_to_cancel  = list()
        self._cus_to_watch   = list()
        self._watch_queue    = Queue.Queue ()

        self._pilot_id = self._cfg['pilot_id']

        # run watcher thread
        self._terminate = threading.Event()
        self._watcher   = threading.Thread(target=self._watch, name="Watcher")
        self._watcher.daemon = True
        self._watcher.start ()

        # The AgentExecutingComponent needs the LaunchMethods to construct
        # commands.
        self._task_launcher = rp.agent.LM.create(
                name   = self._cfg.get('task_launch_method'),
                cfg    = self._cfg,
                logger = self._log)

        self._mpi_launcher = rp.agent.LM.create(
                name   = self._cfg.get('mpi_launch_method'),
                cfg    = self._cfg,
                logger = self._log)

        # communicate successful startup
        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})

        self._cu_environment = self._populate_cu_environment()

        self.tmpdir = tempfile.gettempdir()


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        # terminate watcher thread
        self._terminate.set()
        self._watcher.join()

        # communicate finalization
        self.publish('command', {'cmd' : 'final',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_unit':

            self._log.info("cancel unit command (%s)" % arg)
            with self._cancel_lock:
                self._cus_to_cancel.append(arg)


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

        old_home = new_env.pop('_OLD_VIRTUAL_PYTHONHOME', None)
        if old_home:
            new_env['PYTHON_HOME'] = old_home

        old_ps = new_env.pop('_OLD_VIRTUAL_PS1', None)
        if old_ps:
            new_env['PS1'] = old_ps

        new_env.pop('VIRTUAL_ENV', None)

        # Remove the configured set of environment variables from the
        # environment that we pass to Popen.
        for e in new_env.keys():
            env_removables = list()
            if self._mpi_launcher : env_removables += self._mpi_launcher.env_removables
            if self._task_launcher: env_removables += self._task_launcher.env_removables
            for r in  env_removables:
                if e.startswith(r):
                    new_env.pop(e, None)

        return new_env


    # --------------------------------------------------------------------------
    #
    def work(self, cu):

      # self.advance(cu, rps.AGENT_EXECUTING, publish=True, push=False)
        self.advance(cu, rps.EXECUTING, publish=True, push=False)

        try:
            if cu['description']['mpi']:
                launcher = self._mpi_launcher
            else :
                launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (mpi=%s)" % cu['description']['mpi'])

            self._log.debug("Launching unit with %s (%s).", launcher.name, launcher.launch_command)

            assert(cu['opaque_slots']) # FIXME: no assert, but check
            self._prof.prof('exec', msg='unit launch', uid=cu['_id'])

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
            if cu['opaque_slots']:
                self.publish('unschedule', cu)

            self.advance(cu, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        self._prof.prof('spawn', msg='unit spawn', uid=cu['_id'])

        if False:
            cu_tmpdir = '%s/%s' % (self.tmpdir, cu['_id'])
        else:
            cu_tmpdir = cu['workdir']

        rpu.rec_makedir(cu_tmpdir)
        launch_script_name = '%s/radical_pilot_cu_launch_script.sh' % cu_tmpdir
        self._log.debug("Created launch_script: %s", launch_script_name)

        with open(launch_script_name, "w") as launch_script:
            launch_script.write('#!/bin/sh\n\n')

            if 'RADICAL_PILOT_PROFILE' in os.environ:
                launch_script.write("echo script start_script `%s` >> %s/PROF\n" % (cu['gtod'], cu_tmpdir))
            launch_script.write('\n# Change to working directory for unit\ncd %s\n' % cu_tmpdir)
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                launch_script.write("echo script after_cd `%s` >> %s/PROF\n" % (cu['gtod'], cu_tmpdir))

            # Before the Big Bang there was nothing
            if cu['description']['pre_exec']:
                pre_exec_string = ''
                if isinstance(cu['description']['pre_exec'], list):
                    for elem in cu['description']['pre_exec']:
                        pre_exec_string += "%s\n" % elem
                else:
                    pre_exec_string += "%s\n" % cu['description']['pre_exec']
                # Note: extra spaces below are for visual alignment
                launch_script.write("# Pre-exec commands\n")
                if 'RADICAL_PILOT_PROFILE' in os.environ:
                    launch_script.write("echo pre  start `%s` >> %s/PROF\n" % (cu['gtod'], cu_tmpdir))
                launch_script.write(pre_exec_string)
                if 'RADICAL_PILOT_PROFILE' in os.environ:
                    launch_script.write("echo pre  stop `%s` >> %s/PROF\n" % (cu['gtod'], cu_tmpdir))

            # Create string for environment variable setting
            env_string = 'export'
            if cu['description']['environment']:
                for key,val in cu['description']['environment'].iteritems():
                    env_string += ' %s=%s' % (key, val)
            env_string += " RP_SESSION_ID=%s" % self._cfg['session_id']
            env_string += " RP_PILOT_ID=%s"   % self._cfg['pilot_id']
            env_string += " RP_AGENT_ID=%s"   % self._cfg['agent_name']
            env_string += " RP_SPAWNER_ID=%s" % self.cname
            env_string += " RP_UNIT_ID=%s"    % cu['_id']
            launch_script.write('# Environment variables\n%s\n' % env_string)

            # The actual command line, constructed per launch-method
            try:
                launch_command, hop_cmd = launcher.construct_command(cu, launch_script_name)

                if hop_cmd : cmdline = hop_cmd
                else       : cmdline = launch_script_name

            except Exception as e:
                msg = "Error in spawner (%s)" % e
                self._log.exception(msg)
                raise RuntimeError(msg)

            launch_script.write("# The command to run\n")
            launch_script.write("%s\n" % launch_command)
            launch_script.write("RETVAL=$?\n")
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                launch_script.write("echo script after_exec `%s` >> %s/PROF\n" % (cu['gtod'], cu_tmpdir))

            # After the universe dies the infrared death, there will be nothing
            if cu['description']['post_exec']:
                post_exec_string = ''
                if isinstance(cu['description']['post_exec'], list):
                    for elem in cu['description']['post_exec']:
                        post_exec_string += "%s\n" % elem
                else:
                    post_exec_string += "%s\n" % cu['description']['post_exec']
                launch_script.write("# Post-exec commands\n")
                if 'RADICAL_PILOT_PROFILE' in os.environ:
                    launch_script.write("echo post start `%s` >> %s/PROF\n" % (cu['gtod'], cu_tmpdir))
                launch_script.write('%s\n' % post_exec_string)
                if 'RADICAL_PILOT_PROFILE' in os.environ:
                    launch_script.write("echo post stop  `%s` >> %s/PROF\n" % (cu['gtod'], cu_tmpdir))

            launch_script.write("# Exit the script with the return code from the command\n")
            launch_script.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)
        self._prof.prof('command', msg='launch script constructed', uid=cu['_id'])

        _stdout_file_h = open(cu['stdout_file'], "w")
        _stderr_file_h = open(cu['stderr_file'], "w")
        self._prof.prof('command', msg='stdout and stderr files created', uid=cu['_id'])

        self._log.info("Launching unit %s via %s in %s", cu['_id'], cmdline, cu_tmpdir)

        proc = subprocess.Popen(args               = cmdline,
                                bufsize            = 0,
                                executable         = None,
                                stdin              = None,
                                stdout             = _stdout_file_h,
                                stderr             = _stderr_file_h,
                                preexec_fn         = None,
                                close_fds          = True,
                                shell              = True,
                                cwd                = cu_tmpdir,
                                env                = self._cu_environment,
                                universal_newlines = False,
                                startupinfo        = None,
                                creationflags      = 0)

        self._prof.prof('spawn', msg='spawning passed to popen', uid=cu['_id'])

        cu['started'] = rpu.timestamp()
        cu['proc']    = proc

        self._watch_queue.put(cu)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        self._prof.prof('run', uid=self._pilot_id)
        try:

            while not self._terminate.is_set():

                cus = list()

                try:

                    # we don't want to only wait for one CU -- then we would
                    # pull CU state too frequently.  OTOH, we also don't want to
                    # learn about CUs until all slots are filled, because then
                    # we may not be able to catch finishing CUs in time -- so
                    # there is a fine balance here.  Balance means 100 (FIXME).
                  # self._prof.prof('ExecWorker popen watcher pull cu from queue')
                    MAX_QUEUE_BULKSIZE = 100
                    while len(cus) < MAX_QUEUE_BULKSIZE :
                        cus.append (self._watch_queue.get_nowait())

                except Queue.Empty:

                    # nothing found -- no problem, see if any CUs finished
                    pass

                # add all cus we found to the watchlist
                for cu in cus :

                    self._prof.prof('passed', msg="ExecWatcher picked up unit", uid=cu['_id'])
                    self._cus_to_watch.append (cu)

                # check on the known cus.
                action = self._check_running()

                if not action and not cus :
                    # nothing happened at all!  Zzz for a bit.
                    time.sleep(self._cfg['db_poll_sleeptime'])

        except Exception as e:
            self._log.exception("Error in ExecWorker watch loop (%s)" % e)
            # FIXME: this should signal the ExecWorker for shutdown...


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self):

        action = 0

        for cu in self._cus_to_watch:

            # poll subprocess object
            exit_code = cu['proc'].poll()
            now       = rpu.timestamp()

            if exit_code is None:
                # Process is still running

                if cu['_id'] in self._cus_to_cancel:

                    # FIXME: there is a race condition between the state poll
                    # above and the kill command below.  We probably should pull
                    # state after kill again?

                    # We got a request to cancel this cu
                    action += 1
                    cu['proc'].kill()
                    cu['proc'].wait() # make sure proc is collected

                    with self._cancel_lock:
                        self._cus_to_cancel.remove(cu['_id'])

                    self._prof.prof('final', msg="execution canceled", uid=cu['_id'])

                    del(cu['proc'])  # proc is not json serializable
                    self.publish('unschedule', cu)
                    self.advance(cu, rps.CANCELED, publish=True, push=False)

                    # we don't need to watch canceled CUs
                    self._cus_to_watch.remove(cu)

            else:
                self._prof.prof('exec', msg='execution complete', uid=cu['_id'])

                # make sure proc is collected
                cu['proc'].wait()

                # we have a valid return code -- unit is final
                action += 1
                self._log.info("Unit %s has return code %s.", cu['_id'], exit_code)

                cu['exit_code'] = exit_code
                cu['finished']  = now

                # Free the Slots, Flee the Flots, Ree the Frots!
                self._cus_to_watch.remove(cu)
                del(cu['proc'])  # proc is not json serializable
                self.publish('unschedule', cu)

                if exit_code != 0:
                    # The unit failed - fail after staging output
                    self._prof.prof('final', msg="execution failed", uid=cu['_id'])
                    cu['target_state'] = rps.FAILED

                else:
                    # The unit finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there are no
                    # directives -- at the very least, we'll upload stdout/stderr
                    self._prof.prof('final', msg="execution succeeded", uid=cu['_id'])
                    cu['target_state'] = rps.DONE

                self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        return action



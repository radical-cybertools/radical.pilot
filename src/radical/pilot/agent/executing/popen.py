
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import stat
import time
import queue
import signal
import tempfile
import threading as mt
import traceback
import subprocess

import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


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

        self._cancel_lock    = ru.RLock()
        self._cus_to_cancel  = list()
        self._cus_to_watch   = list()
        self._watch_queue    = queue.Queue ()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._watcher = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()

        # The AgentExecutingComponent needs the LaunchMethod to construct
        # commands.
        self._task_launcher = rp.agent.LaunchMethod.create(
                name    = self._cfg.get('task_launch_method'),
                cfg     = self._cfg,
                session = self._session)

        self._mpi_launcher = rp.agent.LaunchMethod.create(
                name    = self._cfg.get('mpi_launch_method'),
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
    def command_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

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

        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)

        for unit in units:
            self._handle_unit(unit)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, cu):

        try:
            # prep stdout/err so that we can append w/o checking for None
            cu['stdout'] = ''
            cu['stderr'] = ''

            cpt = cu['description']['cpu_process_type']
          # gpt = cu['description']['gpu_process_type']  # FIXME: use

            # FIXME: this switch is insufficient for mixed units (MPI/OpenMP)
            if cpt == 'MPI': launcher = self._mpi_launcher
            else           : launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (process type = %s)" % cpt)

            self._log.debug("Launching unit with %s (%s).",
                            launcher.name, launcher.launch_command)

            # Start a new subprocess to launch the unit
            self.spawn(launcher=launcher, cu=cu)

        except Exception as e:
            # append the startup error to the units stderr.  This is
            # not completely correct (as this text is not produced
            # by the unit), but it seems the most intuitive way to
            # communicate that error to the application/user.
            self._log.exception("error running CU")
            if cu.get('stderr') is None:
                cu['stderr'] = ''
            cu['stderr'] += "\nPilot cannot start compute unit:\n%s\n%s" \
                            % (str(e), traceback.format_exc())

            # Free the Slots, Flee the Flots, Ree the Frots!
            self._prof.prof('unschedule_start', uid=cu['uid'])
            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

            self.advance(cu, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        descr   = cu['description']
        sandbox = cu['unit_sandbox_path']

        # make sure the sandbox exists
        self._prof.prof('exec_mkdir', uid=cu['uid'])
        rpu.rec_makedir(sandbox)
        self._prof.prof('exec_mkdir_done', uid=cu['uid'])
        launch_script_name = '%s/%s.sh' % (sandbox, cu['uid'])

        self._log.debug("Created launch_script: %s", launch_script_name)

        # prep stdout/err so that we can append w/o checking for None
        cu['stdout'] = ''
        cu['stderr'] = ''

        with open(launch_script_name, "w") as launch_script:
            launch_script.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            env_string = ''
            env_string += 'export RP_SESSION_ID="%s"\n'   % self._cfg['sid']
            env_string += 'export RP_PILOT_ID="%s"\n'     % self._cfg['pid']
            env_string += 'export RP_AGENT_ID="%s"\n'     % self._cfg['aid']
            env_string += 'export RP_SPAWNER_ID="%s"\n'   % self.uid
            env_string += 'export RP_UNIT_ID="%s"\n'      % cu['uid']
            env_string += 'export RP_UNIT_NAME="%s"\n'    % cu['description'].get('name')
            env_string += 'export RP_GTOD="%s"\n'         % self.gtod
            env_string += 'export RP_TMP="%s"\n'          % self._cu_tmp
            env_string += 'export RP_PILOT_STAGING="%s/staging_area"\n' \
                                                          % self._pwd
            if self._prof.enabled:
                env_string += 'export RP_PROF="%s/%s.prof"\n' % (sandbox, cu['uid'])

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
    echo "$now,$event,unit_script,MainThread,$RP_UNIT_ID,AGENT_EXECUTING,$msg" >> $RP_PROF
}
'''

            # FIXME: this should be set by an LaunchMethod filter or something (GPU)
            env_string += 'export OMP_NUM_THREADS="%s"\n' % descr['cpu_threads']

            # The actual command line, constructed per launch-method
            try:
                launch_command, hop_cmd = launcher.construct_command(cu, launch_script_name)

                if hop_cmd : cmdline = hop_cmd
                else       : cmdline = launch_script_name

            except Exception as e:
                msg = "Error in spawner (%s)" % e
                self._log.exception(msg)
                raise RuntimeError(msg)

            # also add any env vars requested for export by the resource config
            for k,v in self._env_cu_export.items():
                env_string += "export %s=%s\n" % (k,v)

            # also add any env vars requested in the unit description
            if descr['environment']:
                for key,val in descr['environment'].items():
                    env_string += 'export "%s=%s"\n' % (key, val)

            launch_script.write('\n# Environment variables\n%s\n' % env_string)
            launch_script.write('prof cu_start\n')
            launch_script.write('\n# Change to unit sandbox\ncd %s\n' % sandbox)
            launch_script.write('prof cu_cd_done\n')

            # Before the Big Bang there was nothing
            if self._cfg.get('cu_pre_exec'):
                for val in self._cfg['cu_pre_exec']:
                    launch_script.write("%s\n"  % val)

            if descr['pre_exec']:
                fail = ' (echo "pre_exec failed"; false) || exit'
                pre  = ''
                for elem in descr['pre_exec']:
                    pre += "%s || %s\n" % (elem, fail)
                # Note: extra spaces below are for visual alignment
                launch_script.write("\n# Pre-exec commands\n")
                launch_script.write('prof cu_pre_start\n')
                launch_script.write(pre)
                launch_script.write('prof cu_pre_stop\n')

            launch_script.write("\n# The command to run\n")
            launch_script.write('prof cu_exec_start\n')
            launch_script.write('%s\n' % launch_command)
            launch_script.write('RETVAL=$?\n')
            launch_script.write('prof cu_exec_stop\n')

            # After the universe dies the infrared death, there will be nothing
            if descr['post_exec']:
                fail = ' (echo "post_exec failed"; false) || exit'
                post = ''
                for elem in descr['post_exec']:
                    post += "%s || %s\n" % (elem, fail)
                launch_script.write("\n# Post-exec commands\n")
                launch_script.write('prof cu_post_start\n')
                launch_script.write('%s\n' % post)
                launch_script.write('prof cu_post_stop "$ret=RETVAL"\n')

            launch_script.write("\n# Exit the script with the return code from the command\n")
            launch_script.write("prof cu_stop\n")
            launch_script.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)

        # prepare stdout/stderr
        stdout_file = descr.get('stdout') or 'STDOUT'
        stderr_file = descr.get('stderr') or 'STDERR'

        cu['stdout_file'] = os.path.join(sandbox, stdout_file)
        cu['stderr_file'] = os.path.join(sandbox, stderr_file)

        _stdout_file_h = open(cu['stdout_file'], "w")
        _stderr_file_h = open(cu['stderr_file'], "w")

        self._log.info("Launching unit %s via %s in %s", cu['uid'], cmdline, sandbox)

        self._prof.prof('exec_start', uid=cu['uid'])
        cu['proc'] = subprocess.Popen(args       = cmdline,
                                      executable = None,
                                      stdin      = None,
                                      stdout     = _stdout_file_h,
                                      stderr     = _stderr_file_h,
                                      preexec_fn = os.setsid,
                                      close_fds  = True,
                                      shell      = True,
                                      cwd        = sandbox)
        self._prof.prof('exec_ok', uid=cu['uid'])

        self._watch_queue.put(cu)


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        try:
            while not self._terminate.is_set():

                cus = list()
                try:
                    # we don't want to only wait for one CU -- then we would
                    # pull CU state too frequently.  OTOH, we also don't want to
                    # learn about CUs until all slots are filled, because then
                    # we may not be able to catch finishing CUs in time -- so
                    # there is a fine balance here.  Balance means 100 (FIXME).
                    MAX_QUEUE_BULKSIZE = 100
                    while len(cus) < MAX_QUEUE_BULKSIZE :
                        cus.append (self._watch_queue.get_nowait())

                except queue.Empty:
                    # nothing found -- no problem, see if any CUs finished
                    pass

                # add all cus we found to the watchlist
                for cu in cus :
                    self._cus_to_watch.append (cu)

                # check on the known cus.
                action = self._check_running()

                if not action and not cus :
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
        for cu in self._cus_to_watch:

            # poll subprocess object
            exit_code = cu['proc'].poll()
            uid       = cu['uid']

            if exit_code is None:
                # Process is still running

                if cu['uid'] in self._cus_to_cancel:

                    # FIXME: there is a race condition between the state poll
                    # above and the kill command below.  We probably should pull
                    # state after kill again?

                    self._prof.prof('exec_cancel_start', uid=uid)

                    # We got a request to cancel this cu - send SIGTERM to the
                    # process group (which should include the actual launch
                    # method)
                  # cu['proc'].kill()
                    action += 1
                    try:
                        os.killpg(cu['proc'].pid, signal.SIGTERM)
                    except OSError:
                        # unit is already gone, we ignore this
                        pass
                    cu['proc'].wait()  # make sure proc is collected

                    with self._cancel_lock:
                        self._cus_to_cancel.remove(uid)

                    self._prof.prof('exec_cancel_stop', uid=uid)

                    del(cu['proc'])  # proc is not json serializable
                    self._prof.prof('unschedule_start', uid=cu['uid'])
                    self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)
                    self.advance(cu, rps.CANCELED, publish=True, push=False)

                    # we don't need to watch canceled CUs
                    self._cus_to_watch.remove(cu)

            else:

                self._prof.prof('exec_stop', uid=uid)

                # make sure proc is collected
                cu['proc'].wait()

                # we have a valid return code -- unit is final
                action += 1
                self._log.info("Unit %s has return code %s.", uid, exit_code)

                cu['exit_code'] = exit_code

                # Free the Slots, Flee the Flots, Ree the Frots!
                self._cus_to_watch.remove(cu)
                del(cu['proc'])  # proc is not json serializable
                self._prof.prof('unschedule_start', uid=cu['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

                if exit_code != 0:
                    # The unit failed - fail after staging output
                    cu['target_state'] = rps.FAILED

                else:
                    # The unit finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there are no
                    # directives -- at the very least, we'll upload stdout/stderr
                    cu['target_state'] = rps.DONE

                self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        return action


# ------------------------------------------------------------------------------


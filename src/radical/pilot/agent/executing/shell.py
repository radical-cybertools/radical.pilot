
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import threading
import traceback

import radical.utils as ru

from ... import states    as rps
from ... import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class Shell(AgentExecutingComponent):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        from .... import pilot as rp

        self._pwd = os.getcwd()

        self.register_input(rps.EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        # Mimic what virtualenv's "deactivate" would do
        self._deactivate = "\n# deactivate pilot virtualenv\n"

        old_path  = os.environ.get('_OLD_VIRTUAL_PATH',       None)
        old_ppath = os.environ.get('_OLD_VIRTUAL_PYTHONPATH', None)
        old_home  = os.environ.get('_OLD_VIRTUAL_PYTHONHOME', None)
        old_ps1   = os.environ.get('_OLD_VIRTUAL_PS1',        None)

        if old_ppath: self._deactivate += 'export PATH="%s"\n'        % old_ppath
        if old_path : self._deactivate += 'export PYTHONPATH="%s"\n'  % old_path
        if old_home : self._deactivate += 'export PYTHON_HOME="%s"\n' % old_home
        if old_ps1  : self._deactivate += 'export PS1="%s"\n'         % old_ps1

        self._deactivate += 'unset VIRTUAL_ENV\n\n'

        # FIXME: we should not alter the environment of the running agent, but
        #        only make sure that the CU finds a pristine env.  That also
        #        holds for the unsetting below -- AM
        if old_path : os.environ['PATH']        = old_path
        if old_ppath: os.environ['PYTHONPATH']  = old_ppath
        if old_home : os.environ['PYTHON_HOME'] = old_home
        if old_ps1  : os.environ['PS1']         = old_ps1

        if 'VIRTUAL_ENV' in os.environ:
            del(os.environ['VIRTUAL_ENV'])

        # simplify shell startup / prompt detection
        os.environ['PS1'] = '$ '

        self._task_launcher = rp.agent.LaunchMethod.create(
                name    = self._cfg['task_launch_method'],
                cfg     = self._cfg,
                session = self._session)

        self._mpi_launcher = rp.agent.LaunchMethod.create(
                name    = self._cfg['mpi_launch_method'],
                cfg     = self._cfg,
                session = self._session)

        # TODO: test that this actually works
        # Remove the configured set of environment variables from the
        # environment that we pass to Popen.
        for e in list(os.environ.keys()):

            env_removables = list()

            if self._mpi_launcher:
                env_removables += self._mpi_launcher.env_removables

            if self._task_launcher:
                env_removables += self._task_launcher.env_removables

            for r in  env_removables:
                if e.startswith(r):
                    os.environ.pop(e, None)

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

        # the registry keeps track of units to watch, indexed by their shell
        # spawner process ID.  As the registry is shared between the spawner and
        # watcher thread, we use a lock while accessing it.
        self._registry      = dict()
        self._registry_lock = ru.RLock()

        self._cus_to_cancel  = list()
        self._cancel_lock    = ru.RLock()

        self._cached_events = list()  # keep monitoring events for pid's which
                                      # are not yet known

        # get some threads going -- those will do all the work.
        import radical.saga.utils.pty_shell as sups

        self.launcher_shell = sups.PTYShell("fork://localhost/")
        self.monitor_shell  = sups.PTYShell("fork://localhost/")

        # run the spawner on the shells
        # tmp = tempfile.gettempdir()
        # Moving back to shared file system again, until it reaches maturity,
        # as this breaks launch methods with a hop, e.g. ssh.
        # FIXME: see #658
        self._pid    = self._cfg['pid']
        self._spawner_tmp = "/%s/%s-%s" % (self._pwd, self._pid, self.uid)

        ret, out, _  = self.launcher_shell.run_sync \
                           ("/bin/sh %s/agent/executing/shell_spawner.sh %s"
                           % (os.path.dirname (rp.__file__), self._spawner_tmp))
        if  ret != 0:
            raise RuntimeError ("launcher bootstrap failed: (%s)(%s)", ret, out)

        ret, out, _  = self.monitor_shell.run_sync \
                           ("/bin/sh %s/agent/executing/shell_spawner.sh %s"
                           % (os.path.dirname (rp.__file__), self._spawner_tmp))
        if  ret != 0:
            raise RuntimeError ("monitor bootstrap failed: (%s)(%s)", ret, out)

        # run watcher thread
        self._terminate = threading.Event()
        self._watcher   = threading.Thread(target=self._watch, name="Watcher")
        self._watcher.daemon = True
        self._watcher.start ()

        self.gtod = "%s/gtod" % self._pwd


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_units':

            self._log.info("cancel_units command (%s)" % arg)
            with self._cancel_lock:
                self._cus_to_cancel.append(arg['uids'])

        return True


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

        # check that we don't start any units which need cancelling
        if cu['uid'] in self._cus_to_cancel:

            with self._cancel_lock:
                self._cus_to_cancel.remove(cu['uid'])

            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)
            self.advance(cu, rps.CANCELED, publish=True, push=False)
            return True

        # otherwise, check if we have any active units to cancel
        # FIXME: this should probably go into a separate idle callback
        if self._cus_to_cancel:

            # NOTE: cu cancellation is costly: we keep a potentially long list
            # of cancel candidates, perform one inversion and n lookups on the
            # registry, and lock the registry for that complete time span...

            with self._registry_lock:
                # inverse registry for quick lookups:
                inv_registry = {v: k for k, v in list(self._registry.items())}

                for cu_uid in self._cus_to_cancel:
                    pid = inv_registry.get(cu_uid)
                    if pid:
                        # we own that cu, cancel it!
                        ret, out, _ = self.launcher_shell.run_sync(
                                                             'CANCEL %s\n', pid)
                        if  ret != 0:
                            self._log.error("unit cancel failed '%s': (%s)(%s)",
                                            cu_uid, ret, out)
                        # successful or not, we only try once
                        del(self._registry[pid])

                        with self._cancel_lock:
                            self._cus_to_cancel.remove(cu_uid)

            # The state advance will be managed by the watcher, which will pick
            # up the cancel notification.
            # FIXME: We could optimize a little by publishing the unschedule
            #        right here...

        try:
            if cu['description']['mpi']:
                launcher = self._mpi_launcher
            else:
                launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (mpi=%s)"
                                  % cu['description']['mpi'])

            self._log.debug("Launching unit with %s (%s).",
                            launcher.name, launcher.launch_command)

            assert(cu['slots'])

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
            if cu.get('slots'):
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

            self.advance(cu, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _cu_to_cmd (self, cu, launcher):

        env   = self._deactivate
        cwd   = ""
        pre   = ""
        post  = ""
        io    = ""
        cmd   = ""

        descr   = cu['description']
        sandbox = cu['unit_sandbox_path']

        env  += "# CU environment\n"
        env  += "export RP_SESSION_ID=%s\n"     % self._cfg['sid']
        env  += "export RP_PILOT_ID=%s\n"       % self._cfg['pid']
        env  += "export RP_AGENT_ID=%s\n"       % self._cfg['aid']
        env  += "export RP_SPAWNER_ID=%s\n"     % self.uid
        env  += "export RP_UNIT_ID=%s\n"        % cu['uid']
        env  += 'export RP_UNIT_NAME="%s"\n'    % cu['description'].get('name')
        env  += 'export RP_GTOD="%s"\n'         % cu['gtod']
        env  += 'export RP_PILOT_STAGING="%s/staging_area"\n' \
                                                % self._pwd
        if self._prof.enabled:
            env += 'export RP_PROF="%s/%s.prof"\n' % (sandbox, cu['uid'])
        env  += '''
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

        # also add any env vars requested for export by the resource config
        for k,v in self._env_cu_export.items():
            env += "export %s=%s\n" % (k,v)

        # also add any env vars requested in hte unit description
        if descr['environment']:
            for e in descr['environment']:
                env += "export %s=%s\n"  %  (e, descr['environment'][e])
        env  += "\n"

        cwd  += "# CU sandbox\n"
        cwd  += "mkdir -p %s\n" % sandbox
        cwd  += "cd       %s\n" % sandbox
        cwd  += 'prof cu_cd_done\n'
        cwd  += "\n"

        if  descr['pre_exec']:
            fail  = ' (echo "pre_exec failed"; false) || exit'
            pre  += "\n# CU pre-exec\n"
            pre  += 'prof cu_pre_start\n'
            for elem in descr['pre_exec']:
                pre += "%s || %s\n" % (elem, fail)
            pre  += "\n"
            pre  += 'prof cu_pre_stop\n'
            pre  += "\n"

        if  descr['post_exec']:
            fail  = ' (echo "post_exec failed"; false) || exit'
            post += "\n# CU post-exec\n"
            post += 'prof cu_post_start\n'
            for elem in descr['post_exec']:
                post += "%s || %s\n" % (elem, fail)
            post += 'prof cu_post_stop\n'
            post += "\n"

      # if  descr['stdin'] : io  += "<%s "  % descr['stdin']
      # else               : io  += "<%s "  % '/dev/null'
        if  descr['stdout']: io  += "1>%s " % descr['stdout']
        else               : io  += "1>%s " %       'STDOUT'
        if  descr['stderr']: io  += "2>%s " % descr['stderr']
        else               : io  += "2>%s " %       'STDERR'

        cmd, hop_cmd  = launcher.construct_command(cu,
                                        '/usr/bin/env RP_SPAWNER_HOP=TRUE "$0"')

        script  = '\n%s\n' % env
        script += 'prof cu_start\n'

        if hop_cmd :
            # the script will itself contain a remote callout which calls again
            # the script for the invokation of the real workload (cmd) -- we
            # thus introduce a guard for the first execution.  The hop_cmd MUST
            # set RP_SPAWNER_HOP to some value for the startup to work

            script += "\n# ------------------------------------------------------\n"
            script += '# perform one hop for the actual command launch\n'
            script += 'if test -z "$RP_SPAWNER_HOP"\n'
            script += 'then\n'
            script += '    %s\n' % hop_cmd
            script += '    exit\n'
            script += 'fi\n\n'

        script += "\n# ------------------------------------------------------\n"
        script += "%s"        %  cwd
        script += "%s"        %  pre
        script += "\n# CU execution\n"
        script += 'prof cu_exec_start\n'
        script += "%s %s\n\n" % (cmd, io)
        script += "RETVAL=$?\n"
        script += 'prof cu_exec_stop\n'
        script += "%s"        %  post
        script += "exit $RETVAL\n"
        script += "# ------------------------------------------------------\n\n"

      # self._log.debug ("execution script:\n%s\n", script)

        return script


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        sandbox = cu['sandbox']

        # prep stdout/err so that we can append w/o checking for None
        cu['stdout'] = ''
        cu['stderr'] = ''

        # we got an allocation: go off and launch the process.  we get
        # a multiline command, so use the wrapper's BULK/LRUN mode.
        cmd       = self._cu_to_cmd (cu, launcher)
        run_cmd   = "BULK\nLRUN\n%s\nLRUN_EOT\nBULK_RUN\n" % cmd

      # TODO: Remove this commented out block?
      # if  self.rm.target_is_macos :
      #     run_cmd = run_cmd.replace ("\\", "\\\\\\\\") # hello MacOS

        self._prof.prof('exec_start', uid=cu['uid'])
        ret, out, _ = self.launcher_shell.run_sync (run_cmd)

        if  ret != 0 :
            raise RuntimeError("failed to run unit '%s': (%s)(%s)" %
                               (run_cmd, ret, out))

        lines = [_f for _f in out.split ("\n") if _f]

        self._log.debug (lines)

        if  len (lines) < 2 :
            raise RuntimeError ("Failed to run unit (%s)", lines)

        if  lines[-2] != "OK" :
            raise RuntimeError ("Failed to run unit (%s)" % lines)

        # FIXME: verify format of returned pid (\d+)!
        pid       = lines[-1].strip ()
        cu['pid'] = pid

        # before we return, we need to clean the
        # 'BULK COMPLETED message from lrun
        ret, out = self.launcher_shell.find_prompt ()
        if  ret != 0 :
            self._prof.prof('exec_fail', uid=cu['uid'])
            raise RuntimeError ("failed to run unit '%s': (%s)(%s)"
                             % (run_cmd, ret, out))

        self._prof.prof('exec_ok', uid=cu['uid'])

        # for convenience, we link the ExecWorker job-cwd to the unit sandbox
        try:
            os.symlink("%s/%s" % (self._spawner_tmp, cu['pid']),
                       "%s/%s" % (sandbox, 'SHELL_SPAWNER_TMP'))
        except Exception as e:
            self._log.exception('shell cwd symlink failed: %s' % e)

        # FIXME: this is too late, there is already a race with the monitoring
        # thread for this CU execution.  We need to communicate the PIDs/CUs via
        # a queue again!
        with self._registry_lock :
            self._registry[pid] = cu


    # --------------------------------------------------------------------------
    #
    def _watch (self) :

        MONITOR_READ_TIMEOUT = 1.0   # check for stop signal now and then
        static_cnt           = 0

        try:

            self.monitor_shell.run_async ("MONITOR")

            while not self._terminate.is_set () :

                _, out = self.monitor_shell.find (['\n'],
                                                  timeout=MONITOR_READ_TIMEOUT)

                line = out.strip ()
              # self._log.debug ('monitor line: %s', line)

                if not line:

                    # just a read timeout, i.e. an opportunity to check for
                    # termination signals...
                    if  self._terminate.is_set() :
                        self._log.debug ("stop monitoring")
                        return

                    # ... and for health issues ...
                    if not self.monitor_shell.alive () :
                        self._log.warn ("monitoring channel died")
                        return

                    # ... and to handle cached events.
                    if not self._cached_events :
                        static_cnt += 1

                    else :
                        static_cnt += 1

                        if static_cnt == 10 :
                            # 10 times cache to check, dump it for debugging
                            static_cnt = 0

                        cache_copy          = self._cached_events[:]
                        self._cached_events = list()
                        events_to_handle    = list()
                        self._log.info ("monitoring channel checks cache (%d)",
                                        len(self._cached_events))

                        with self._registry_lock :

                            for pid, state, data in cache_copy :
                                cu = self._registry.get (pid, None)

                                if cu:
                                    events_to_handle.append(
                                                         [cu, pid, state, data])
                                else:
                                    self._cached_events.append(
                                                             [pid, state, data])

                        # FIXME: measure if using many locks in the loop below
                        # is really better than doing all ops in the locked loop
                        # above
                        for cu, pid, state, data in events_to_handle :
                            self._handle_event (cu, pid, state, data)

                    # all is well...
                  # self._log.info ("monitoring channel finish idle loop")
                    continue


                elif line == 'EXIT' or line == "Killed" :
                    self._log.error ("monitoring channel failed (%s)", line)
                    self._terminate.set()
                    return

                elif ':' not in line :
                    self._log.warn ("monitoring channel noise: %s", line)

                else :
                    elems = line.split (':', 2)
                    if len(elems) != 3:
                        raise ValueError("parse error for (%s)", line)
                    pid, state, data = elems

                    # we are not interested in non-final state information, at
                    # the moment
                    if state in ['RUNNING'] :
                        continue

                    self._log.info ("monitoring channel event: %s", line)
                    cu = None

                    with self._registry_lock :
                        cu = self._registry.get (pid, None)

                    if cu:
                        self._handle_event (cu, pid, state, data)
                    else:
                        self._cached_events.append ([pid, state, data])

        except Exception as e:

            self._log.exception("Exception in job monitoring thread: %s", e)
            self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def _handle_event (self, cu, pid, state, data) :

        # got an explicit event to handle
        self._log.info ("monitoring handles event for %s: %s:%s:%s",
                        cu['uid'], pid, state, data)

        rp_state = {'DONE'     : rps.DONE,
                    'FAILED'   : rps.FAILED,
                    'CANCELED' : rps.CANCELED}[state]

        if rp_state not in [rps.DONE, rps.FAILED, rps.CANCELED]:
            # non-final state
            self._log.debug ("ignore shell level state transition (%s:%s:%s)",
                             pid, state, data)
            return

        self._prof.prof('exec_stop', uid=cu['uid'])

        # for final states, we can free the slots.
        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

        if data : cu['exit_code'] = int(data)
        else    : cu['exit_code'] = None

        if rp_state in [rps.FAILED, rps.CANCELED] :
            # The unit failed - fail after staging output
            cu['target_state'] = rps.FAILED

        else:
            # The unit finished cleanly, see if we need to deal with
            # output data.  We always move to stageout, even if there are no
            # directives -- at the very least, we'll upload stdout/stderr
            cu['target_state'] = rps.DONE

        self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING,
                         publish=True, push=True)

        # we don't need the cu in the registry anymore
        with self._registry_lock :
            if pid in self._registry :  # why wouldn't it be in there though?
                del(self._registry[pid])


# ------------------------------------------------------------------------------


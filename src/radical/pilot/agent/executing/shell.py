
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import Queue
import tempfile
import threading
import traceback

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import AgentExecutingComponent


# ==============================================================================
#
class Shell(AgentExecutingComponent):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        AgentExecutingComponent.__init__ (self, cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        from .... import pilot as rp

        self.declare_input (rps.EXECUTING_PENDING, rpc.AGENT_EXECUTING_QUEUE)
        self.declare_worker(rps.EXECUTING_PENDING, self.work)

        self.declare_output(rps.AGENT_STAGING_OUTPUT_PENDING, rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.declare_publisher ('unschedule', rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.declare_publisher ('state',      rpc.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rpc.AGENT_COMMAND_PUBSUB)
        self.declare_subscriber('command', rpc.AGENT_COMMAND_PUBSUB, self.command_cb)

        # Mimic what virtualenv's "deactivate" would do
        self._deactivate = "# deactivate pilot virtualenv\n"

        old_path = os.environ.get('_OLD_VIRTUAL_PATH',       None)
        old_home = os.environ.get('_OLD_VIRTUAL_PYTHONHOME', None)
        old_ps1  = os.environ.get('_OLD_VIRTUAL_PS1',        None)

        if old_path: self._deactivate += 'export PATH="%s"\n'        % old_path
        if old_home: self._deactivate += 'export PYTHON_HOME="%s"\n' % old_home
        if old_ps1:  self._deactivate += 'export PS1="%s"\n'         % old_ps1

        self._deactivate += 'unset VIRTUAL_ENV\n\n'

        # FIXME: we should not alter the environment of the running agent, but
        #        only make sure that the CU finds a pristine env.  That also
        #        holds for the unsetting below -- AM
        if old_path: os.environ['PATH']        = old_path
        if old_home: os.environ['PYTHON_HOME'] = old_home
        if old_ps1:  os.environ['PS1']         = old_ps1

        if 'VIRTUAL_ENV' in os.environ :
            del(os.environ['VIRTUAL_ENV'])

        # simplify shell startup / prompt detection
        os.environ['PS1'] = '$ '

        # FIXME:
        #
        # The AgentExecutingComponent needs the LaunchMethods to construct
        # commands.  Those need the scheduler for some lookups and helper
        # methods, and the scheduler needs the LRMS.  The LRMS can in general
        # only initialized in the original agent environment -- which ultimately
        # limits our ability to place the CU execution on other nodes.
        #
        # As a temporary workaround we pass a None-Scheduler -- this will only
        # work for some launch methods, and specifically not for ORTE, DPLACE
        # and RUNJOB.
        #
        # The clean solution seems to be to make sure that, on 'allocating', the
        # scheduler derives all information needed to use the allocation and
        # attaches them to the CU, so that the launch methods don't need to look
        # them up again.  This will make the 'opaque_slots' more opaque -- but
        # that is the reason of their existence (and opaqueness) in the first
        # place...

        self._task_launcher = rp.agent.LM.create(
                name   = self._cfg['task_launch_method'],
                cfg    = self._cfg,
                logger = self._log)

        self._mpi_launcher = rp.agent.LM.create(
                name   = self._cfg['mpi_launch_method'],
                cfg    = self._cfg,
                logger = self._log)

        # TODO: test that this actually works
        # Remove the configured set of environment variables from the
        # environment that we pass to Popen.
        for e in os.environ.keys():
            env_removables = list()
            if self._mpi_launcher : env_removables += self._mpi_launcher.env_removables
            if self._task_launcher: env_removables += self._task_launcher.env_removables
            for r in  env_removables:
                if e.startswith(r):
                    os.environ.pop(e, None)

        # the registry keeps track of units to watch, indexed by their shell
        # spawner process ID.  As the registry is shared between the spawner and
        # watcher thread, we use a lock while accessing it.
        self._registry      = dict()
        self._registry_lock = threading.RLock()

        self._cus_to_cancel  = list()
        self._cancel_lock    = threading.RLock()

        self._cached_events = list() # keep monitoring events for pid's which
                                     # are not yet known

        # get some threads going -- those will do all the work.
        import saga.utils.pty_shell as sups
        self.launcher_shell = sups.PTYShell("fork://localhost/")
        self.monitor_shell  = sups.PTYShell("fork://localhost/")

        # run the spawner on the shells
        # tmp = tempfile.gettempdir()
        # Moving back to shared file system again, until it reaches maturity,
        # as this breaks launch methods with a hop, e.g. ssh.
        tmp = os.getcwd() # FIXME: see #658
        self._pilot_id    = self._cfg['pilot_id']
        self._spawner_tmp = "/%s/%s-%s" % (tmp, self._pilot_id, self._cname)

        ret, out, _  = self.launcher_shell.run_sync \
                           ("/bin/sh %s/agent/radical-pilot-spawner.sh %s" \
                           % (os.path.dirname (rp.__file__), self._spawner_tmp))
        if  ret != 0 :
            raise RuntimeError ("failed to bootstrap launcher: (%s)(%s)", ret, out)

        ret, out, _  = self.monitor_shell.run_sync \
                           ("/bin/sh %s/agent/radical-pilot-spawner.sh %s" \
                           % (os.path.dirname (rp.__file__), self._spawner_tmp))
        if  ret != 0 :
            raise RuntimeError ("failed to bootstrap monitor: (%s)(%s)", ret, out)

        # run watcher thread
        self._terminate = threading.Event()
        self._watcher   = threading.Thread(target=self._watch, name="Watcher")
        self._watcher.daemon = True
        self._watcher.start ()

        self._prof.prof('run setup done', uid=self._pilot_id)

        # communicate successful startup
        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

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
    def work(self, cu):

        # check that we don't start any units which need cancelling
        if cu['_id'] in self._cus_to_cancel:

            with self._cancel_lock:
                self._cus_to_cancel.remove(cu['_id'])

            self.publish('unschedule', cu)
            self.advance(cu, rps.CANCELED, publish=True, push=False)
            return True

        # otherwise, check if we have any active units to cancel
        # FIXME: this should probably go into a separate idle callback
        if self._cus_to_cancel:

            # NOTE: cu cancellation is costly: we keep a potentially long list
            # of cancel candidates, perform one inversion and n lookups on the
            # registry, and lock the registry for that complete time span...

            with self._registry_lock :
                # inverse registry for quick lookups:
                inv_registry = {v: k for k, v in self._registry.items()}

                for cu_uid in self._cus_to_cancel:
                    pid = inv_registry.get(cu_uid)
                    if pid:
                        # we own that cu, cancel it!
                        ret, out, _ = self.launcher_shell.run_sync ('CANCEL %s\n', pid)
                        if  ret != 0 :
                            self._log.error("failed to cancel unit '%s': (%s)(%s)", \
                                            cu_uid, ret, out)
                        # successful or not, we only try once
                        del(self._registry[pid])

                        with self._cancel_lock:
                            self._cus_to_cancel.remove(cu_uid)

            # The state advance will be managed by the watcher, which will pick
            # up the cancel notification.
            # FIXME: We could optimize a little by publishing the unschedule
            #        right here...


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
    def _cu_to_cmd (self, cu, launcher) :

        # ----------------------------------------------------------------------
        def quote_args (args) :

            ret = list()
            for arg in args :

                if not arg:
                    continue

                # if string is between outer single quotes,
                #    pass it as is.
                # if string is between outer double quotes,
                #    pass it as is.
                # otherwise (if string is not quoted)
                #    escape all double quotes

                if  arg[0] == arg[-1]  == "'" :
                    ret.append (arg)
                elif arg[0] == arg[-1] == '"' :
                    ret.append (arg)
                else :
                    arg = arg.replace ('"', '\\"')
                    ret.append ('"%s"' % arg)

            return  ret

        # ----------------------------------------------------------------------

        args  = ""
        env   = self._deactivate
        cwd   = ""
        pre   = ""
        post  = ""
        io    = ""
        cmd   = ""
        descr = cu['description']

        if  cu['workdir'] :
            cwd  += "# CU workdir\n"
            cwd  += "mkdir -p %s\n" % cu['workdir']
            # TODO: how do we align this timing with the mkdir with POPEN? (do we at all?)
            cwd  += "cd       %s\n" % cu['workdir']
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                cwd  += "echo script after_cd `%s` >> %s/PROF\n" % (cu['gtod'], cu['workdir'])
            cwd  += "\n"

        env  += "# CU environment\n"
        if descr['environment']:
            for e in descr['environment'] :
                env += "export %s=%s\n"  %  (e, descr['environment'][e])
        env  += "export RP_SESSION_ID=%s\n" % self._cfg['session_id']
        env  += "export RP_PILOT_ID=%s\n"   % self._cfg['pilot_id']
        env  += "export RP_AGENT_ID=%s\n"   % self._cfg['agent_name']
        env  += "export RP_SPAWNER_ID=%s\n" % self.cname
        env  += "export RP_UNIT_ID=%s\n"    % cu['_id']
        env  += "\n"

        if  descr['pre_exec'] :
            pre  += "# CU pre-exec\n"
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                pre  += "echo pre  start `%s` >> %s/PROF\n" % (cu['gtod'], cu['workdir'])
            pre  += '\n'.join(descr['pre_exec' ])
            pre  += "\n"
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                pre  += "echo pre  stop  `%s` >> %s/PROF\n" % (cu['gtod'], cu['workdir'])
            pre  += "\n"

        if  descr['post_exec'] :
            post += "# CU post-exec\n"
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                post += "echo post start `%s` >> %s/PROF\n" % (cu['gtod'], cu['workdir'])
            post += '\n'.join(descr['post_exec' ])
            post += "\n"
            if 'RADICAL_PILOT_PROFILE' in os.environ:
                post += "echo post stop  `%s` >> %s/PROF\n" % (cu['gtod'], cu['workdir'])
            post += "\n"

        if  descr['arguments']  :
            args  = ' ' .join (quote_args (descr['arguments']))

      # if  descr['stdin']  : io  += "<%s "  % descr['stdin']
      # else                : io  += "<%s "  % '/dev/null'
        if  descr['stdout'] : io  += "1>%s " % descr['stdout']
        else                : io  += "1>%s " %       'STDOUT'
        if  descr['stderr'] : io  += "2>%s " % descr['stderr']
        else                : io  += "2>%s " %       'STDERR'

        cmd, hop_cmd  = launcher.construct_command(cu, '/usr/bin/env RP_SPAWNER_HOP=TRUE "$0"')

        script = ''
        if 'RADICAL_PILOT_PROFILE' in os.environ:
            script += "echo script start_script `%s` >> %s/PROF\n" % (cu['gtod'], cu['workdir'])

        if hop_cmd :
            # the script will itself contain a remote callout which calls again
            # the script for the invokation of the real workload (cmd) -- we
            # thus introduce a guard for the first execution.  The hop_cmd MUST
            # set RP_SPAWNER_HOP to some value for the startup to work

            script += "# ------------------------------------------------------\n"
            script += '# perform one hop for the actual command launch\n'
            script += 'if test -z "$RP_SPAWNER_HOP"\n'
            script += 'then\n'
            script += '    %s\n' % hop_cmd
            script += '    exit\n'
            script += 'fi\n\n'

        script += "# ------------------------------------------------------\n"
        script += "%s"        %  cwd
        script += "%s"        %  env
        script += "%s"        %  pre
        script += "# CU execution\n"
        script += "%s %s\n\n" % (cmd, io)
        script += "RETVAL=$?\n"
        if 'RADICAL_PILOT_PROFILE' in os.environ:
            script += "echo script after_exec `%s` >> %s/PROF\n" % (cu['gtod'], cu['workdir'])
        script += "%s"        %  post
        script += "exit $RETVAL\n"
        script += "# ------------------------------------------------------\n\n"

      # self._log.debug ("execution script:\n%s\n" % script)

        return script


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        uid = cu['_id']

        self._prof.prof('spawn', msg='unit spawn', uid=uid)

        # we got an allocation: go off and launch the process.  we get
        # a multiline command, so use the wrapper's BULK/LRUN mode.
        cmd       = self._cu_to_cmd (cu, launcher)
        run_cmd   = "BULK\nLRUN\n%s\nLRUN_EOT\nBULK_RUN\n" % cmd

        self._prof.prof('command', msg='launch script constructed', uid=cu['_id'])

      # TODO: Remove this commented out block?
      # if  self.lrms.target_is_macos :
      #     run_cmd = run_cmd.replace ("\\", "\\\\\\\\") # hello MacOS

        ret, out, _ = self.launcher_shell.run_sync (run_cmd)

        if  ret != 0 :
            raise RuntimeError("failed to run unit '%s': (%s)(%s)" % \
                               (run_cmd, ret, out))

        lines = filter (None, out.split ("\n"))

        self._log.debug (lines)

        if  len (lines) < 2 :
            raise RuntimeError ("Failed to run unit (%s)", lines)

        if  lines[-2] != "OK" :
            raise RuntimeError ("Failed to run unit (%s)" % lines)

        # FIXME: verify format of returned pid (\d+)!
        pid           = lines[-1].strip ()
        cu['pid']     = pid
        cu['started'] = rpu.timestamp()

        # before we return, we need to clean the
        # 'BULK COMPLETED message from lrun
        ret, out = self.launcher_shell.find_prompt ()
        if  ret != 0 :
            raise RuntimeError ("failed to run unit '%s': (%s)(%s)" \
                             % (run_cmd, ret, out))

        self._prof.prof('spawn', msg='spawning passed to pty', uid=uid)

        # for convenience, we link the ExecWorker job-cwd to the unit workdir
        try:
            os.symlink("%s/%s" % (self._spawner_tmp, cu['pid']),
                       "%s/%s" % (cu['workdir'], 'SHELL_SPAWNER_TMP'))
        except Exception as e:
            self._log.exception('shell cwd symlink failed: %s' % e)

        # FIXME: this is too late, there is already a race with the monitoring
        # thread for this CU execution.  We need to communicate the PIDs/CUs via
        # a queue again!
        self._prof.prof('pass', msg="to watcher (%s)" % cu['state'], uid=cu['_id'])
        with self._registry_lock :
            self._registry[pid] = cu


    # --------------------------------------------------------------------------
    #
    def _watch (self) :

        MONITOR_READ_TIMEOUT = 1.0   # check for stop signal now and then
        static_cnt           = 0

        self._prof.prof('run', uid=self._pilot_id)
        try:

            self.monitor_shell.run_async ("MONITOR")

            while not self._terminate.is_set () :

                _, out = self.monitor_shell.find (['\n'], timeout=MONITOR_READ_TIMEOUT)

                line = out.strip ()
              # self._log.debug ('monitor line: %s' % line)

                if  not line :

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
                        self._log.info ("monitoring channel checks cache (%d)", len(self._cached_events))
                        static_cnt += 1

                        if static_cnt == 10 :
                            # 10 times cache to check, dump it for debugging
                            static_cnt = 0

                        cache_copy          = self._cached_events[:]
                        self._cached_events = list()
                        events_to_handle    = list()

                        with self._registry_lock :

                            for pid, state, data in cache_copy :
                                cu = self._registry.get (pid, None)

                                if cu : events_to_handle.append ([cu, pid, state, data])
                                else  : self._cached_events.append ([pid, state, data])

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

                elif not ':' in line :
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
                        self._prof.prof('passed', msg="ExecWatcher picked up unit",
                                state=cu['state'], uid=cu['_id'])
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
        self._log.info ("monitoring handles event for %s: %s:%s:%s", cu['_id'], pid, state, data)

        rp_state = {'DONE'     : rps.DONE,
                    'FAILED'   : rps.FAILED,
                    'CANCELED' : rps.CANCELED}[state]

        if rp_state not in [rps.DONE, rps.FAILED, rps.CANCELED]:
            # non-final state
            self._log.debug ("ignore shell level state transition (%s:%s:%s)",
                             pid, state, data)
            return

        self._prof.prof('exec', msg='execution complete', uid=cu['_id'])

        # for final states, we can free the slots.
        self.publish('unschedule', cu)

        # record timestamp, exit code on final states
        cu['finished'] = rpu.timestamp()

        if data : cu['exit_code'] = int(data)
        else    : cu['exit_code'] = None

        if rp_state in [rps.FAILED, rps.CANCELED] :
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

        # we don't need the cu in the registry anymore
        with self._registry_lock :
            if pid in self._registry :  # why wouldn't it be in there though?
                del(self._registry[pid])




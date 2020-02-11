
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import threading
import subprocess    as sp

import radical.utils as ru

from ... import states    as rps
from ... import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class ShellFS(AgentExecutingComponent):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        from .... import pilot as rp

        self._pwd = os.getcwd()
        self._tmp = self._pwd   # keep temporary files in $PWD for now (slow)

        self.register_input(rps.AGENT_EXECUTING_PENDING,
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

        if 'VIRTUAL_ENV' in os.environ :
            del(os.environ['VIRTUAL_ENV'])

        self._task_launcher = None
        self._mpi_launcher  = None

        try:
            self._task_launcher = rp.agent.LaunchMethod.create(
                    name    = self._cfg['task_launch_method'],
                    cfg     = self._cfg,
                    session = self._session)
        except:
            self._log.warn('no task launcher found')

        try:
            self._mpi_launcher = rp.agent.LaunchMethod.create(
                    name    = self._cfg['mpi_launch_method'],
                    cfg     = self._cfg,
                    session = self._session)
        except:
            self._log.warn('no mpi launcher found')

        # TODO: test that this actually works
        # Remove the configured set of environment variables from the
        # environment that we pass to Popen.
        for e in list(os.environ.keys()):
            env_removables = list()
            if self._mpi_launcher : env_removables += self._mpi_launcher.env_removables
            if self._task_launcher: env_removables += self._task_launcher.env_removables
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

        # the registry keeps track of units to watch
        self._registry      = dict()
        self._registry_lock = ru.RLock()

        self._to_cancel  = list()
        self._cancel_lock    = ru.RLock()

        self._cached_events = list()  # keep monitoring events for pid's which
                                      # are not yet known

        self.gtod = "%s/gtod" % self._pwd

        # create line buffered fifo's to communicate with the shell executor
        self._fifo_cmd_name = "%s/%s.cmd.pipe" % (self._tmp, self._uid)
        self._fifo_inf_name = "%s/%s.inf.pipe" % (self._tmp, self._uid)

        os.mkfifo(self._fifo_cmd_name)
        os.mkfifo(self._fifo_inf_name)

        self._fifo_cmd = open(self._fifo_cmd_name, 'w+', 1)
        self._fifo_inf = open(self._fifo_inf_name, 'r+', 1)

        # run thread to watch then info fifo
        self._terminate = threading.Event()
        self._watcher   = threading.Thread(target=self._watch, name="Watcher")
        self._watcher.daemon = True
        self._watcher.start ()

        # start the shell executor
        sh_exe   = "%s/shell_spawner_fs.sh" % os.path.dirname(__file__)
        sh_cmd   = "%s %s %s %s" % (sh_exe, self._pwd, self._tmp, self.uid)
                                 # script   base       work       sid
        self._log.debug('start shell executor [%s]', sh_cmd)
        self._sh = sp.Popen(sh_cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._terminate.set()

        try:
            self._sh.kill()
        except:
            pass

        try:
            os.unlink(self._fifo_cmd_name)
            os.unlink(self._fifo_inf_name)
            self._fifo_cmd.close()
            self._fifo_inf.close()
        except:
            pass


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_units':

            self._log.info("cancel_units command (%s)" % arg)
            uids = arg['uids']

            if not isinstance(uids, list):
                uids = [uids]

            with self._cancel_lock:
                for uid in uids:
                    self._fifo_cmd.write('KILL %s\n' % uid)
                    self._to_cancel.append(uid)

            self._fifo_cmd.flush()

            # The state advance will be managed by the watcher, which will pick
            # up the cancel notification.
            # FIXME: We could optimize a little by publishing the unschedule
            #        right here...

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)

        for unit in units:
            self._handle_unit(unit)

        # fail on dead watcher
        if self._terminate.is_set():
            raise RuntimeError('watcher died')


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, cu):

        # check that we don't start any units which need cancelling
        if cu['uid'] in self._to_cancel:

            with self._cancel_lock:
                self._to_cancel.remove(cu['uid'])

            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)
            self.advance(cu, rps.CANCELED, publish=True, push=False)
            return True

        # launch the new unit
        try:
            mpi = cu['description'].get('mpi', False)
            if mpi: launcher = self._mpi_launcher
            else  : launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (mpi=%s)" % mpi)

            self._log.debug("Launching with %s (%s).", launcher.name,
                            launcher.launch_command)

            assert(cu['slots'])

            self.spawn(launcher=launcher, cu=cu)


        except Exception as e:
            # append the startup error to the units stderr.  This is
            # not completely correct (as this text is not produced
            # by the unit), but it seems the most intuitive way to
            # communicate that error to the application/user.
            self._log.exception("error running CU: %s", e)

            # Free the Slots, Flee the Flots, Ree the Frots!
            if cu.get('slots'):
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

            self.advance(cu, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _cu_to_cmd (self, cu, launcher) :

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
        env  += 'export RP_GTOD="%s"\n'         % self.gtod
        if self._prof.enabled:
            env += 'export RP_PROF="%s/%s.prof"\n' % (sandbox, cu['uid'])
        env  += '''
prof(){
    test -z "$RP_PROF" && return
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
            for e in descr['environment'] :
                env += "export %s=%s\n"  %  (e, descr['environment'][e])
        env  += "\n"

        cwd  += "# CU sandbox\n"
        cwd  += "mkdir -p %s\n" % sandbox
        cwd  += "cd       %s\n" % sandbox
        cwd  += 'prof cu_cd_done\n'
        cwd  += "\n"

        if  descr['pre_exec'] :
            fail  = ' (echo "pre_exec failed"; false) || exit'
            pre  += "\n# CU pre-exec\n"
            pre  += 'prof cu_pre_start\n'
            for elem in descr['pre_exec']:
                pre += "%s || %s\n" % (elem, fail)
            pre  += "\n"
            pre  += 'prof cu_pre_stop\n'
            pre  += "\n"

        if  descr['post_exec'] :
            fail  = ' (echo "post_exec failed"; false) || exit'
            post += "\n# CU post-exec\n"
            post += 'prof cu_post_start\n'
            for elem in descr['post_exec']:
                post += "%s || %s\n" % (elem, fail)
            post += 'prof cu_post_stop\n'
            post += "\n"

        stdout_file = descr.get('stdout') or 'STDOUT'
        stderr_file = descr.get('stderr') or 'STDERR'

        cu['stdout_file'] = os.path.join(sandbox, stdout_file)
        cu['stderr_file'] = os.path.join(sandbox, stderr_file)

        io  += "1>%s " % cu['stdout_file']
        io  += "2>%s " % cu['stderr_file']

        cmd, hop_cmd  = launcher.construct_command(cu, '/usr/bin/env RP_SPAWNER_HOP=TRUE "$0"')

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
        script += "# notify the agent\n"
        script += "echo \"FINAL %s $RETVAL\" > %s\n" % (cu['uid'],
                                                        self._fifo_inf_name)
        script += "exit $RETVAL\n"
        script += "# ------------------------------------------------------\n\n"

      # self._log.debug ("execution script:\n%s\n", script)

        return script


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        uid     = cu['uid']
        sandbox = cu['sandbox']

        try:
            os.makedirs(sandbox)
        except:
            pass

        # prep stdout/err so that we can append w/o checking for None
        cu['stdout'] = ''
        cu['stderr'] = ''

        # we got an allocation: go off and launch the process.  we get
        # a multiline command, so use the wrapper's BULK/LRUN mode.
        cmd = self._cu_to_cmd (cu, launcher)
        with open("%s/%s.sh" % (sandbox, uid), 'w+') as fout:
            fout.write(cmd)

        self._prof.prof('exec_start', uid=uid)
        self._fifo_cmd.write('EXEC %s\n' % uid)
        self._fifo_cmd.flush()

        with self._registry_lock :
            self._registry[uid] = cu

        # FIXME: HERE

    # --------------------------------------------------------------------------
    #
    def _readline(self, timeout=None):

        # NOTE: this will only work for line-buffered I/O

        import select

        r, _, _ = select.select([self._fifo_inf], [], [], timeout)

        if r:
            return self._fifo_inf.readline()


    # --------------------------------------------------------------------------
    #
    def _watch (self) :

        TIMEOUT = 1.0   # check for stop signal now and then

        while not self._terminate.is_set():

            try:
                line     = self._readline(TIMEOUT)

                if not line:
                    continue

                cmd, msg = line.split(' ', 1)

                if cmd == 'FINAL':
                    self._handle_event (msg)

                elif line == 'EXIT' or line == "Killed" :
                    self._log.error ("monitoring channel failed (%s)", msg)
                    self._terminate.set()
                    return

            except Exception as e:
                self._log.exception("Exception in job monitoring thread: %s", e)
                self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def _handle_event (self, msg):

        if ' ' in msg:
            uid, ret = msg.split(' ', 1)
        else:
            uid = msg.strip()
            ret = None

        self._prof.prof('exec_stop', uid=uid)

        with self._registry_lock:
            cu = self._registry[uid]
            del(self._registry[uid])

        # free unit slots.
        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

        if ret is None:
            cu['exit_code'] = None
        else:
            cu['exit_code'] = int(ret)

        if   cu['exit_code'] == 0   : rp_state = rps.DONE
        elif cu['exit_code'] is None: rp_state = rps.CANCELED
        else                        : rp_state = rps.FAILED

        if rp_state in [rps.FAILED, rps.CANCELED] :
            # The unit failed - fail after staging output
            cu['target_state'] = rps.FAILED

        else:
            # The unit finished cleanly, see if we need to deal with
            # output data.  We always move to stageout, even if there are no
            # directives -- at the very least, we'll upload stdout/stderr
            cu['target_state'] = rps.DONE

        self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


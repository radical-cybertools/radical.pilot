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
        os.killpg(pid, signal.SIGTERM)


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

        self._cancel_lock    = ru.RLock()
        self._cus_to_cancel  = list()
        self._cus_to_watch   = list()
        self._watch_queue    = queue.Queue ()

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

        # prepare environment setup
        self._env_orig  = ru.env_read('./env.orig')
        self._env_lm    = {k:v for k,v in os.environ.items()}


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
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)

        for unit in units:
            self._handle_unit(unit)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, cu):
        # create two shell scripts: a launcher script (task.launch.sh) which
        # sets the launcher environment, performs pre_launch commands, and then
        # launches the second script which executes the task.
        #
        # The second script (`task.exec.sh`) is instantiated once per task rank.
        # It first resets the environment created by the launcher, then prepares
        # the environment for the tasks.  Next it runs the `pre_exec` directives
        # for all ranks, then the individual `pre_rank` directives are executed,
        # and then, after all ranks are synchronized, finally the task ranks
        # begin to run.
        #
        # The scripts thus show the following structure:
        #
        # Launcher Script (`task.000000.launch.sh`):
        # ----------------------------------------------------------------------
        # #!/bin/sh
        #
        # # task pre_launch commands
        # date > data.input
        #
        # # launcher environment setup
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
        # clean launch environment
        # module unload mpi
        #
        # # task environment setup (`pre_exec`)
        # module load gromacs
        #
        # # rank specific setup
        # touch task.000000.ranks
        # if test "$MPI_RANK" = 0; then
        #   export CUDA_VISIBLE_DEVICES=0
        #   export OENMP_NUM_THREADS=2
        #   export RANK_0_VAR=foo
        #   echo 0 >> task.000000.ranks
        # elif test "$MPI_RANK" = 1; then
        #   export CUDA_VISIBLE_DEVICES=1
        #   export OENMP_NUM_THREADS=4
        #   export RANK_1_VAR=bar
        #   echo 1 >> task.000000.ranks
        # fi
        #
        # # synchronize ranks
        # while $(wc -l task.000000.ranks) != $MPI_RANKS; do
        #   sleep 1
        # done
        #
        # # run the task
        # mdrun -i ... -o ... -foo ... 1> task.000000.$MPI_RANK.out \
        #                              2> task.000000.$MPI_RANK.err
        #
        # # now do the very same stuff for the `post_rank` and `post_exec`
        # # directives
        # ...
        #
        # ----------------------------------------------------------------------
        #
        # We deviate from the above in two ways:
        #
        #   - `pre_exec` directives are not actually executed for each rank,
        #     but they are executed once, the resulting env is captured and
        #     applied to all ranks.
        #   - we create one `task.000000.exec.sh` script per rank (with
        #     a `.<rank>` suffix), to make the overall flow simpler.

      # tid  = cu['uid']
      # sbox = cu['unit_sandbox_path']

        try:

          # with open('%s/%s.launch.sh' % (sbox, tid), 'w') as fout:
          #
          #     fout.write(self._get_pre_launch(cu))
          #     fout.write(self._get_launch_cmd(cu))
          #
          # with open('%s/%s.task.sh' % (sbox, tid), 'w') as fout:
          #
          #     n_ranks = len(cu['slots']['ranks'])
          #
          #     fout.write(self._get_pre_launch(cu))
          #     fout.write(self._get_launch_cmd(cu))

            descr = cu['description']

            # ensure that the named env exists
            env = descr.get('named_env')
            if env:
                if not os.path.isdir('%s/%s' % (self._pwd, env)):
                    raise ValueError('invalid named env %s for task %s'
                                    % (env, cu['uid']))
                pre = ru.as_list(descr.get('pre_exec'))
                pre.insert(0, '. %s/%s/bin/activate' % (self._pwd, env))
                pre.insert(0, '. %s/deactivate'      % (self._pwd))
                descr['pre_exec'] = pre


            # prep stdout/err so that we can append w/o checking for None
            cu['stdout'] = ''
            cu['stderr'] = ''

            cpt = descr['cpu_process_type']
          # gpt = descr['gpu_process_type']  # FIXME: use

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
        slots_fname        = '%s/%s.sl' % (sandbox, cu['uid'])

        self._log.debug("Created launch_script: %s", launch_script_name)

        # prep stdout/err so that we can append w/o checking for None
        cu['stdout'] = ''
        cu['stderr'] = ''

        with open(slots_fname, "w") as fout:
            fout.write('\n%s\n\n' % pprint.pformat(cu['slots']))

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
            env_string += 'export RP_UNIT_ID="%s"\n'       % cu['uid']
            env_string += 'export RP_UNIT_NAME="%s"\n'     % cu['description'].get('name')
            env_string += 'export RP_GTOD="%s"\n'          % self.gtod
            env_string += 'export RP_TMP="%s"\n'           % self._cu_tmp
            env_string += 'export RP_PILOT_SANDBOX="%s"\n' % self._pwd
            env_string += 'export RP_PILOT_STAGING="%s"\n' % self._pwd
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
                raise RuntimeError (msg) from e

            # also add any env vars requested in the unit description
            if descr['environment']:
                for key,val in descr['environment'].items():
                    env_string += 'export "%s=%s"\n' % (key, val)

            launch_script.write('\n# Environment variables\n%s\n' % env_string)
            launch_script.write('prof cu_start\n')
            launch_script.write('\n# Change to unit sandbox\ncd %s\n' % sandbox)

            # FIXME: cu_pre_exec should be LM specific
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
        stdout_file = descr.get('stdout') or '%s.out' % cu['uid']
        stderr_file = descr.get('stderr') or '%s.err' % cu['uid']

        cu['stdout_file'] = os.path.join(sandbox, stdout_file)
        cu['stderr_file'] = os.path.join(sandbox, stderr_file)

        _stdout_file_h = open(cu['stdout_file'], 'a')
        _stderr_file_h = open(cu['stderr_file'], 'a')

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

        # store pid for last-effort termination
        _pids.append(cu['proc'].pid)

        self._watch_queue.put(cu)


    # --------------------------------------------------------------------------
    #
    def _prep_env(self, task):
        # prepare a `<task_uid>.env` file which captures the task's execution
        # environment.  That file is to be sourced by all ranks individually
        # *after* the launch method.
        #
        # That prepared environment is based upon `env.orig` as initially
        # captured by the bootstrapper.  On top of that environment, we apply:
        #
        #   - the unit environment       (`task.description.environment`)
        #   - launch method env settings (such as `CUDA_VISIBLE_DEVICES` etc)
        #   - the unit pre_exec commands (`task.description.environment`)
        #
        # The latter (`pre_exec`) can be rather costly, and for example in the
        # case of activating virtual envs or conda envs can put significant
        # strain on shared file systems.  We thus perform the above env setup
        # once and cache the results, and then apply the env settings to all
        # future tasks with the same setup (`environment` and `pre_exec`
        # setting).  That cache is managed by the underlying RU implementation
        # of `env_prep`.
        #
        # The task sandbox exists at this point.

        tid  = task['uid']
        td   = task['description']
        sbox = task['unit_sandbox_path']
        tgt  = '%s/%s.env' % (sbox, tid)

        # we consider the `td['environment']` a part of the `pre_exec`, and
        # thus prepend the respective `export` statements
        pre_exec = ['export %s="%s"' % (k, v)
                    for k,v in td['environment'].items()]
        pre_exec += td['pre_exec']

        ru.env_prep(source=self._env_orig, pre_exec=pre_exec, target=tgt)


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


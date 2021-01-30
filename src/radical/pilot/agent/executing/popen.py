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
import threading as mt
import traceback
import subprocess

import radical.utils as ru

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

    _header    = '#!/bin/sh\n'
    _separator = '\n# -------------------------------------------------------' \
                 '-----------------------\n'

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._watcher   = None
        self._terminate = mt.Event()

        AgentExecutingComponent.__init__ (self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        AgentExecutingComponent.initialize(self)

        self._cancel_lock    = ru.RLock()
        self._cus_to_cancel  = list()
        self._cus_to_watch   = list()
        self._watch_queue    = queue.Queue ()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._watcher = mt.Thread(target=self._watch)
      # self._watcher.daemon = True
        self._watcher.start()



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
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in ru.as_list(tasks):

            try:
                self._handle_task(task)

            except Exception as e:
                # append the startup error to the task's stderr.  This is
                # not completely correct (as this text is not produced
                # by the task), but it seems the most intuitive way to
                # communicate that error to the application/user.
                self._log.exception("error running task")

                if not task.get('stderr'):
                    task['stderr'] = ''
                task['stderr'] += "\nPilot cannot start task:\n%s\n%s" \
                                % (str(e), traceback.format_exc())

                # Free the Slots, Flee the Flots, Ree the Frots!
                self._prof.prof('unschedule_start', uid=task['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                task['control'] = 'umgr_pending'
                task['$all']    = True
                self.advance(task, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task):

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
        # # rank specific setup (`pre_rank`)
        # touch task.000000.ranks
        # if test "$MPI_RANK" = 0; then
        #   export CUDA_VISIBLE_DEVICES=0
        #   export OENMP_NUM_THREADS=2
        #   export RANK_0_VAR=foo
        #   echo 0 >> task.000000.ranks
        # fi
        #
        # if test "$MPI_RANK" = 1; then
        #   export CUDA_VISIBLE_DEVICES=1
        #   export OENMP_NUM_THREADS=4
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
        # # now do the very same stuff for the `post_rank` and `post_exec`
        # # directives
        # ...
        #
        # ----------------------------------------------------------------------
        #
        # NOTE: MongoDB only accepts string keys, and thus the rank IDs in
        #       pre_rank and post_rank dictionaries are rendered as strings.
        #       This should be changed to more intuitive integers once MongoDB
        #       is phased out.

        tid   = task['uid']
        sbox  = task['unit_sandbox_path']
        td = task['description']
        cpt   = td['cpu_process_type']

        self._prof.prof('exec_mkdir', uid=tid)
        ru.rec_makedir(sbox)
        self._prof.prof('exec_mkdir_done', uid=tid)

        if cpt == 'MPI': launcher = self._mpi_launcher
        else           : launcher = self._task_launcher

        if not launcher:
            raise RuntimeError("no launcher (process type = %s)" % cpt)

        self._log.debug("Launching unit with %s (%s).",
                        launcher.name, launcher.launch_command)

        launch_script = '%s.launch.sh' % tid
        exec_script   = '%s.exec.sh'   % tid

        with open('%s/%s' % (sbox, launch_script), 'w') as fout:

            fout.write(self._header)
            fout.write(self._separator)
            fout.write('# change to task sandbox\n')
            fout.write('cd %s\n' % sbox)

            fout.write(self._separator)
            fout.write('# prepare launcher env\n')
            fout.write(self._get_launch_env(task, launcher))

            fout.write(self._separator)
            fout.write('# pre-launch commands\n')
            fout.write(self._get_pre_launch(task, launcher))

            fout.write(self._separator)
            fout.write('# launch commands\n')
            fout.write('%s\n' % self._get_launch_cmd(task, launcher, exec_script))
            fout.write('RP_RET=$?\n')

            fout.write(self._separator)
            fout.write('# post-launch commands\n')
            fout.write(self._get_post_launch(task, launcher))

            fout.write(self._separator)
            fout.write('exit $RP_RET\n')

            fout.write(self._separator)
            fout.write('\n')

        ranks   = task['slots']['ranks']
        n_ranks = len(ranks)

        with open('%s/%s' % (sbox, exec_script), 'w') as fout:

            fout.write(self._header)
            fout.write(self._separator)
            fout.write('# rank ID\n')
            fout.write(self._get_rank_ids(n_ranks, launcher))

            fout.write(self._separator)
            fout.write('# task environment\n')
            fout.write(self._get_task_env(task, launcher))

            fout.write(self._separator)
            fout.write('# pre-exec commands\n')
            fout.write(self._get_pre_exec(task))

            # pre_rank list is applied to rank 0, dict to the ranks listed
            pre_rank = td['pre_rank']
            if isinstance(pre_rank, list): pre_rank = {'0': pre_rank}

            if pre_rank:
                fout.write(self._separator)
                fout.write('# pre-rank commands\n')
                fout.write('case "$RP_RANK" in\n')
                for rank_id, cmds in pre_rank.items():
                    rank_id = int(rank_id)
                    fout.write('    %d)\n' % rank_id)
                    fout.write(self._get_pre_rank(rank_id, cmds))
                    fout.write('        ;;\n')
                fout.write('esac\n\n')

                fout.write('# sync ranks after pre-rank commands\n')
                fout.write('echo $RP_RANK >> %s.sig\n\n' % 'pre_rank')
                fout.write(self._get_rank_sync('pre_rank'))

            fout.write(self._separator)
            fout.write('# execute ranks\n')
            fout.write('case "$RP_RANK" in\n')
            for rank_id, rank in enumerate(ranks):
                fout.write('    %d)\n' % rank_id)
                fout.write(self._get_rank_exec(task, rank_id, rank, launcher))
                fout.write('        ;;\n')
            fout.write('esac\n')
            fout.write("RP_RET=$?\n")

            # post_rank list is applied to rank 0, dict to the ranks listed
            post_rank = td['post_rank']
            if isinstance(post_rank, list): post_rank = {'0': post_rank}

            if post_rank:
                fout.write(self._separator)
                fout.write('# sync ranks before post-rank commands\n')
                fout.write('echo $RP_RANK >> %s.sig\n\n' % 'post_rank')
                fout.write(self._get_rank_sync('post_rank'))

                fout.write('\n# post-rank commands\n')
                fout.write('case "$RP_RANK" in\n')
                for rank_id, cmds in post_rank.items():
                    rank_id = int(rank_id)
                    fout.write('    %d)\n' % rank_id)
                    fout.write(self._get_post_rank(rank_id, rank, cmds))
                    fout.write('        ;;\n')
                fout.write('esac\n\n')

            fout.write(self._separator)
            fout.write('# post exec commands\n')
            fout.write(self._get_post_exec(task))

            fout.write(self._separator)
            fout.write('exit $RP_RET\n')

            fout.write(self._separator)
            fout.write('\n')


      # # ensure that the named env exists
      # env = td.get('named_env')
      # if env:
      #     if not os.path.isdir('%s/%s' % (self._pwd, env)):
      #         raise ValueError('invalid named env %s for task %s'
      #                         % (env, task['uid']))
      #     pre = ru.as_list(td.get('pre_exec'))
      #     pre.insert(0, '. %s/%s/bin/activate' % (self._pwd, env))
      #     pre.insert(0, '. %s/deactivate'      % (self._pwd))
      #     td['pre_exec'] = pre

        # make sure scripts are executable
        st = os.stat('%s/%s' % (sbox, launch_script))
        st = os.stat('%s/%s' % (sbox, exec_script))
        os.chmod('%s/%s' % (sbox, launch_script), st.st_mode | stat.S_IEXEC)
        os.chmod('%s/%s' % (sbox, exec_script),   st.st_mode | stat.S_IEXEC)

        tid  = task['uid']
        td   = task['description']
        sbox = task['unit_sandbox_path']

        # make sure the sandbox exists
        slots_fname = '%s/%s.sl' % (sbox, tid)

        with open(slots_fname, "w") as fout:
            fout.write('\n%s\n\n' % pprint.pformat(task['slots']))

        # launch and exec sript are done, get ready for execution.
        cmdline = '/bin/sh %s' % launch_script

        # prepare stdout/stderr
        stdout_file = td.get('stdout') or '%s.out' % tid
        stderr_file = td.get('stderr') or '%s.err' % tid

        task['stdout'] = ''
        task['stderr'] = ''

        task['stdout_file'] = os.path.join(sbox, stdout_file)
        task['stderr_file'] = os.path.join(sbox, stderr_file)

        _stdout_file_h = open(task['stdout_file'], 'a')
        _stderr_file_h = open(task['stderr_file'], 'a')

        self._log.info("Launching unit %s via %s in %s", tid, cmdline, sbox)

        self._prof.prof('exec_start', uid=tid)
        task['proc'] = subprocess.Popen(args     = cmdline,
                                      executable = None,
                                      stdin      = None,
                                      stdout     = _stdout_file_h,
                                      stderr     = _stderr_file_h,
                                      preexec_fn = os.setsid,
                                      close_fds  = True,
                                      shell      = True,
                                      cwd        = sbox)
        self._prof.prof('exec_ok', uid=tid)

        # store pid for last-effort termination
        _pids.append(task['proc'].pid)

        self._watch_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _prep_env(self, task):

        # prepare a `<task_uid>.env` file which captures the task's execution
        # environment.  That file is to be sourced by all ranks individually
        # *after* the launch method placed them on the compute nodes
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

                self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING,
                                 publish=True,  push=True)

        return action


    # --------------------------------------------------------------------------
    #
    # launcher
    #
    def _get_launch_env(self, task, launcher):

        ret  = ''
        cmds = launcher.get_launcher_env()
        for cmd in cmds:
            ret += '%-30s || (echo "launcher env failed"; false) || exit 1\n' \
                   % cmd
        return ret


    # --------------------------------------------------------------------------
    #
    def _get_pre_launch(self, task, launcher):

        ret  = ''
        cmds = task['description']['pre_launch']
        for cmd in cmds:
            ret += '%-30s || (echo "pre_launch failed"; false) || exit 1\n' \
                   % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_launch_cmd(self, task, launcher, exec_script):

        ret = launcher.get_launch_command(task, exec_script)

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_post_launch(self, task, launcher):

        ret  = ''
        cmds = task['description']['post_launch']
        for cmd in cmds:
            ret += '%-30s || (echo "post_launch failed"; false) || exit 1\n' % cmd

        return ret


    # --------------------------------------------------------------------------
    # exec
    #
    def _get_task_env(self, task, launcher):

        tid  = task['uid']
        td   = task['description']
        name = task['name'] or tid
        sbox = task['unit_sandbox_path']

        ret  = ''
        ret += 'export RP_SESSION_ID="%s"\n'    % self._cfg['sid']
        ret += 'export RP_PILOT_ID="%s"\n'      % self._cfg['pid']
        ret += 'export RP_AGENT_ID="%s"\n'      % self._cfg['aid']
        ret += 'export RP_SPAWNER_ID="%s"\n'    % self.uid
        ret += 'export RP_UNIT_ID="%s"\n'       % tid
        ret += 'export RP_UNIT_NAME="%s"\n'     % name
        ret += 'export RP_PILOT_SANDBOX="%s"\n' % self._pwd
        ret += 'export RP_TMP="%s"\n'           % self._tmp
        ret += 'export RP_GTOD="%s"\n'          % self.gtod
        ret += 'export RP_PROF="%s"\n'          % self.prof

        if self._prof.enabled:
            ret += 'export RP_PROF_TGT="%s/%s.prof"\n' % (sbox, tid)
        else:
            ret += 'unset  RP_PROF_TGT\n'

        if 'RP_APP_TUNNEL' in os.environ:
            ret += 'export RP_APP_TUNNEL="%s"\n' \
                    % os.environ['RP_APP_TUNNEL']

        # named_env's are prepared by the launcher
        if td['named_env']:
            ret += '\n# named environment\b'
            ret += '. %s\n' % launcher.get_task_env(td['named_env'])

        # also add any env vars requested in the unit description
        if td['environment']:
            ret += '\n# task env settings\b'
            for key,val in td['environment'].items():
                ret += 'export %s="%s"\n' % (key, val)


        return ret


    # --------------------------------------------------------------------------
    #
    def _get_pre_exec(self, task):

        ret  = ''

        cmds = task['description']['pre_exec']
        for cmd in cmds:
            ret += '%-30s || (echo "pre_exec failed"; false) || exit 1\n' \
                   % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rank_ids(self, n_ranks, launcher):

        ret  = ''
        ret += 'export RP_RANKS=%s\n' % n_ranks
        ret += launcher.get_rank_cmd()

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_post_exec(self, task):

        ret  = ''
        cmds = task['description']['post_exec']
        for cmd in cmds:
            ret += '%-30s || (echo "post_exec failed"; false) || exit 1\n' \
                   % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    # rank
    #
    def _get_pre_rank(self, rank_id, cmds=[]):

        ret = ''
        for cmd in cmds:
            # FIXME: exit on error, but don't stall other ranks on sync
            ret += '        %s\n' % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rank_sync(self, sig):

        # FIXME: make sure that all ranks are alive
        ret  = 'while test $(cat %s.sig | wc -l) -lt $RP_RANKS; do\n' % sig
        ret += '    sleep 1\n'
        ret += 'done\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_rank_exec(self, task, rank_id, rank, launcher):

        # FIXME: this assumes that the rank has a `gpu_maps` and `core_maps`
        #        with exactly one entry, corresponding to the rank process to be
        #        started.

        ret  = ''
        gmap = rank['gpu_map'][0]
        if gmap:
            gpus = ','.join([str(gpu) for gpu in gmap])
            ret += '        export CUDA_VISIBLE_DEVICES=%s\n' % gpus

        cmap = rank['core_map'][0]
        ret += '        export OMP_NUM_THREADS="%d"\n' % len(cmap)

        # FIXME: core pinning goes here


        cmds = ru.as_list(launcher.get_rank_exec(task, rank_id, rank))
        for cmd in cmds:
            ret += '        %s\n' % cmd

        return ret


    # --------------------------------------------------------------------------
    #
    def _get_post_rank(self, rank_id, rank, cmds=[]):

        ret = ''
        for cmd in cmds:
            ret += '        %-30s || (echo "post_rank failed"; false) || exit 1\n' \
                   % cmd

        return ret


# ------------------------------------------------------------------------------


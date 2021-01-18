
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import errno
import queue
import tempfile
import threading
import traceback

from orte_cffi import ffi, lib as orte_lib                                # noca

import radical.utils as ru

from ....  import pilot     as rp
from ...   import states    as rps
from ...   import constants as rpc
from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
def rec_makedir(target):

    # recursive makedir which ignores errors if dir already exists

    try:
        os.makedirs(target)

    except OSError as e:
        # ignore failure on existing directory
        if e.errno == errno.EEXIST and os.path.isdir(os.path.dirname(target)):
            pass
        else:
            raise


# ------------------------------------------------------------------------------
#
@ffi.def_extern()
def launch_cb(task, jdata, status, cbdata):
    return ffi.from_handle(cbdata).task_spawned_cb(task, status)


# ------------------------------------------------------------------------------
#
@ffi.def_extern()
def finish_cb(task, jdata, status, cbdata):
    return ffi.from_handle(cbdata).task_completed_cb(task, status)


# ------------------------------------------------------------------------------
#
class ORTE(AgentExecutingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__(self, cfg, session)

        self._watcher   = None
        self._terminate = threading.Event()


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
        self._watch_queue     = queue.Queue ()

        self._pid = self._cfg['pid']

        self.task_map = {}
        self.task_map_lock = ru.Lock()

        # we needs the LaunchMethod to construct commands.
        assert(self._cfg['task_launch_method'] ==
               self._cfg['mpi_launch_method' ] == "ORTE_LIB"), \
               "ORTE_LIB spawner only works with ORTE_LIB LaunchMethod."

        self._task_launcher = rp.agent.LaunchMethod.create(name="ORTE_LIB",
                                           cfg=self._cfg, session=self._session)
        self._orte_initialized = False
        self._task_environment   = self._populate_task_environment()

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

        # TODO: verify this snippet from:
        # https://github.com/radical-cybertools/radical.pilot/pull/973/files
        # old_ppath = new_env.pop('_OLD_VIRTUAL_PYTHONPATH', None)
        # if old_ppath:
        #     new_env['PYTHONPATH'] = old_ppath

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
            if self._task_launcher:
                env_removables += self._task_launcher.env_removables
            for r in  env_removables:
                if e.startswith(r):
                    new_env.pop(e, None)

        return new_env


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

        # prep stdout/err so that we can append w/o checking for None
        task['stdout'] = ''
        task['stderr'] = ''

        if not self._orte_initialized:
            self._log.debug("ORTE not yet initialized!")
            ret = self.init_orte(task)
            if ret != 0:
                self._log.debug("ORTE initialisation failed!")
            else:
                self._log.debug("ORTE initialisation succeeded!")

        try:
            launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher")

            self._log.debug("Launching task with %s (%s).",
                            launcher.name, launcher.launch_command)

            assert(task['slots']), 'task unscheduled'
            # Start a new subprocess to launch the task
            self.spawn(launcher=launcher, task=task)

        except Exception as e:
            # append the startup error to the tasks stderr.  This is
            # not completely correct (as this text is not produced
            # by the task), but it seems the most intuitive way to
            # communicate that error to the application/user.
            self._log.exception("error running Task: %s", str(e))
            task['stderr'] += "\nPilot cannot start task:\n%s\n%s" \
                            % (str(e), traceback.format_exc())

            # Free the Slots, Flee the Flots, Ree the Frots!
            if task['slots']:
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

            self.advance(task, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def task_spawned_cb(self, task, status):

        with self.task_map_lock:
            task = self.task_map[task]
        uid = task['uid']

        if status:
            with self.task_map_lock:
                del self.task_map[task]

            # task launch failed
            self._prof.prof('exec_fail', uid=uid)
            self._log.error("task %s startup failed: %s", uid, status)
            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

            task['target_state'] = rps.FAILED
            self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                         publish=True, push=True)

        else:
            task['started'] = time.time()
            self._log.debug("task %s startup ok", uid)
            self.advance(task, rps.AGENT_EXECUTING, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def task_completed_cb(self, task, exit_code):

        timestamp = time.time()

        with self.task_map_lock:
            task = self.task_map[task]
            del self.task_map[task]

        self._prof.prof('exec_stop', uid=task['uid'])
        self._log.info("task %s finished [%s]", task['uid'], exit_code)

        task['exit_code'] = exit_code
        task['finished']  = timestamp

        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

        if exit_code != 0:
            # task failed - fail after staging output
            task['target_state'] = rps.FAILED

        else:
            # task finished cleanly.  We always move to stageout, even if there
            # are no staging directives -- at the very least, we'll upload
            # stdout/stderr
            task['target_state'] = rps.DONE

        self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                     publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def init_orte(self, task):

        # FIXME: it feels as a hack to get the DVM URI from the Task

        slots = task['slots']

        if 'lm_info' not in slots:
            raise RuntimeError('No lm_info to init via %s: %s'
                               % (self.name, slots))

        if not slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s'
                               % (self.name, slots))

        if 'dvm_uri' not in slots['lm_info']:
            raise RuntimeError('dvm_uri not in lm_info for %s: %s'
                               % (self.name, slots))

        dvm_uri = slots['lm_info']['dvm_uri']

        # Notify orte that we are using threads and that we require mutexes
        orte_lib.opal_set_using_threads(True)

        argv_keepalive = [
            ffi.new("char[]", "RADICAL-Pilot"),  # will be stripped off by lib
            ffi.new("char[]", "--hnp"), ffi.new("char[]", str(dvm_uri)),
            ffi.NULL,  # required
        ]
        argv = ffi.new("char *[]", argv_keepalive)
        ret = orte_lib.orte_submit_init(3, argv, ffi.NULL)

        self._myhandle = ffi.new_handle(self)
        self._orte_initialized = True

        return ret


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, task):

        sandbox = task['task_sandbox_path']

        if False:
            task_tmpdir = '%s/%s' % (self.tmpdir, task['uid'])
        else:
            task_tmpdir = sandbox

        rec_makedir(task_tmpdir)

        # TODO: pre_exec
        # # Before the Big Bang there was nothing
        # if task['description']['pre_exec']:
        #     fail = ' (echo "pre_exec failed"; false) || exit'
        #     pre  = ''
        #     for elem in task['description']['pre_exec']:
        #         pre += "%s || %s\n" % (elem, fail)
        #     # Note: extra spaces below are for visual alignment
        #     launch_script.write("# Pre-exec commands\n")
        #     if self._prof.enabled:
        #         launch_script.write("echo task_pre_start `%s` >> %s/%s.prof\n"\
        #                           % (task['gtod'], sandbox, task['uid']))
        #     launch_script.write(pre)
        #     if self._prof.enabled:
        #         launch_script.write("echo task_pre_stop `%s` >> %s/%s.prof\n" \
        #                           % (task['gtod'], sandbox, task['uid']))

        # TODO: post_exec
        # # After the universe dies the infrared death, there will be nothing
        # if task['description']['post_exec']:
        #     fail = ' (echo "post_exec failed"; false) || exit'
        #     post = ''
        #     for elem in task['description']['post_exec']:
        #         post += "%s || %s\n" % (elem, fail)
        #     launch_script.write("# Post-exec commands\n")
        #     if self._prof.enabled:
        #         launch_script.write("echo task_post_start `%s` >> %s/%s.prof\n"
        #                           % (task['gtod'], sandbox, task['uid']))
        #     launch_script.write('%s\n' % post)
        #     if self._prof.enabled:
        #         launch_script.write("echo task_post_stop  `%s` >> %s/%s.prof\n"
        #                           % (task['gtod'], sandbox, task['uid']))


        # The actual command line, constructed per launch-method
        try:
            orte_command, task_command = launcher.construct_command(task, None)
        except Exception as e:
            msg = "Error in spawner (%s)" % e
            self._log.exception(msg)
            raise RuntimeError(msg)

        # Construct arguments to submit_job
        arg_list = []

        # Take the orte specific commands and split them
        for arg in orte_command.split():
            arg_list.append(ffi.new("char[]", str(arg)))

        # Set the working directory
        arg_list.append(ffi.new("char[]", "--wdir"))
        arg_list.append(ffi.new("char[]", str(sandbox)))

        # Set RP environment variables
        rp_envs = [
            "RP_SESSION_ID=%s"    % self._cfg['sid'],
            "RP_PILOT_ID=%s"      % self._cfg['pid'],
            "RP_AGENT_ID=%s"      % self._cfg['aid'],
            "RP_SPAWNER_ID=%s"    % self.uid,
            "RP_TASK_ID=%s"       % task['uid'],
            "RP_TASK_NAME=%s"     % task['description'].get('name'),
            "RP_PILOT_STAGING=%s" % self._pwd
        ]
        for env in rp_envs:
            arg_list.append(ffi.new("char[]", "-x"))
            arg_list.append(ffi.new("char[]", str(env)))

        # Set pre-populated environment variables
        if self._task_environment:
            for key,val in self._task_environment.items():
                arg_list.append(ffi.new("char[]", "-x"))
                arg_list.append(ffi.new("char[]", "%s=%s" % (key, val)))

        # Set environment variables specified for this Task
        if task['description']['environment']:
            for key,val in task['description']['environment'].items():
                arg_list.append(ffi.new("char[]", "-x"))
                arg_list.append(ffi.new("char[]", "%s=%s" % (key, val)))

        # Let the orted write stdout and stderr to rank-based output files
        arg_list.append(ffi.new("char[]", "--output-filename"))
        arg_list.append(ffi.new("char[]", "%s:nojobid,nocopy" % str(sandbox)))

        # Save retval of actual Task application (in case we have post-exec)
        task_command += "; RETVAL=$?"

        # Wrap in (sub)shell for output redirection
        arg_list.append(ffi.new("char[]", "sh"))
        arg_list.append(ffi.new("char[]", "-c"))
        if self._prof.enabled:
            task_command = "echo script task_start `%s` >> %s/%s.prof; " \
                         % (self.gtod, sandbox, task['uid']) \
                         + "echo script task_exec_start `%s` >> %s/%s.prof; " \
                         % (self.gtod, sandbox, task['uid']) \
                         + task_command \
                         + "; echo script task_exec_stop `%s` >> %s/%s.prof" \
                         % (self.gtod, sandbox, task['uid'])
        arg_list.append(ffi.new("char[]", str("%s; exit $RETVAL"
                                            % str(task_command))))

        self._log.debug("Launching task %s via %s %s", task['uid'],
                        orte_command, task_command)

        # NULL termination, required by ORTE
        arg_list.append(ffi.NULL)
        argv = ffi.new("char *[]", arg_list)

        # stdout/stderr filenames can'task be set with orte
        # TODO: assert here or earlier?
        # assert task['description'].get('stdout') == None
        # assert task['description'].get('stderr') == None

        # prepare stdout/stderr
        # TODO: when mpi==True && cores>1 there will be multiple files that need
        #       to be concatenated.
        task['stdout_file'] = os.path.join(sandbox, 'rank.0/stdout')
        task['stderr_file'] = os.path.join(sandbox, 'rank.0/stderr')

        # Submit to the DVM!
        index = ffi.new("int *")
        with self.task_map_lock:

            self._prof.prof('exec_start', uid=task['uid'])
            rc = orte_lib.orte_submit_job(argv, index, orte_lib.launch_cb,
                                          self._myhandle, orte_lib.finish_cb,
                                          self._myhandle)
            if rc:
                raise Exception("submit job failed with error: %d" % rc)

            self.task_map[index[0]] = task      # map ORTE index to Task
            self._prof.prof('exec_ok', uid=task['uid'])

        self._log.debug("Task %d submitted!", task['uid'])


# ------------------------------------------------------------------------------


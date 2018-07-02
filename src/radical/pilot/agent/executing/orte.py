
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import errno
import Queue
import tempfile
import threading
import traceback

from orte_cffi import ffi, lib as orte_lib

from ....  import pilot as rp
from ...  import states    as rps
from ...  import constants as rpc
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


# ==============================================================================
#
@ffi.def_extern()
def launch_cb(task, jdata, status, cbdata):
    return ffi.from_handle(cbdata).unit_spawned_cb(task, status)


# ==============================================================================
#
@ffi.def_extern()
def finish_cb(task, jdata, status, cbdata):
    return ffi.from_handle(cbdata).unit_completed_cb(task, status)


# ==============================================================================
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
    def initialize_child(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        self._cancel_lock    = threading.RLock()
        self._cus_to_cancel  = list()
        self._watch_queue    = Queue.Queue ()

        self._pilot_id = self._cfg['pilot_id']

        self.task_map = {}
        self.task_map_lock = threading.Lock()

        # we needs the LaunchMethods to construct commands.
        assert(self._cfg['task_launch_method'] == \
               self._cfg['mpi_launch_method' ] == \
               "ORTE_LIB"), "ORTE_LIB spawner only works with ORTE_LIB LM's."

        self._task_launcher = rp.agent.LM.create(name    = "ORTE_LIB",
                                                 cfg     = self._cfg,
                                                 session = self._session)
        self._orte_initialized = False
        self._cu_environment   = self._populate_cu_environment()

        self.gtod   = "%s/gtod" % self._pwd
        self.tmpdir = tempfile.gettempdir()


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
        for e in new_env.keys():
            env_removables = list()
            if self._task_launcher:
                env_removables += self._task_launcher.env_removables
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

        # prep stdout/err so that we can append w/o checking for None
        cu['stdout'] = ''
        cu['stderr'] = ''

        if not self._orte_initialized:
            self._log.debug("ORTE not yet initialized!")
            ret = self.init_orte(cu)
            if ret != 0:
                self._log.debug("ORTE initialisation failed!")
            else:
                self._log.debug("ORTE initialisation succeeded!")

        try:
            launcher = self._task_launcher

            if not launcher:
                raise RuntimeError("no launcher (mpi=%s)" % cu['description']['mpi'])

            self._log.debug("Launching unit with %s (%s).", 
                            launcher.name, launcher.launch_command)

            assert(cu['slots']), 'unit unscheduled'
            # Start a new subprocess to launch the unit
            self.spawn(launcher=launcher, cu=cu)

        except Exception as e:
            # append the startup error to the units stderr.  This is
            # not completely correct (as this text is not produced
            # by the unit), but it seems the most intuitive way to
            # communicate that error to the application/user.
            self._log.exception("error running CU: %s", str(e))
            cu['stderr'] += "\nPilot cannot start compute unit:\n%s\n%s" \
                            % (str(e), traceback.format_exc())

            # Free the Slots, Flee the Flots, Ree the Frots!
            if cu['slots']:
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

            self.advance(cu, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def unit_spawned_cb(self, task, status):

        with self.task_map_lock:
            cu = self.task_map[task]
        uid = cu['uid']

        if status:
            with self.task_map_lock:
                del self.task_map[task]

            # unit launch failed
            self._prof.prof('exec_fail', uid=uid)
            self._log.error("unit %s startup failed: %s", uid, status)
            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

            cu['target_state'] = rps.FAILED
            self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING, 
                         publish=True, push=True)

        else:
            cu['started'] = time.time()
            self._log.debug("unit %s startup ok", uid)
            self.advance(cu, rps.AGENT_EXECUTING, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def unit_completed_cb(self, task, exit_code):

        timestamp = time.time()

        with self.task_map_lock:
            cu = self.task_map[task]
            del self.task_map[task]

        self._prof.prof('exec_stop', uid=cu['uid'])
        self._log.info("unit %s finished [%s]", cu['uid'], exit_code)

        cu['exit_code'] = exit_code
        cu['finished']  = timestamp

        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, cu)

        if exit_code != 0:
            # unit failed - fail after staging output
            cu['target_state'] = rps.FAILED

        else:
            # unit finished cleanly.  We always move to stageout, even if there 
            # are no staging directives -- at the very least, we'll upload
            # stdout/stderr
            cu['target_state'] = rps.DONE

        self.advance(cu, rps.AGENT_STAGING_OUTPUT_PENDING, 
                     publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def init_orte(self, cu):

        # FIXME: it feels as a hack to get the DVM URI from the CU

        slots = cu['slots']

        if 'lm_info' not in slots:
            raise RuntimeError('No lm_info to init via %s: %s' \
                               % (self.name, slots))

        if not slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s' \
                               % (self.name, slots))

        if 'dvm_uri' not in slots['lm_info']:
            raise RuntimeError('dvm_uri not in lm_info for %s: %s' \
                               % (self.name, slots))

        dvm_uri = slots['lm_info']['dvm_uri']

        # Notify orte that we are using threads and that we require mutexes
        orte_lib.opal_set_using_threads(True)

        argv_keepalive = [
            ffi.new("char[]", "RADICAL-Pilot"), # will be stripped off by lib
            ffi.new("char[]", "--hnp"), ffi.new("char[]", str(dvm_uri)),
            ffi.NULL, # required
        ]
        argv = ffi.new("char *[]", argv_keepalive)
        ret = orte_lib.orte_submit_init(3, argv, ffi.NULL)

        self._myhandle = ffi.new_handle(self)
        self._orte_initialized = True

        return ret

    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        # NOTE: see documentation of cu['sandbox'] semantics in the ComputeUnit
        #       class definition.
        sandbox = '%s/%s' % (self._pwd, cu['uid'])

        if False:
            cu_tmpdir = '%s/%s' % (self.tmpdir, cu['uid'])
        else:
            cu_tmpdir = sandbox

        rec_makedir(cu_tmpdir)

        # TODO: pre_exec
        # # Before the Big Bang there was nothing
        # if cu['description']['pre_exec']:
        #     fail = ' (echo "pre_exec failed"; false) || exit'
        #     pre  = ''
        #     for elem in cu['description']['pre_exec']:
        #         pre += "%s || %s\n" % (elem, fail)
        #     # Note: extra spaces below are for visual alignment
        #     launch_script.write("# Pre-exec commands\n")
        #     if 'RADICAL_PILOT_PROFILE' in os.environ:
        #         launch_script.write("echo cu_pre_start `%s` >> %s/%s.prof\n"\
        #                           % (cu['gtod'], cu_tmpdir, cu['uid']))
        #     launch_script.write(pre)
        #     if 'RADICAL_PILOT_PROFILE' in os.environ:
        #         launch_script.write("echo cu_pre_stop `%s` >> %s/%s.prof\n" \
        #                           % (cu['gtod'], cu_tmpdir, cu['uid']))

        # TODO: post_exec
        # # After the universe dies the infrared death, there will be nothing
        # if cu['description']['post_exec']:
        #     fail = ' (echo "post_exec failed"; false) || exit'
        #     post = ''
        #     for elem in cu['description']['post_exec']:
        #         post += "%s || %s\n" % (elem, fail)
        #     launch_script.write("# Post-exec commands\n")
        #     if 'RADICAL_PILOT_PROFILE' in os.environ:
        #         launch_script.write("echo cu_post_start `%s` >> %s/%s.prof\n" \
        #                           % (cu['gtod'], cu_tmpdir, cu['uid']))
        #     launch_script.write('%s\n' % post)
        #     if 'RADICAL_PILOT_PROFILE' in os.environ:
        #         launch_script.write("echo cu_post_stop  `%s` >> %s/%s.prof\n" \
        #                           % (cu['gtod'], cu_tmpdir, cu['uid']))


        # The actual command line, constructed per launch-method
        try:
            orte_command, task_command = launcher.construct_command(cu, None)
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
        arg_list.append(ffi.new("char[]", str(cu_tmpdir)))

        # Set RP environment variables
        rp_envs = [
            "RP_SESSION_ID=%s" % self._cfg['session_id'],
            "RP_PILOT_ID=%s" % self._cfg['pilot_id'],
            "RP_AGENT_ID=%s" % self._cfg['agent_name'],
            "RP_SPAWNER_ID=%s" % self.uid,
            "RP_UNIT_ID=%s" % cu['uid']
        ]
        for env in rp_envs:
            arg_list.append(ffi.new("char[]", "-x"))
            arg_list.append(ffi.new("char[]", str(env)))

        # Set pre-populated environment variables
        if self._cu_environment:
            for key,val in self._cu_environment.iteritems():
                arg_list.append(ffi.new("char[]", "-x"))
                arg_list.append(ffi.new("char[]", "%s=%s" % (key, val)))

        # Set environment variables specified for this CU
        if cu['description']['environment']:
            for key,val in cu['description']['environment'].iteritems():
                arg_list.append(ffi.new("char[]", "-x"))
                arg_list.append(ffi.new("char[]", "%s=%s" % (key, val)))

        # Let the orted write stdout and stderr to rank-based output files
        arg_list.append(ffi.new("char[]", "--output-filename"))
        arg_list.append(ffi.new("char[]", "%s:nojobid,nocopy" % str(cu_tmpdir)))

        # Save retval of actual CU application (in case we have post-exec)
        task_command += "; RETVAL=$?"

        # Wrap in (sub)shell for output redirection
        arg_list.append(ffi.new("char[]", "sh"))
        arg_list.append(ffi.new("char[]", "-c"))
        if 'RADICAL_PILOT_PROFILE' in os.environ:
            task_command = "echo script cu_start `%s` >> %s/%s.prof; " \
                         % (self.gtod, cu_tmpdir, cu['uid']) \
                         + "echo script cu_cd_done `%s` >> %s/%s.prof; " \
                         % (self.gtod, cu_tmpdir, cu['uid']) \
                         + "echo script cu_exec_start `%s` >> %s/%s.prof; " \
                         % (self.gtod, cu_tmpdir, cu['uid']) \
                         + task_command \
                         + "; echo script cu_exec_stop `%s` >> %s/%s.prof" \
                         % (self.gtod, cu_tmpdir, cu['uid'])
        arg_list.append(ffi.new("char[]", str("%s; exit $RETVAL" \
                                            % str(task_command))))

        self._log.debug("Launching unit %s via %s %s", cu['uid'], 
                        orte_command, task_command)

        # NULL termination, required by ORTE
        arg_list.append(ffi.NULL)
        argv = ffi.new("char *[]", arg_list)

        # stdout/stderr filenames can't be set with orte
        # TODO: assert here or earlier?
        # assert cu['description'].get('stdout') == None
        # assert cu['description'].get('stderr') == None

        # prepare stdout/stderr
        # TODO: when mpi==True && cores>1 there will be multiple files that need
        #       to be concatenated.
        cu['stdout_file'] = os.path.join(cu_tmpdir, 'rank.0/stdout')
        cu['stderr_file'] = os.path.join(cu_tmpdir, 'rank.0/stderr')

        # Submit to the DVM!
        index = ffi.new("int *")
        with self.task_map_lock:

            self._prof.prof('exec_start', uid=cu['uid'])
            rc = orte_lib.orte_submit_job(argv, index, orte_lib.launch_cb, 
                                          self._myhandle, orte_lib.finish_cb, 
                                          self._myhandle)
            if rc:
                raise Exception("submit job failed with error: %d" % rc)

            self.task_map[index[0]] = cu      # map ORTE index to CU
            self._prof.prof('exec_ok', uid=cu['uid'])

        self._log.debug("Task %d submitted!", cu['uid'])


# ------------------------------------------------------------------------------


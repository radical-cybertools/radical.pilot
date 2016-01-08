#!/usr/bin/env python

"""
.. module:: radical.pilot.agent
   :platform: Unix
   :synopsis: The agent for RADICAL-Pilot.

   The agent gets CUs by means of the MongoDB.
   The execution of CUs by the Agent is (primarily) configured by the
   triplet (LRMS, LAUNCH_METHOD(s), SCHEDULER):
   - The LRMS detects and structures the information about the resources
     available to agent.
   - The Scheduler maps the execution requests of the LaunchMethods to a
     subset of the resources available to the Agent.
     It does not deal with the "presentation" of this subset.
   - The LaunchMethods configure how to execute (regular and MPI) tasks,
     and know about the specific format to specify the subset of resources.


   Structure:
   ----------
   This represents the planned architecture, which is not fully represented in
   code, yet.

     - class Agent
       - represents the whole thing
       - has a set of StageinWorkers  (threads or procs)
       - has a set of StageoutWorkers (threads or procs)
       - has a set of ExecWorkers     (threads or procs)
       - has a set of UpdateWorkers   (threads or procs)
       - has a HeartbeatMonitor       (threads or procs)
       - has a inputstaging  queue
       - has a outputstaging queue
       - has a execution queue
       - has a update queue
       - loops forever
       - in each iteration
         - pulls CU bulks from DB
         - pushes CUs into inputstaging queue or execution queue (based on
           obvious metric)

     class StageinWorker
       - competes for CU input staging requests from inputstaging queue
       - for each received CU
         - performs staging
         - pushes CU into execution queue
         - pushes stage change notification request into update queue

     class StageoutWorker
       - competes for CU output staging requests from outputstaging queue
       - for each received CU
         - performs staging
         - pushes stage change notification request into update queue

     class ExecWorker
       - manages a partition of the allocated cores
         (partition size == max cu size)
       - competes for CU execution reqeusts from execute queue
       - for each CU
         - prepares execution command
         - pushes command to ExecutionEnvironment
         - pushes stage change notification request into update queue

     class Spawner
       - executes CUs according to ExecWorker instruction
       - monitors CU execution (for completion)
       - gets CU execution reqeusts from ExecWorker
       - for each CU
         - executes CU command
         - monitors CU execution
         - on CU completion
           - pushes CU to outputstaging queue (if staging is needed)
           - pushes stage change notification request into update queue

     class UpdateWorker
       - competes for CU state update reqeusts from update queue
       - for each CU
         - pushes state update (collected into bulks if possible)
         - cleans CU workdir if CU is final and cleanup is requested

     Agent
       |
       +--------------------------------------------------------
       |           |              |              |             |
       |           |              |              |             |
       V           V              V              V             V
     ExecWorker* StageinWorker* StageoutWorker* UpdateWorker* HeartbeatMonitor
       |
       +-------------------------------------------------
       |     |               |                |         |
       |     |               |                |         |
       V     V               V                V         V
     LRMS  MPILaunchMethod TaskLaunchMethod Scheduler Spawner


    NOTE:
    -----
      - Units are progressing through the different worker threads, where, in
        general, the unit changes state when transitioning to the next thread.
        The unit ownership thus *defines* the unit state (its owned by the
        InputStagingWorker, it is in StagingInput state, etc), and the state
        update notifications to the DB are merely informational (and can thus be
        asynchron).  The updates need to be ordered though, to reflect valid and
        correct state transition history.


    TODO:
    -----

    - add option to scheduler to ignore core 0 (which hosts the agent process)
    - add LRMS.partition (n) to return a set of partitioned LRMS for partial
      ExecWorkers
    - publish pilot slot history once on shutdown?  Or once in a while when
      idle?  Or push continuously?
    - Schedulers, LRMSs, LaunchMethods, etc need to be made threadsafe, for the
      case where more than one execution worker threads are running.
    - move util functions to rp.utils or r.utils, and pull the from there
    - split the agent into logical components (classes?), and install along with
      RP.
    - add state asserts after `queue.get ()`
    - move mkdir etc from ingest thread to where its used (input staging or
      execution)
    - the structure of the base scheduler should be suitable for both, UMGR
      scheduling and Agent scheduling.  The algs will be different though,
      mostly because the pilots (as targets of the umgr scheduler) have a wait
      queue, but the cores (targets of the agent scheduler) have not.  Is it
      worthwhile to re-use the structure anyway?
    - all stop() method calls need to be replaced with commands which travel
      through the queues.  To deliver commands timely though we either need
      command prioritization (difficult), or need separate command queues...

"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import copy
import math
import stat
import sys
import time
import errno
import Queue
import pprint
import signal
import shutil
import hostlist
import tempfile
import netifaces
import fractions
import threading
import traceback
import subprocess
import collections
import multiprocessing
import json
import urllib2 as ul

import saga                as rs
import radical.utils       as ru
import radical.pilot       as rp
import radical.pilot.utils as rpu



# ------------------------------------------------------------------------------
#
# http://stackoverflow.com/questions/9539052/python-dynamically-changing-base-classes-at-runtime-how-to
#
# Depending on agent architecture (which is specific to the resource type it
# runs on) can switch between different component types: using threaded (when
# running on the same node), multiprocessing (also for running on the same node,
# but avoiding python's threading problems, for the prices of slower queues),
# and remote processes (for running components on different nodes, using zeromq
# queues for communication).
#
# We do some trickery to keep the actual components independent from the actual
# schema:
#
#   - we wrap the different queue types into a rpu.Queue object
#   - we change the base class of the component dynamically to the respective type
#
# This requires components to adhere to the following restrictions:
#
#   - *only* communicate over queues -- no shared data with other components or
#     component instances.  Note that this also holds for example for the
#     scheduler!
#   - no shared data between the component class and it's run() method.  That
#     includes no sharing of queues.
#   - components inherit from base_component, and the constructor needs to
#     register all required component-internal and -external queues with that
#     base class -- the run() method can then transparently retrieve them from
#     there.
#

# this needs git attribute 'ident' set for this file
git_ident = "$Id$"


# ------------------------------------------------------------------------------
# CONSTANTS
#
# 'enum' for pilot's unit spawner types
SPAWNER_NAME_POPEN          = "POPEN"
SPAWNER_NAME_SHELL          = "SHELL"
SPAWNER_NAME_ABDS           = "ABDS"

# defines for pilot commands
COMMAND_CANCEL_PILOT        = "Cancel_Pilot"
COMMAND_CANCEL_COMPUTE_UNIT = "Cancel_Compute_Unit"
COMMAND_KEEP_ALIVE          = "Keep_Alive"
COMMAND_FIELD               = "commands"
COMMAND_TYPE                = "type"
COMMAND_ARG                 = "arg"
COMMAND_CANCEL              = "Cancel"
COMMAND_SCHEDULE            = "schedule"
COMMAND_RESCHEDULE          = "reschedule"
COMMAND_UNSCHEDULE          = "unschedule"
COMMAND_WAKEUP              = "wakeup"


# 'enum' for staging action operators
COPY     = 'Copy'     # local cp
LINK     = 'Link'     # local ln -s
MOVE     = 'Move'     # local mv
TRANSFER = 'Transfer' # saga remote transfer
                      # TODO: This might just be a special case of copy

# tri-state for unit spawn retval
OK       = 'OK'
FAIL     = 'FAIL'
RETRY    = 'RETRY'

# ----------------------------------------------------------------------------------
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
def pilot_FAILED(mongo_p=None, pilot_uid=None, logger=None, msg=None):

    if logger:
        logger.error(msg)
        logger.error(ru.get_trace())

    print msg
    print ru.get_trace()

    if mongo_p and pilot_uid:

        now = rpu.timestamp()
        out = None
        err = None
        log = None

        try    : out = open('./agent.out', 'r').read()
        except : pass
        try    : err = open('./agent.err', 'r').read()
        except : pass
        try    : log = open('./agent.log', 'r').read()
        except : pass

        msg = [{"message": msg,              "timestamp": now},
               {"message": rpu.get_rusage(), "timestamp": now}]

        mongo_p.update({"_id": pilot_uid},
            {"$pushAll": {"log"         : msg},
             "$push"   : {"statehistory": {"state"     : rp.FAILED,
                                           "timestamp" : now}},
             "$set"    : {"state"       : rp.FAILED,
                          "stdout"      : rpu.tail(out),
                          "stderr"      : rpu.tail(err),
                          "logfile"     : rpu.tail(log),
                          "finished"    : now}
            })

    else:
        if logger:
            logger.error("cannot log error state in database!")

        print "cannot log error state in database!"


# ------------------------------------------------------------------------------
#
def pilot_CANCELED(mongo_p=None, pilot_uid=None, logger=None, msg=None):

    if logger:
        logger.warning(msg)

    print msg

    if mongo_p and pilot_uid:

        now = rpu.timestamp()
        out = None
        err = None
        log = None

        try    : out = open('./agent.out', 'r').read()
        except : pass
        try    : err = open('./agent.err', 'r').read()
        except : pass
        try    : log = open('./agent.log',    'r').read()
        except : pass

        msg = [{"message": msg,              "timestamp": now},
               {"message": rpu.get_rusage(), "timestamp": now}]

        mongo_p.update({"_id": pilot_uid},
            {"$pushAll": {"log"         : msg},
             "$push"   : {"statehistory": {"state"     : rp.CANCELED,
                                           "timestamp" : now}},
             "$set"    : {"state"       : rp.CANCELED,
                          "stdout"      : rpu.tail(out),
                          "stderr"      : rpu.tail(err),
                          "logfile"     : rpu.tail(log),
                          "finished"    : now}
            })

    else:
        if logger:
            logger.error("cannot log cancel state in database!")

        print "cannot log cancel state in database!"


# ------------------------------------------------------------------------------
#
def pilot_DONE(mongo_p=None, pilot_uid=None, logger=None, msg=None):

    if mongo_p and pilot_uid:

        now = rpu.timestamp()
        out = None
        err = None
        log = None

        try    : out = open('./agent.out', 'r').read()
        except : pass
        try    : err = open('./agent.err', 'r').read()
        except : pass
        try    : log = open('./agent.log',    'r').read()
        except : pass

        msg = [{"message": "pilot done",     "timestamp": now},
               {"message": rpu.get_rusage(), "timestamp": now}]

        mongo_p.update({"_id": pilot_uid},
            {"$pushAll": {"log"         : msg},
             "$push"   : {"statehistory": {"state"    : rp.DONE,
                                           "timestamp": now}},
             "$set"    : {"state"       : rp.DONE,
                          "stdout"      : rpu.tail(out),
                          "stderr"      : rpu.tail(err),
                          "logfile"     : rpu.tail(log),
                          "finished"    : now}
            })

    else:
        if logger:
            logger.error("cannot log cancel state in database!")

        print "cannot log cancel state in database!"




# ==============================================================================
#
# Worker Classes
#
# ==============================================================================
#
class AgentExecutingComponent(rpu.Component):
    """
    Manage the creation of CU processes, and watch them until they are completed
    (one way or the other).  The spawner thus moves the unit from
    PendingExecution to Executing, and then to a final state (or PendingStageOut
    of course).
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.Component.__init__(self, 'AgentExecutingComponent', cfg)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Spawner
    #
    @classmethod
    def create(cls, cfg):

        # Make sure that we are the base-class!
        if cls != AgentExecutingComponent:
            raise TypeError("Factory only available to base class!")

        name   = cfg['spawner']

        try:
            impl = {
                SPAWNER_NAME_POPEN : AgentExecutingComponent_POPEN,
                SPAWNER_NAME_SHELL : AgentExecutingComponent_SHELL,
                SPAWNER_NAME_ABDS  : AgentExecutingComponent_ABDS
            }[name]

            impl = impl(cfg)
            return impl

        except KeyError:
            raise ValueError("AgentExecutingComponent '%s' unknown or defunct" % name)



# ==============================================================================
#
class AgentExecutingComponent_POPEN (AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        AgentExecutingComponent.__init__ (self, cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

      # self.declare_input (rp.AGENT_EXECUTING_PENDING, rp.AGENT_EXECUTING_QUEUE)
      # self.declare_worker(rp.AGENT_EXECUTING_PENDING, self.work)

        self.declare_input (rp.EXECUTING_PENDING, rp.AGENT_EXECUTING_QUEUE)
        self.declare_worker(rp.EXECUTING_PENDING, self.work)

        self.declare_output(rp.AGENT_STAGING_OUTPUT_PENDING, rp.AGENT_STAGING_OUTPUT_QUEUE)

        self.declare_publisher ('unschedule', rp.AGENT_UNSCHEDULE_PUBSUB)
        self.declare_publisher ('state',      rp.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rp.AGENT_COMMAND_PUBSUB)
        self.declare_subscriber('command', rp.AGENT_COMMAND_PUBSUB, self.command_cb)

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

      # self.advance(cu, rp.AGENT_EXECUTING, publish=True, push=False)
        self.advance(cu, rp.EXECUTING, publish=True, push=False)

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

            self.advance(cu, rp.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        self._prof.prof('spawn', msg='unit spawn', uid=cu['_id'])

        if False:
            cu_tmpdir = '%s/%s' % (self.tmpdir, cu['_id'])
        else:
            cu_tmpdir = cu['workdir']

        rec_makedir(cu_tmpdir)
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
                    self.advance(cu, rp.CANCELED, publish=True, push=False)

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
                    cu['target_state'] = rp.FAILED

                else:
                    # The unit finished cleanly, see if we need to deal with
                    # output data.  We always move to stageout, even if there are no
                    # directives -- at the very least, we'll upload stdout/stderr
                    self._prof.prof('final', msg="execution succeeded", uid=cu['_id'])
                    cu['target_state'] = rp.DONE

                self.advance(cu, rp.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        return action


# ==============================================================================
#
class AgentExecutingComponent_SHELL(AgentExecutingComponent):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        AgentExecutingComponent.__init__ (self, cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self.declare_input (rp.EXECUTING_PENDING, rp.AGENT_EXECUTING_QUEUE)
        self.declare_worker(rp.EXECUTING_PENDING, self.work)

        self.declare_output(rp.AGENT_STAGING_OUTPUT_PENDING, rp.AGENT_STAGING_OUTPUT_QUEUE)

        self.declare_publisher ('unschedule', rp.AGENT_UNSCHEDULE_PUBSUB)
        self.declare_publisher ('state',      rp.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rp.AGENT_COMMAND_PUBSUB)
        self.declare_subscriber('command', rp.AGENT_COMMAND_PUBSUB, self.command_cb)

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
            self.advance(cu, rp.CANCELED, publish=True, push=False)
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
                        ret, out, _ = self.launcher_shell.run_sync ('CANCEL %s\n' % pid)
                        if  ret != 0 :
                            self._log.error ("failed to cancel unit '%s': (%s)(%s)" \
                                            , (cu_uid, ret, out))
                        # successful or not, we only try once
                        del(self._registry[pid])

                        with self._cancel_lock:
                            self._cus_to_cancel.remove(cu_uid)

            # The state advance will be managed by the watcher, which will pick
            # up the cancel notification.
            # FIXME: We could optimize a little by publishing the unschedule
            #        right here...


      # self.advance(cu, rp.AGENT_EXECUTING, publish=True, push=False)
        self.advance(cu, rp.EXECUTING, publish=True, push=False)

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

            self.advance(cu, rp.FAILED, publish=True, push=False)


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
            self._log.error ("failed to run unit '%s': (%s)(%s)" \
                            , (run_cmd, ret, out))
            return FAIL

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

        rp_state = {'DONE'     : rp.DONE,
                    'FAILED'   : rp.FAILED,
                    'CANCELED' : rp.CANCELED}.get (state, rp.UNKNOWN)

        if rp_state not in [rp.DONE, rp.FAILED, rp.CANCELED] :
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

        if rp_state in [rp.FAILED, rp.CANCELED] :
            # The unit failed - fail after staging output
            self._prof.prof('final', msg="execution failed", uid=cu['_id'])
            cu['target_state'] = rp.FAILED

        else:
            # The unit finished cleanly, see if we need to deal with
            # output data.  We always move to stageout, even if there are no
            # directives -- at the very least, we'll upload stdout/stderr
            self._prof.prof('final', msg="execution succeeded", uid=cu['_id'])
            cu['target_state'] = rp.DONE

        self.advance(cu, rp.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        # we don't need the cu in the registry anymore
        with self._registry_lock :
            if pid in self._registry :  # why wouldn't it be in there though?
                del(self._registry[pid])


# ==============================================================================
#
class AgentExecutingComponent_ABDS (AgentExecutingComponent) :

    # The name is rong based on the abstraction, but for the moment I do not
    # have any other ideas

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        AgentExecutingComponent.__init__ (self, cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

      # self.declare_input (rp.AGENT_EXECUTING_PENDING, rp.AGENT_EXECUTING_QUEUE)
      # self.declare_worker(rp.AGENT_EXECUTING_PENDING, self.work)

        self.declare_input (rp.EXECUTING_PENDING, rp.AGENT_EXECUTING_QUEUE)
        self.declare_worker(rp.EXECUTING_PENDING, self.work)

        self.declare_output(rp.AGENT_STAGING_OUTPUT_PENDING, rp.AGENT_STAGING_OUTPUT_QUEUE)

        self.declare_publisher ('unschedule', rp.AGENT_UNSCHEDULE_PUBSUB)
        self.declare_publisher ('state',      rp.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rp.AGENT_COMMAND_PUBSUB)
        self.declare_subscriber('command', rp.AGENT_COMMAND_PUBSUB, self.command_cb)

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
                name   = self._cfg['task_launch_method'],
                cfg    = self._cfg,
                logger = self._log)

        self._mpi_launcher = rp.agent.LM.create(
                name   = self._cfg['mpi_launch_method'],
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

        elif cmd == 'shutdown':
            self._log.info('received shutdown command')
            self.stop()


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

        self.advance(cu, rp.ALLOCATING, publish=True, push=False)


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

            self.advance(cu, rp.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, cu):

        self._prof.prof('spawn', msg='unit spawn', uid=cu['_id'])

        if False:
            cu_tmpdir = '%s/%s' % (self.tmpdir, cu['_id'])
        else:
            cu_tmpdir = cu['workdir']

        rec_makedir(cu_tmpdir)
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

            # YARN pre execution folder permission change
            launch_script.write('\n## Changing Working Directory permissions for YARN\n')
            launch_script.write('old_perm="`stat -c %a .`"\n')
            launch_script.write('chmod -R 777 .\n')

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
                self._log.debug("Launch Script Name %s",launch_script_name)
                launch_command, hop_cmd = launcher.construct_command(cu, launch_script_name)
                self._log.debug("Launch Command %s from %s",(launch_command,launcher.name))

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

            # YARN pre execution folder permission change
            launch_script.write('\n## Changing Working Directory permissions for YARN\n')
            launch_script.write('chmod $old_perm .\n')

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

        cname = self.name.replace('Component', 'Watcher')
        self._prof = rpu.Profiler(cname)
        self._prof.prof('run', uid=self._pilot_id)
        try:
            self._log = ru.get_logger(cname, target="%s.log" % cname,
                                      level='DEBUG') # FIXME?

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

        self._prof.prof('stop', uid=self._pilot_id)
        self._prof.flush()


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self):

        action = 0

        for cu in self._cus_to_watch:
            #-------------------------------------------------------------------
            # This code snippet reads the YARN application report file and if
            # the application is RUNNING it update the state of the CU with the
            # right time stamp. In any other case it works as it was.
            if cu['state']==rp.ALLOCATING \
               and os.path.isfile(cu['workdir']+'/YarnApplicationReport.log'):

                yarnreport=open(cu['workdir']+'/YarnApplicationReport.log','r')
                report_contents = yarnreport.readlines()
                yarnreport.close()

                for report_line in report_contents:
                    if report_line.find('RUNNING') != -1:
                        self._log.debug(report_contents)
                        line = report_line.split(',')
                        timestamp = (int(line[3].split('=')[1])/1000)
                        action += 1
                        proc = cu['proc']
                        self._log.debug('Proc Print {0}'.format(proc))
                        del(cu['proc'])  # proc is not json serializable
                        self.advance(cu, rp.EXECUTING, publish=True, push=False,timestamp=timestamp)
                        cu['proc']    = proc

                        # FIXME: Ioannis, what is this supposed to do?
                        # I wanted to update the state of the cu but keep it in the watching
                        # queue. I am not sure it is needed anymore.
                        index = self._cus_to_watch.index(cu)
                        self._cus_to_watch[index]=cu

            else :
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

                        self._cus_to_watch.remove(cu)

                        del(cu['proc'])  # proc is not json serializable
                        self.publish('unschedule', cu)
                        self.advance(cu, rp.CANCELED, publish=True, push=False)

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

                    if os.path.isfile("%s/PROF" % cu['workdir']):
                        with open("%s/PROF" % cu['workdir'], 'r') as prof_f:
                            try:
                                txt = prof_f.read()
                                for line in txt.split("\n"):
                                    if line:
                                        x1, x2, x3 = line.split()
                                        self._prof.prof(x1, msg=x2, timestamp=float(x3), uid=cu['_id'])
                            except Exception as e:
                                self._log.error("Pre/Post profiling file read failed: `%s`" % e)

                    if exit_code != 0:
                        # The unit failed - fail after staging output
                        self._prof.prof('final', msg="execution failed", uid=cu['_id'])
                        cu['target_state'] = rp.FAILED

                    else:
                        # The unit finished cleanly, see if we need to deal with
                        # output data.  We always move to stageout, even if there are no
                        # directives -- at the very least, we'll upload stdout/stderr
                        self._prof.prof('final', msg="execution succeeded", uid=cu['_id'])
                        cu['target_state'] = rp.DONE

                    self.advance(cu, rp.AGENT_STAGING_OUTPUT_PENDING, publish=True, push=True)

        return action


# ==============================================================================
#
class AgentStagingInputComponent(rpu.Component):
    """
    This component performs all agent side input staging directives for compute
    units.  It gets units from the agent_staging_input_queue, in
    AGENT_STAGING_INPUT_PENDING state, will advance them to AGENT_STAGING_INPUT
    state while performing the staging, and then moves then to the
    AGENT_SCHEDULING_PENDING state, into the agent_scheduling_queue.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.Component.__init__(self, 'AgentStagingInputComponent', cfg)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg):

        return cls(cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self.declare_input (rp.AGENT_STAGING_INPUT_PENDING, rp.AGENT_STAGING_INPUT_QUEUE)
        self.declare_worker(rp.AGENT_STAGING_INPUT_PENDING, self.work)

        self.declare_output(rp.ALLOCATING_PENDING, rp.AGENT_SCHEDULING_QUEUE)

        self.declare_publisher('state', rp.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rp.AGENT_COMMAND_PUBSUB)

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
    def work(self, cu):

        self.advance(cu, rp.AGENT_STAGING_INPUT, publish=True, push=False)
        self._log.info('handle %s' % cu['_id'])

        workdir      = os.path.join(self._cfg['workdir'], '%s' % cu['_id'])
        gtod         = os.path.join(self._cfg['workdir'], 'gtod')
        staging_area = os.path.join(self._cfg['workdir'], self._cfg['staging_area'])
        staging_ok   = True

        cu['workdir']     = workdir
        cu['stdout']      = ''
        cu['stderr']      = ''
        cu['opaque_clot'] = None
        # TODO: See if there is a more central place to put this
        cu['gtod']        = gtod

        stdout_file       = cu['description'].get('stdout')
        stdout_file       = stdout_file if stdout_file else 'STDOUT'
        stderr_file       = cu['description'].get('stderr')
        stderr_file       = stderr_file if stderr_file else 'STDERR'

        cu['stdout_file'] = os.path.join(workdir, stdout_file)
        cu['stderr_file'] = os.path.join(workdir, stderr_file)

        # create unit workdir
        rec_makedir(workdir)
        self._prof.prof('unit mkdir', uid=cu['_id'])

        try:
            for directive in cu['Agent_Input_Directives']:

                self._prof.prof('Agent input_staging queue', uid=cu['_id'],
                         msg="%s -> %s" % (str(directive['source']), str(directive['target'])))

                # Perform input staging
                self._log.info("unit input staging directives %s for cu: %s to %s",
                               directive, cu['_id'], workdir)

                # Convert the source_url into a SAGA Url object
                source_url = rs.Url(directive['source'])

                # Handle special 'staging' scheme
                if source_url.scheme == self._cfg['staging_scheme']:
                    self._log.info('Operating from staging')
                    # Remove the leading slash to get a relative path from the staging area
                    rel2staging = source_url.path.split('/',1)[1]
                    source = os.path.join(staging_area, rel2staging)
                else:
                    self._log.info('Operating from absolute path')
                    source = source_url.path

                # Get the target from the directive and convert it to the location
                # in the workdir
                target = directive['target']
                abs_target = os.path.join(workdir, target)

                # Create output directory in case it doesn't exist yet
                rec_makedir(os.path.dirname(abs_target))

                self._log.info("Going to '%s' %s to %s", directive['action'], source, abs_target)

                if   directive['action'] == LINK: os.symlink     (source, abs_target)
                elif directive['action'] == COPY: shutil.copyfile(source, abs_target)
                elif directive['action'] == MOVE: shutil.move    (source, abs_target)
                else:
                    # FIXME: implement TRANSFER mode
                    raise NotImplementedError('Action %s not supported' % directive['action'])

                log_message = "%s'ed %s to %s - success" % (directive['action'], source, abs_target)
                self._log.info(log_message)

        except Exception as e:
            self._log.exception("staging input failed -> unit failed")
            staging_ok = False


        # Agent input staging is done (or failed)
        if staging_ok:
          # self.advance(cu, rp.AGENT_SCHEDULING_PENDING, publish=True, push=True)
            self.advance(cu, rp.ALLOCATING_PENDING, publish=True, push=True)
        else:
            self.advance(cu, rp.FAILED, publish=True, push=False)


# ==============================================================================
#
class AgentStagingOutputComponent(rpu.Component):
    """
    This component performs all agent side output staging directives for compute
    units.  It gets units from the agent_staging_output_queue, in
    AGENT_STAGING_OUTPUT_PENDING state, will advance them to
    AGENT_STAGING_OUTPUT state while performing the staging, and then moves then
    to the UMGR_STAGING_OUTPUT_PENDING state, which at the moment requires the
    state change to be published to MongoDB (no push into a queue).

    Note that this component also collects stdout/stderr of the units (which
    can also be considered staging, really).
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.Component.__init__(self, 'AgentStagingOutputComponent', cfg)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg):

        return cls(cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self.declare_input (rp.AGENT_STAGING_OUTPUT_PENDING, rp.AGENT_STAGING_OUTPUT_QUEUE)
        self.declare_worker(rp.AGENT_STAGING_OUTPUT_PENDING, self.work)

        # we don't need an output queue -- units are picked up via mongodb
        self.declare_output(rp.PENDING_OUTPUT_STAGING, None) # drop units

        self.declare_publisher('state', rp.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rp.AGENT_COMMAND_PUBSUB)

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
    def work(self, cu):

        self.advance(cu, rp.AGENT_STAGING_OUTPUT, publish=True, push=False)

        staging_area = os.path.join(self._cfg['workdir'], self._cfg['staging_area'])
        staging_ok   = True

        workdir = cu['workdir']

        ## parked from unit state checker: unit postprocessing
        if os.path.isfile(cu['stdout_file']):
            with open(cu['stdout_file'], 'r') as stdout_f:
                try:
                    txt = unicode(stdout_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stdout contains binary data -- use file staging directives"

                cu['stdout'] += rpu.tail(txt)

        if os.path.isfile(cu['stderr_file']):
            with open(cu['stderr_file'], 'r') as stderr_f:
                try:
                    txt = unicode(stderr_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stderr contains binary data -- use file staging directives"

                cu['stderr'] += rpu.tail(txt)

        if 'RADICAL_PILOT_PROFILE' in os.environ:
            if os.path.isfile("%s/PROF" % cu['workdir']):
                try:
                    with open("%s/PROF" % cu['workdir'], 'r') as prof_f:
                        txt = prof_f.read()
                        for line in txt.split("\n"):
                            if line:
                                x1, x2, x3 = line.split()
                                self._prof.prof(x1, msg=x2, timestamp=float(x3), uid=cu['_id'])
                except Exception as e:
                    self._log.error("Pre/Post profiling file read failed: `%s`" % e)

        # NOTE: all units get here after execution, even those which did not
        #       finish successfully.  We do that so that we can make
        #       stdout/stderr available for failed units.  But at this point we
        #       don't need to advance those units anymore, but can make them
        #       final.
        if cu['target_state'] != rp.DONE:
            self.advance(cu, cu['target_state'], publish=True, push=False)
            return


        try:
            # all other units get their (expectedly valid) output files staged
            for directive in cu['Agent_Output_Directives']:

                self._prof.prof('Agent output_staging', uid=cu['_id'],
                         msg="%s -> %s" % (str(directive['source']), str(directive['target'])))

                # Perform output staging
                self._log.info("unit output staging directives %s for cu: %s to %s",
                        directive, cu['_id'], workdir)

                # Convert the target_url into a SAGA Url object
                target_url = rs.Url(directive['target'])

                # Handle special 'staging' scheme
                if target_url.scheme == self._cfg['staging_scheme']:
                    self._log.info('Operating from staging')
                    # Remove the leading slash to get a relative path from
                    # the staging area
                    rel2staging = target_url.path.split('/',1)[1]
                    target = os.path.join(staging_area, rel2staging)
                else:
                    self._log.info('Operating from absolute path')
                    # FIXME: will this work for TRANSFER mode?
                    target = target_url.path

                # Get the source from the directive and convert it to the location
                # in the workdir
                source = str(directive['source'])
                abs_source = os.path.join(workdir, source)

                # Create output directory in case it doesn't exist yet
                # FIXME: will this work for TRANSFER mode?
                rec_makedir(os.path.dirname(target))

                self._log.info("Going to '%s' %s to %s", directive['action'], abs_source, target)

                if directive['action'] == LINK:
                    # This is probably not a brilliant idea, so at least give a warning
                    os.symlink(abs_source, target)
                elif directive['action'] == COPY:
                    shutil.copyfile(abs_source, target)
                elif directive['action'] == MOVE:
                    shutil.move(abs_source, target)
                else:
                    # FIXME: implement TRANSFER mode
                    raise NotImplementedError('Action %s not supported' % directive['action'])

                log_message = "%s'ed %s to %s - success" %(directive['action'], abs_source, target)
                self._log.info(log_message)

        except Exception as e:
            self._log.exception("staging output failed -> unit failed")
            staging_ok = False


        # Agent output staging is done (or failed)
        if staging_ok:
          # self.advance(cu, rp.UMGR_STAGING_OUTPUT_PENDING, publish=True, push=True)
            self.advance(cu, rp.PENDING_OUTPUT_STAGING, publish=True, push=False)
        else:
            self.advance(cu, rp.FAILED, publish=True, push=False)



# ==============================================================================
#
class AgentWorker(rpu.Worker):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self.agent_name = cfg['agent_name']
        rpu.Worker.__init__(self, 'AgentWorker', cfg)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._log.debug('starting AgentWorker for %s' % self.agent_name)

        # everything which comes after the worker init is limited in scope to
        # the current process, and will not be available in the worker process.
        self._pilot_id    = self._cfg['pilot_id']
        self._session_id  = self._cfg['session_id']
        self.final_cause  = None

        # all components use the command channel for control messages
        self.declare_subscriber('command', rp.AGENT_COMMAND_PUBSUB, self.command_cb)


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        # This callback is invoked as a thread in the process context of the
        # main agent (parent process) class.
        #
        # NOTE: That means it is *not* joined in the finalization of the run
        # loop (child), and the subscriber thread needs to be joined specifically in the
        # current process context.  At the moment that requires a call to
        # self._finalize() in the main process.

        cmd = msg['cmd']
        arg = msg['arg']

        self._log.info('agent command: %s %s' % (cmd, arg))

        if cmd == 'shutdown':

            # let agent know what caused the termination (first cause)
            if not self.final_cause:
                self.final_cause = arg

                self._log.info("shutdown command (%s)" % arg)
                self.stop()

            else:
                self._log.info("shutdown command (%s) - ignore" % arg)


    # --------------------------------------------------------------------------
    #
    def barrier_cb(self, topic, msg):

        # This callback is invoked in the process context of the run loop, and
        # will be cleaned up automatically.

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'alive':

            name = arg
            self._log.debug('waiting alive: \n%s\n%s\n%s'
                    % (self._components.keys(), self._workers.keys(),
                        self._sub_agents.keys()))

            # we only look at ALIVE messages which come from *this* agent, and
            # simply ignore all others (this is a shared medium after all)
            if name.startswith (self.agent_name):

                if name in self._components:
                    self._log.debug("component ALIVE (%s)" % name)
                    self._components[name]['alive'] = True

                elif name in self._workers:
                    self._log.debug("worker    ALIVE (%s)" % name)
                    self._workers[name]['alive'] = True

                else:
                    self._log.error("unknown   ALIVE (%s)" % name)

            elif name in self._sub_agents:
                self._log.debug("sub-agent ALIVE (%s)" % name)
                self._sub_agents[name]['alive'] = True


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        Read the configuration file, setup logging and mongodb connection.
        This prepares the stage for the component setup (self._setup()).
        """

        # keep track of objects we need to stop in the finally clause
        self._sub_agents = dict()
        self._components = dict()
        self._workers    = dict()

        # sanity check on config settings
        if not 'cores'               in self._cfg: raise ValueError("Missing number of cores")
        if not 'debug'               in self._cfg: raise ValueError("Missing DEBUG level")
        if not 'lrms'                in self._cfg: raise ValueError("Missing LRMS")
        if not 'mongodb_url'         in self._cfg: raise ValueError("Missing MongoDB URL")
        if not 'pilot_id'            in self._cfg: raise ValueError("Missing pilot id")
        if not 'runtime'             in self._cfg: raise ValueError("Missing or zero agent runtime")
        if not 'scheduler'           in self._cfg: raise ValueError("Missing agent scheduler")
        if not 'session_id'          in self._cfg: raise ValueError("Missing session id")
        if not 'spawner'             in self._cfg: raise ValueError("Missing agent spawner")
        if not 'task_launch_method'  in self._cfg: raise ValueError("Missing unit launch method")
        if not 'agent_layout'        in self._cfg: raise ValueError("Missing agent layout")

        self._pilot_id   = self._cfg['pilot_id']
        self._session_id = self._cfg['session_id']
        self._runtime    = self._cfg['runtime']
        self._sub_cfg    = self._cfg['agent_layout'][self.agent_name]
        self._pull_units = self._sub_cfg.get('pull_units', False)

        # this better be on a shared FS!
        self._cfg['workdir'] = os.getcwd()

        # another sanity check
        if self.agent_name == 'agent_0':
            if self._sub_cfg.get('target', 'local') != 'local':
                raise ValueError("agent_0 must run on target 'local'")

        # configure the agent logger
        self._log.setLevel(self._cfg['debug'])
        self._log.info('git ident: %s' % git_ident)

        # set up db connection -- only for the master agent and for the agent
        # which pulls units (which might be the same)
        if self.agent_name == 'agent_0' or self._pull_units:
            self._log.debug('connecting to mongodb at %s for unit pull')
            _, mongo_db, _, _, _  = ru.mongodb_connect(self._cfg['mongodb_url'])

            self._p  = mongo_db["%s.p"  % self._session_id]
            self._cu = mongo_db["%s.cu" % self._session_id]
            self._log.debug('connected to mongodb')

        # first order of business: set the start time and state of the pilot
        # Only the master agent performs this action
        if self.agent_name == 'agent_0':
            now = rpu.timestamp()
            ret = self._p.update(
                {"_id": self._pilot_id},
                {"$set" : {"state"        : rp.ACTIVE,
                           "started"      : now},
                 "$push": {"statehistory" : {"state"    : rp.ACTIVE,
                                             "timestamp": now}}
                })
            # TODO: Check for return value, update should be true!
            self._log.info("Database updated: %s", ret)

        # make sure we collect commands, specifically to implement the startup
        # barrier on bootstrap_4
        self.declare_publisher ('command', rp.AGENT_COMMAND_PUBSUB)
        self.declare_subscriber('command', rp.AGENT_COMMAND_PUBSUB, self.barrier_cb)

        # Now instantiate all communication and notification channels, and all
        # components and workers.  It will then feed a set of units to the
        # lead-in queue (staging_input).  A state notification callback will
        # then register all units which reached a final state (DONE).  Once all
        # units are accounted for, it will tear down all created objects.

        # we pick the layout according to our role (name)
        # NOTE: we don't do sanity checks on the agent layout (too lazy) -- but
        #       we would hiccup badly over ill-formatted or incomplete layouts...
        if not self.agent_name in self._cfg['agent_layout']:
            raise RuntimeError("no agent layout section for %s" % self.agent_name)

        try:
            self.start_sub_agents()
            self.start_components()

            # before we declare bootstrapping-success, the we wait for all
            # components, workers and sub_agents to complete startup.  For that,
            # all sub-agents will wait ALIVE messages on the COMMAND pubsub for
            # all entities it spawned.  Only when all are alive, we will
            # continue here.
            self.alive_barrier()

        except Exception as e:
            self._log.exception("Agent setup error: %s" % e)
            raise

        self._prof.prof('Agent setup done', logger=self._log.debug, uid=self._pilot_id)

        # also watch all components (once per second)
        self.declare_idle_cb(self.watcher_cb, 10.0)

        # once bootstrap_4 is done, we signal success to the parent agent
        # -- if we have any parent...
        if self.agent_name != 'agent_0':
            self.publish('command', {'cmd' : 'alive',
                                     'arg' : self.agent_name})

        # the pulling agent registers the staging_input_queue as this is what we want to push to
        # FIXME: do a sanity check on the config that only one agent pulls, as
        #        this is a non-atomic operation at this point
        self._log.debug('agent will pull units: %s' % bool(self._pull_units))
        if self._pull_units:

            self.declare_output(rp.AGENT_STAGING_INPUT_PENDING, rp.AGENT_STAGING_INPUT_QUEUE)
            self.declare_publisher('state', rp.AGENT_STATE_PUBSUB)

            # register idle callback, to pull for units -- which is the only action
            # we have to perform, really
            self.declare_idle_cb(self.idle_cb, self._cfg['db_poll_sleeptime'])


    # --------------------------------------------------------------------------
    #
    def alive_barrier(self):

        # FIXME: wait for bridges, too?  But we need pubsub for counting... Duh!
        total = len(self._components) + \
                len(self._workers   ) + \
                len(self._sub_agents)
        start   = time.time()
        timeout = 300

        while True:
            # check the procs for all components which are not yet alive
            to_check  = self._components.items() \
                      + self._workers.items() \
                      + self._sub_agents.items()

            alive_cnt = 0
            total_cnt = len(to_check)
            for name,c in to_check:
                if c['alive']:
                    alive_cnt += 1
                else:
                    self._log.debug('checking %s: %s', name, c)
                    if None != c['handle'].poll():
                        # process is dead and has never been alive.  Oops
                        raise RuntimeError('component %s did not come up' % name)

            self._log.debug('found alive: %2d / %2d' % (alive_cnt, total_cnt))

            if alive_cnt == total_cnt:
                self._log.debug('bootstrap barrier success')
                break

            if time.time() - timeout > start:
                raise RuntimeError('component barrier failed (timeout)')

            time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def watcher_cb(self):
        """
        we do a poll() on all our bridges, components, workers and sub-agent,
        to check if they are still alive.  If any goes AWOL, we will begin to
        tear down this agent.
        """

        to_watch = list(self._components.iteritems()) \
                 + list(self._workers.iteritems())    \
                 + list(self._sub_agents.iteritems())

      # self._log.debug('watch: %s' % pprint.pformat(to_watch))

        self._log.debug('checking %s things' % len(to_watch))
        for name, thing in to_watch:
            state = thing['handle'].poll()
            if state == None:
                self._log.debug('%-40s: ok' % name)
            else:
                raise RuntimeError ('%s died - shutting down' % name)

        return True # always idle


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        self._log.info("Agent finalizes")
        self._prof.prof('stop', uid=self._pilot_id)

        # tell other sub-agents get lost
        self.publish('command', {'cmd' : 'shutdown',
                                 'arg' : '%s finalization' % self.agent_name})

        # burn the bridges, burn EVERYTHING
        for name,sa in self._sub_agents.items():
            try:
                self._log.info("closing sub-agent %s", sa)
                sa['handle'].stop()
            except Exception as e:
                self._log.exception('ignore failing sub-agent terminate')

        for name,c in self._components.items():
            try:
                self._log.info("closing component %s", c)
                c['handle'].stop()
            except Exception as e:
                self._log.exception('ignore failing component terminate')

        for name,w in self._workers.items():
            try:
                self._log.info("closing worker %s", w)
                w['handle'].stop()
            except Exception as e:
                self._log.exception('ignore failing worker terminate')

        # communicate finalization to parent agent
        # -- if we have any parent...
        if self.agent_name != 'agent_0':
            self.publish('command', {'cmd' : 'final',
                                     'arg' : self.agent_name})

        self._log.info("Agent finalized")


    # --------------------------------------------------------------------------
    #
    def start_sub_agents(self):
        """
        For the list of sub_agents, get a launch command and launch that
        agent instance on the respective node.  We pass it to the seconds
        bootstrap level, there is no need to pass the first one again.
        """

        self._log.debug('start_sub_agents')

        sa_list = self._sub_cfg.get('sub_agents', [])

        if not sa_list:
            self._log.debug('start_sub_agents noop')
            return

        # the configs are written, and the sub-agents can be started.  To know
        # how to do that we create the agent launch method, have it creating
        # the respective command lines per agent instance, and run via
        # popen.
        #
        # actually, we only create the agent_lm once we really need it for
        # non-local sub_agents.
        agent_lm = None
        for sa in sa_list:
            target = self._cfg['agent_layout'][sa]['target']

            if target == 'local':

                # start agent locally
                cmdline = "/bin/sh -l %s/bootstrap_2.sh %s" % (os.getcwd(), sa)

            elif target == 'node':

                if not agent_lm:
                    agent_lm = rp.agent.LM.create(
                        name   = self._cfg['agent_launch_method'],
                        cfg    = self._cfg,
                        logger = self._log)

                node = self._cfg['lrms_info']['agent_nodes'][sa]
                # start agent remotely, use launch method
                # NOTE:  there is some implicit assumption that we can use
                #        the 'agent_node' string as 'agent_string:0' and
                #        obtain a well format slot...
                # FIXME: it is actually tricky to translate the agent_node
                #        into a viable 'opaque_slots' structure, as that is
                #        usually done by the schedulers.  So we leave that
                #        out for the moment, which will make this unable to
                #        work with a number of launch methods.  Can the
                #        offset computation be moved to the LRMS?
                # FIXME: are we using the 'hop' correctly?
                ls_name = "%s/%s.sh" % (os.getcwd(), sa)
                opaque_slots = {
                        'task_slots'   : ['%s:0' % node],
                        'task_offsets' : [],
                        'lm_info'      : self._cfg['lrms_info']['lm_info']}
                agent_cmd = {
                        'opaque_slots' : opaque_slots,
                        'description'  : {
                            'cores'      : 1,
                            'executable' : "/bin/sh",
                            'arguments'  : ["%s/bootstrap_2.sh" % os.getcwd(), sa]
                            }
                        }
                cmd, hop = agent_lm.construct_command(agent_cmd,
                        launch_script_hop='/usr/bin/env RP_SPAWNER_HOP=TRUE "%s"' % ls_name)

                with open (ls_name, 'w') as ls:
                    # note that 'exec' only makes sense if we don't add any
                    # commands (such as post-processing) after it.
                    ls.write('#!/bin/sh\n\n')
                    ls.write("exec %s\n" % cmd)
                    st = os.stat(ls_name)
                    os.chmod(ls_name, st.st_mode | stat.S_IEXEC)

                if hop : cmdline = hop
                else   : cmdline = ls_name

            # spawn the sub-agent
            self._prof.prof("create", msg=sa, uid=self._pilot_id)
            self._log.info ("create sub-agent %s: %s" % (sa, cmdline))
            sa_out = open("%s.out" % sa, "w")
            sa_err = open("%s.err" % sa, "w")
            sa_proc = subprocess.Popen(args=cmdline.split(), stdout=sa_out, stderr=sa_err)

            # make sure we can stop the sa_proc
            sa_proc.stop = sa_proc.terminate

            self._sub_agents[sa] = {'handle': sa_proc,
                                    'out'   : sa_out,
                                    'err'   : sa_err,
                                    'pid'   : sa_proc.pid,
                                    'alive' : False}
            self._prof.prof("created", msg=sa, uid=self._pilot_id)

        self._log.debug('start_sub_agents done')

    # --------------------------------------------------------------------------
    #
    def start_components(self):
        """
        For all componants defined on this agent instance, create the required
        number of those.  Keep a handle around for shutting them down later.
        """

        self._log.debug("start_components")

        # We use a static map from component names to class types for now --
        # a factory might be more appropriate (FIXME)
        cmap = {
            "AgentStagingInputComponent"  : AgentStagingInputComponent,
            "AgentSchedulingComponent"    : rp.agent.Scheduler,
            "AgentExecutingComponent"     : AgentExecutingComponent,
            "AgentStagingOutputComponent" : AgentStagingOutputComponent
            }
        for cname, cnum in self._sub_cfg.get('components',{}).iteritems():
            for i in range(cnum):
                # each component gets its own copy of the config
                ccfg = copy.deepcopy(self._cfg)
                ccfg['number'] = i
                comp = cmap[cname].create(ccfg)
                comp.start()
                self._components[comp.childname] = {'handle' : comp,
                                                    'alive'  : False}
                self._log.info('created component %s (%s): %s', cname, cnum, comp.cname)

        # we also create *one* instance of every 'worker' type -- which are the
        # heartbeat and update worker.  To ensure this, we only create workers
        # in agent_0.
        # FIXME: make this configurable, both number and placement
        if self.agent_name == 'agent_0':
            wmap = {
                rp.AGENT_UPDATE_WORKER    : rp.worker.Update,
                rp.AGENT_HEARTBEAT_WORKER : rp.worker.Heartbeat
                }
            for wname in wmap:
                self._log.info('create worker %s', wname)
                wcfg   = copy.deepcopy(self._cfg)
                worker = wmap[wname].create(wcfg)
                worker.start()
                self._workers[worker.childname] = {'handle' : worker,
                                                   'alive'  : False}

        self._log.debug("start_components done")


    # --------------------------------------------------------------------------
    #
    def idle_cb(self):
        """
        This method will be driving all other agent components, in the sense
        that it will manage the connection to MongoDB to retrieve units, and
        then feed them to the respective component queues.
        """

        # only do something if configured to do so
        if not self._pull_units:
            self._log.debug('not configured to pull for units')
            return True  # fake work to avoid busy noops

        try:
            # check for new units
            return self.check_units()

        except Exception as e:
            # exception in the main loop is fatal
            pilot_FAILED(self._p, self._pilot_id, self._log,
                "ERROR in agent main loop: %s. %s" % (e, traceback.format_exc()))
            sys.exit(1)


    # --------------------------------------------------------------------------
    #
    def check_units(self):

        # Check if there are compute units waiting for input staging
        # and log that we pulled it.
        #
        # FIXME: Unfortunately, 'find_and_modify' is not bulkable, so we have
        # to use 'find'.  To avoid finding the same units over and over again,
        # we update the state *before* running the next find -- so we do it
        # right here...  No idea how to avoid that roundtrip...
        # This also blocks us from using multiple ingest threads, or from doing
        # late binding by unit pull :/
        cu_cursor = self._cu.find(spec  = {"pilot"   : self._pilot_id,
                                           'state'   : rp.AGENT_STAGING_INPUT_PENDING,
                                           'control' : 'umgr'})
        if not cu_cursor.count():
            # no units whatsoever...
            self._log.info("units pulled:    0")
            return False

        # update the unit states to avoid pulling them again next time.
        cu_list = list(cu_cursor)
        cu_uids = [cu['_id'] for cu in cu_list]

        self._cu.update(multi    = True,
                        spec     = {"_id"   : {"$in"     : cu_uids}},
                        document = {"$set"  : {"control" : 'agent'}})

        self._log.info("units pulled: %4d"   % len(cu_list))
        self._prof.prof('get', msg="bulk size: %d" % len(cu_list), uid=self._pilot_id)
        for cu in cu_list:
            self._prof.prof('get', msg="bulk size: %d" % len(cu_list), uid=cu['_id'])

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).  We don't publish nor profile as advance,
        # since that happened already on the module side when the state was set.
        self.advance(cu_list, publish=False, push=True, prof=False)

        # indicate that we did some work (if we did...)
        return True



# ==============================================================================
#
# Agent bootstrap stage 3
#
# ==============================================================================
#
def start_bridges(cfg, log):
    """
    For all bridges defined on this agent instance, create that bridge.
    Keep a handle around for shutting them down later.
    """

    log.debug('start_bridges')

    # ----------------------------------------------------------------------
    # shortcut for bridge creation
    bridge_type = {rp.AGENT_STAGING_INPUT_QUEUE  : 'queue',
                   rp.AGENT_SCHEDULING_QUEUE     : 'queue',
                   rp.AGENT_EXECUTING_QUEUE      : 'queue',
                   rp.AGENT_STAGING_OUTPUT_QUEUE : 'queue',
                   rp.AGENT_UNSCHEDULE_PUBSUB    : 'pubsub',
                   rp.AGENT_RESCHEDULE_PUBSUB    : 'pubsub',
                   rp.AGENT_COMMAND_PUBSUB       : 'pubsub',
                   rp.AGENT_STATE_PUBSUB         : 'pubsub'}

    def _create_bridge(name):
        if bridge_type[name] == 'queue':
            return rpu.Queue.create(rpu.QUEUE_ZMQ, name, rpu.QUEUE_BRIDGE)
        elif bridge_type[name] == 'pubsub':
            return rpu.Pubsub.create(rpu.PUBSUB_ZMQ, name, rpu.PUBSUB_BRIDGE)
        else:
            raise ValueError('unknown bridge type for %s' % name)
    # ----------------------------------------------------------------------

    # create all bridges we need.  Use the default addresses,
    # ie. they will bind to all local interfacces on ports 10.000++.
    bridges = dict()
    sub_cfg = cfg['agent_layout']['agent_0']
    for b in sub_cfg.get('bridges', []):

        bridge     = _create_bridge(b)
        bridge_in  = bridge.bridge_in
        bridge_out = bridge.bridge_out
        bridges[b] = {'handle' : bridge,
                      'in'     : bridge_in,
                      'out'    : bridge_out,
                      'alive'  : True}  # no alive check done, yet
        log.info('created bridge %s: %s', b, bridge.name)

    log.debug('start_bridges done')

    return bridges


# --------------------------------------------------------------------------
#
def write_sub_configs(cfg, bridges, nodeip, log):
    """
    create a sub_config for each sub-agent we intent to spawn
    """

    # get bridge addresses from our bridges, and append them to the config
    if not 'bridge_addresses' in cfg:
        cfg['bridge_addresses'] = dict()

    for b in bridges:
        # to avoid confusion with component input and output, we call bridge
        # input a 'sink', and a bridge output a 'source' (from the component
        # perspective)
        sink   = ru.Url(bridges[b]['in'])
        source = ru.Url(bridges[b]['out'])

        # we replace the ip address with what we got from LRMS (nodeip).  The
        # bridge should be listening on all interfaces, but we want to make sure
        # the sub-agents connect on an IP which is accessible to them
        sink.host   = nodeip
        source.host = nodeip

        # keep the resultin URLs as strings, to be used as addresses
        cfg['bridge_addresses'][b] = dict()
        cfg['bridge_addresses'][b]['sink']   = str(sink)
        cfg['bridge_addresses'][b]['source'] = str(source)

    # write deep-copies of the config (with the corrected agent_name) for each
    # sub-agent (apart from agent_0, obviously)
    for sa in cfg.get('agent_layout'):
        if sa != 'agent_0':
            sa_cfg = copy.deepcopy(cfg)
            sa_cfg['agent_name'] = sa
            ru.write_json(sa_cfg, './%s.cfg' % sa)


# --------------------------------------------------------------------------
#
# avoid undefined vars on finalization / signal handling
bridges = dict()
agent   = None
lrms    = None

def bootstrap_3():
    """
    This method continues where the bootstrapper left off, but will quickly pass
    control to the Agent class which will spawn the functional components.

    Most of bootstrap_3 applies only to agent_0, in particular all mongodb
    interactions remains excluded for other sub-agent instances.

    The agent interprets a config file, which will specify in an agent_layout
    section:
      - what nodes should be used for sub-agent startup
      - what bridges should be started
      - what components should be started
      - what are the endpoints for bridges which are not started
    bootstrap_3 will create derived config files for all sub-agents.

    The agent master (agent_0) will collect information about the nodes required
    for all instances.  That is added to the config itself, for the benefit of
    the LRMS initialisation which is expected to block those nodes from the
    scheduler.
    """

    global lrms, agent, bridges

    # find out what agent instance name we have
    if len(sys.argv) != 2:
        raise RuntimeError('invalid number of parameters (%s)' % sys.argv)
    agent_name = sys.argv[1]

    # load the agent config, and overload the config dicts
    agent_cfg  = "%s/%s.cfg" % (os.getcwd(), agent_name)
    print "startup agent %s : %s" % (agent_name, agent_cfg)

    cfg = ru.read_json_str(agent_cfg)
    cfg['agent_name'] = agent_name
    pilot_id = cfg['pilot_id']

    # set up a logger and profiler
    prof = rpu.Profiler ('%s.bootstrap_3' % agent_name)
    prof.prof('sync ref', msg='agent start', uid=pilot_id)
    log  = ru.get_logger('%s.bootstrap_3' % agent_name,
                         '%s.bootstrap_3.log' % agent_name, 'DEBUG')  # FIXME?
    log.info('start')
    prof.prof('sync ref', msg='agent start')

    try:
        import setproctitle as spt
        spt.setproctitle('radical.pilot %s' % agent_name)
    except Exception as e:
        log.debug('no setproctitle: %s', e)

    log.setLevel(cfg.get('debug', 'INFO'))

    print "Agent config (%s):\n%s\n\n" % (agent_cfg, pprint.pformat(cfg))

    # quickly set up a mongodb handle so that we can report errors.
    # FIXME: signal handlers need mongo_p, but we won't have that until later
    if agent_name == 'agent_0':

        # Check for the RADICAL_PILOT_DB_HOSTPORT env var, which will hold the
        # address of the tunnelized DB endpoint.
        # If it exists, we overrule the agent config with it.
        hostport = os.environ.get('RADICAL_PILOT_DB_HOSTPORT')
        if hostport:
            dburl = ru.Url(cfg['mongodb_url'])
            dburl.host, dburl.port = hostport.split(':')
            cfg['mongodb_url'] = str(dburl)

        _, mongo_db, _, _, _  = ru.mongodb_connect(cfg['mongodb_url'])
        mongo_p = mongo_db["%s.p" % cfg['session_id']]

        if not mongo_p:
            raise RuntimeError('could not get a mongodb handle')


    # set up signal and exit handlers
    def exit_handler():
        global lrms, agent, bridges

        print 'atexit'
        if lrms:
            lrms.stop()
            lrms = None
        if bridges:
            for b in bridges:
                b.stop()
            bridges = dict()
        if agent:
            agent.stop()
            agent = None
        sys.exit(1)

    def sigint_handler(signum, frame):
        if agent_name == 'agent_0':
            pilot_FAILED(msg='Caught SIGINT. EXITING (%s)' % frame)
        print 'sigint'
        prof.prof('stop', msg='sigint_handler', uid=pilot_id)
        prof.close()
        sys.exit(2)

    def sigterm_handler(signum, frame):
        if agent_name == 'agent_0':
            pilot_FAILED(msg='Caught SIGTERM. EXITING (%s)' % frame)
        print 'sigterm'
        prof.prof('stop', msg='sigterm_handler %s' % os.getpid(), uid=pilot_id)
        prof.close()
        sys.exit(3)

    def sigalarm_handler(signum, frame):
        if agent_name == 'agent_0':
            pilot_FAILED(msg='Caught SIGALRM (Walltime limit?). EXITING (%s)' % frame)
        print 'sigalrm'
        prof.prof('stop', msg='sigalarm_handler', uid=pilot_id)
        prof.close()
        sys.exit(4)

    import atexit
    atexit.register(exit_handler)
    signal.signal(signal.SIGINT,  sigint_handler)
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGALRM, sigalarm_handler)

    # if anything went wrong up to this point, we would have been unable to
    # report errors into mongodb.  From here on, any fatal error should result
    # in one of the above handlers or exit handlers being activated, thus
    # reporting the error dutifully.

    try:
        # ----------------------------------------------------------------------
        # des Pudels Kern: merge LRMS info into cfg and get the agent started

        if agent_name == 'agent_0':

            # only the master agent creates LRMS and sub-agent config files.
            # The LRMS which will give us the set of agent_nodes to use for
            # sub-agent startup.  Add the remaining LRMS information to the
            # config, for the benefit of the scheduler).

            lrms = rp.agent.RM.create(name   = cfg['lrms'],
                             cfg    = cfg,
                             logger = log)
            cfg['lrms_info'] = lrms.lrms_info


            # the master agent also is the only one which starts bridges.  It
            # has to do so before creating the AgentWorker instance, as that is
            # using the bridges already.

            bridges = start_bridges(cfg, log)
            # FIXME: make sure all communication channels are in place.  This could
            # be replaced with a proper barrier, but not sure if that is worth it...
            time.sleep (1)

            # after we started bridges, we'll add their in and out addresses
            # to the config, so that the communication channels can connect to
            # them.  At this point we also write configs for all sub-agents this
            # instance intents to spawn.
            #
            # FIXME: we should point the address to the node of the subagent
            #        which hosts the bridge, not the local IP.  Until this
            #        is fixed, bridges MUST run on agent_0 (which is what
            #        RM.hostip() below will point to).
            nodeip = rp.agent.RM.hostip(cfg.get('network_interface'), logger=log)
            write_sub_configs(cfg, bridges, nodeip, log)

            # Store some runtime information into the session
            if 'version_info' in lrms.lm_info:
                mongo_p.update({"_id": pilot_id},
                               {"$set": {"lm_info": lrms.lm_info['version_info']}})

        # we now have correct bridge addresses added to the agent_0.cfg, and all
        # other agents will have picked that up from their config files -- we
        # can start the agent and all its components!
        agent = AgentWorker(cfg)
        agent.start()

        log.debug('waiting for agent %s to join' % agent_name)
        agent.join()
        log.debug('agent %s joined' % agent_name)

        # ----------------------------------------------------------------------

    except SystemExit:
        log.exception("Exit running agent: %s" % agent_name)
        if agent and not agent.final_cause:
            agent.final_cause = "sys.exit"

    except Exception as e:
        log.exception("Error running agent: %s" % agent_name)
        if agent and not agent.final_cause:
            agent.final_cause = "error"

    finally:

        # in all cases, make sure we perform an orderly shutdown.  I hope python
        # does not mind doing all those things in a finally clause of
        # (essentially) main...
        if agent:
            agent.stop()
            agent = None
        log.debug('agent %s finalized' % agent_name)

        # agent.stop will not tear down bridges -- we do that here at last
        for name,b in bridges.items():
            try:
                log.info("closing bridge %s", b)
                b['handle'].stop()
            except Exception as e:
                log.exception('ignore failing bridge terminate (%s)', e)
        bridges = dict()

        # make sure the lrms release whatever it acquired
        if lrms:
            lrms.stop()
            lrms = None

        # agent_0 will also report final pilot state to the DB
        if agent_name == 'agent_0':
            if agent and agent.final_cause == 'timeout':
                pilot_DONE(mongo_p, pilot_id, log, "TIMEOUT received. Terminating.")
            elif agent and agent.final_cause == 'cancel':
                pilot_CANCELED(mongo_p, pilot_id, log, "CANCEL received. Terminating.")
            elif agent and agent.final_cause == 'finalize':
                log.info('shutdown due to component finalization -- assuming error')
                pilot_FAILED(mongo_p, pilot_id, log, "FINALIZE received")
            elif agent:
                pilot_FAILED(mongo_p, pilot_id, log, "TERMINATE received")
            else:
                pilot_FAILED(mongo_p, pilot_id, log, "FAILED startup")

        log.info('stop')
        prof.prof('stop', msg='finally clause agent', uid=pilot_id)
        prof.close()


# ==============================================================================
#
if __name__ == "__main__":

    print "---------------------------------------------------------------------"
    print
    print "PYTHONPATH: %s"  % sys.path
    print "python: %s"      % sys.version
    print "utils : %-5s : %s" % (ru.version_detail, ru.__file__)
    print "saga  : %-5s : %s" % (rs.version_detail, rs.__file__)
    print "pilot : %-5s : %s" % (rp.version_detail, rp.__file__)
    print "        type  : multicore"
    print "        gitid : %s" % git_ident
    print
    print "---------------------------------------------------------------------"
    print

    bootstrap_3()

    print "bootstrap_3 done"

#
# ------------------------------------------------------------------------------

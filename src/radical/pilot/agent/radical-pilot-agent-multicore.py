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

__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import copy
import time
import pprint
import signal
import setproctitle

import saga                 as rs
import radical.utils        as ru
import radical.pilot        as rp
import radical.pilot.utils  as rpu
import radical.pilot.states as rps


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
#
def update_db(state, session=None, pilot_uid=None, logger=None, msg=None):

    if logger:
        logger.info('pilot state: %s', state)
        logger.info(msg)
        logger.info(ru.get_trace())

    print 'pilot state: %s' % state
    print msg
    print ru.get_trace()

    if session.is_connected and pilot_uid:

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

        session.get_db()._c.update({'type'   : 'pilot', 
                                    "uid"    : pilot_uid},
              {"$pushAll" : {"log"           : msg},
               "$push"    : {"state_history" : {"state"     : state, 
                                                "timestamp" : now}},
               "$set"     : {"state"         : state, 
                             "stdout"        : rpu.tail(out),
                             "stderr"        : rpu.tail(err),
                             "logfile"       : rpu.tail(log),
                             "finished"      : now}
              })

    else:
        if logger:
            logger.error("cannot log state in database!")

        print "cannot log state in database!"

    session.close()



# ==============================================================================
#
# Agent bootstrap stage 3
#
# ==============================================================================
#
# avoid undefined vars on finalization / signal handling
agent = None

def bootstrap_3(agent_name):
    """
    This method continues where the bootstrapper left off, but will quickly pass
    control to the Agent class which will spawn the functional components.

    Most of bootstrap_3 applies only to agent_0, in particular all mongodb
    interactions remains excluded for other sub-agent instances.

    The agent interprets a config file, which will specify in an agent_layout
    section:
      - what nodes should be used for sub-agent startup
      - what bridges should be started
      - what are the endpoints for bridges which are not started
      - what components should be started
    bootstrap_3 will create derived config files for all sub-agents.
    """

    global agent

    session = None

    print "startup agent %s" % agent_name

    try:
        setproctitle.setproctitle('radical.pilot %s' % agent_name)

        # load the agent config, and overload the config dicts
        agent_cfg  = "%s/%s.cfg" % (os.getcwd(), agent_name)
        cfg        = ru.read_json_str(agent_cfg)
        pilot_id   = cfg['pilot_id']
        cfg['uid'] = cfg['session_id']

        # set up a logger and profiler
        prof = rpu.Profiler ('%s.bootstrap_3'     % agent_name)
        log  = ru.get_logger('%s.bootstrap_3'     % agent_name,
                             '%s.bootstrap_3.log' % agent_name, 'DEBUG')  # FIXME?

        prof.prof('sync ref', msg='agent start', uid=pilot_id)
        log.setLevel(cfg.get('debug', 'INFO'))
        log.info('start')

        print "Agent config (%s):\n%s\n\n" % (agent_cfg, pprint.pformat(cfg))

        # quickly set up a mongodb handle so that we can report state and errors.
        if agent_name == 'agent_0':

            # Check for the RADICAL_PILOT_DB_HOSTPORT env var, which will hold
            # the address of the tunnelized DB endpoint. If it exists, we
            # overrule the agent config with it.
            hostport = os.environ.get('RADICAL_PILOT_DB_HOSTPORT')
            if hostport:
                dburl = ru.Url(cfg['mongodb_url'])
                dburl.host, dburl.port = hostport.split(':')
                cfg['mongodb_url'] = str(dburl)
        
            session = rp.Session(cfg=cfg)

            if not session.is_connected:
                raise RuntimeError('agent_0 could not connect to mongodb')

        # other sub agents also need a session, but no connection to mongodb
        else:
            session = rp.Session(cfg=cfg, _connect=False)


        # set up signal and exit handlers
        def exit_handler():
            sys.exit(1)

        def sigint_handler(signum, frame):
            global agent
            if agent:
                agent.final_cause = 'sigint'
            sys.exit(2)

        def sigterm_handler(signum, frame):
            global agent
            if agent:
                agent.final_cause = 'sigterm'
            sys.exit(3)

        def sigalarm_handler(signum, frame):
            global agent
            if agent:
                agent.final_cause = 'sigalarm'
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

        # ----------------------------------------------------------------------
        # des Pudels Kern
        
        cfg['agent_name'] = agent_name
        agent = rp.worker.Agent(cfg, session)
        agent.start()
        agent.join()
        log.debug('agent %s joined', agent_name)

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
        log.debug('agent %s finalized' % agent_name)

        # agent_0 will also report final pilot state to the DB
        if agent_name == 'agent_0':

            # FIXME: make sure that final_cause is not set multiple times

            if agent: final_cause = agent.final_cause
            else    : final_cause = 'failed startup'

            if   final_cause == 'timeout'  : state = rps.DONE 
            elif final_cause == 'cancel'   : state = rps.CANCELED
            elif final_cause == 'sys.exit' : state = rps.CANCELED
            else                           : state = rps.FAILED

            update_db(state, session, pilot_id, log, final_cause)


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

    
    bootstrap_3(agent_name=sys.argv[1])

    print "bootstrap_3 done"

#
# ------------------------------------------------------------------------------


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
            # has to do so before creating the Agent Worker instance, as that is
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
        agent = rp.worker.Agent(cfg)
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
            elif agent and agent.final_cause == 'sys.exit':
                pilot_CANCELED(mongo_p, pilot_id, log, "EXIT received. Terminating.")
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


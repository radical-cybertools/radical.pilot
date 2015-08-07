#!/usr/bin/env python

import os
import sys
import time
import pprint
import signal
import traceback
import threading           as mt
import multiprocessing     as mp
import radical.utils       as ru
import saga                as rs
import radical.pilot       as rp
import radical.pilot.utils as rpu

# put git commit id into the logs so that we can pinpoint which exact version we
# are running
git_ident = "$Id$"

# this should give us stack traces on haning pilots
dh = ru.DebugHelper()

# we do a busy poll for units from mongodb.  POLL_DELAY defines the sleep time
# between individual polls.
POLL_DELAY = 0.01

# we keep a global handle to the pilot collection in mongodb connection, so that
# we can push pilot state updates to the db.  This is initialized in main().
# Also keep the pilot_id and other essentials around globally, for the same
# purpose.
mongo_p     = None
mongodb_url = None
pilot_id    = None
session_id  = None


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
def pilot_FAILED(message, logger=None):

    if logger: logger.error(message)
    else     : print "ERROR: %s" % message

    if not mongo_p:
        if logger: logger.error("cannot log error state in database!")
        else     : print "cannot log error state in database!"
        return

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

    logentry = [{"message": message,          "timestamp": now},
                {"message": rpu.get_rusage(), "timestamp": now}]

    mongo_p.update({"_id": pilot_id},
        {"$pushAll": {"log"         : logentry},
         "$push"   : {"statehistory": {"state"     : rp.FAILED,
                                       "timestamp" : now}},
         "$set"    : {"state"       : rp.FAILED,
                      "stdout"      : rpu.tail(out),
                      "stderr"      : rpu.tail(err),
                      "logfile"     : rpu.tail(log),
                      "finished"    : now}
        })


# ------------------------------------------------------------------------------
#
def pilot_CANCELED(message, logger=None):

    if logger: logger.warning(message)
    else     : print "WARNING: %s" % message

    if not mongo_p:
        if logger: logger.error("cannot log cancellation in database!")
        else     : print "cannot log cancellation in database!"
        return

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

    logentry = [{"message": message,          "timestamp": now},
                {"message": rpu.get_rusage(), "timestamp": now}]

    mongo_p.update({"_id": pilot_id},
        {"$pushAll": {"log"         : logentry},
         "$push"   : {"statehistory": {"state"     : rp.CANCELED,
                                       "timestamp" : now}},
         "$set"    : {"state"       : rp.CANCELED,
                      "stdout"      : rpu.tail(out),
                      "stderr"      : rpu.tail(err),
                      "logfile"     : rpu.tail(log),
                      "finished"    : now}
        })


# ------------------------------------------------------------------------------
#
def pilot_DONE(logger=None):

    if logger: logger.info("pilot %s is done" % pilot_id)
    else     : print "INFO: pilot %s is done" % pilot_id

    if not mongo_p:
        if logger: logger.error("cannot log completion in database!")
        else     : print "cannot log completion in database!"
        return

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

    logentry = [{"message": "pilot done",     "timestamp": now},
                {"message": rpu.get_rusage(), "timestamp": now}]

    mongo_p.update({"_id": pilot_id},
        {"$pushAll": {"log"         : logentry},
         "$push"   : {"statehistory": {"state"    : rp.DONE,
                                       "timestamp": now}},
         "$set"    : {"state"       : rp.DONE,
                      "stdout"      : rpu.tail(out),
                      "stderr"      : rpu.tail(err),
                      "logfile"     : rpu.tail(log),
                      "finished"    : now}
        })



# ==============================================================================
#
class AgentUpdateWorker(rpu.ComponentBase):
    """
    An AgentUpdateWorker pushes CU and Pilot state updates to mongodb.  Its instances
    compete for update requests on the update_queue.  Those requests will be
    triplets of collection name, query dict, and update dict.  Update requests
    will be collected into bulks over some time (BULK_COLLECTION_TIME), to
    reduce number of roundtrips.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg):

        rpu.ComponentBase.__init__(self, cfg=cfg)

        self._cfg           = cfg
        self._session_id    = cfg['session_id']


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        _, db, _, _, _      = ru.mongodb_connect(mongodb_url)
        self._mongo_db      = db
        self._cinfo         = dict()  # collection cache

        # get state notifications, because that is what we want to push out
        self.declare_subscriber('state', 'agent_state_pubsub', self.state_cb)

        # we run a separate thread which actually does the deed.  The main
        # process will only collect units to push
        self._stop   = mt.Event()
        self._lock   = mt.RLock()
        self._thread = mt.Thread(target=self._updater, args=[self._stop])
        self._thread.start()

        # FIXME: if the lock is slowing down too much, we need to use a simple
        # queue or alternating buffer between state_cb and updater thread.


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # make sure we collect the worker thread
        self._stop.set()
        self._thread.join()  # FIXME: timeout?


    # --------------------------------------------------------------------------
    #
    def state_cb(self, topic, unit):
        """
        we get state update requests, and push them onto bulks of similar
        requests, so that the worker thread can pick them up to send out to
        mongodb.
        """

        update_request = unit  # FIXME

        # got a new request.  Add to bulk (create as needed),
        # and push bulk if time is up.
        uid         = update_request.get('_id')
        state       = update_request.get('state', None)
        cbase       = update_request.get('cbase', '.cu')
        query_dict  = update_request.get('query', dict())
        update_dict = update_request.get('update',dict())

        if state :
            rpu.prof('get', msg="update_queue to AgentUpdateWorker (%s)" % state, uid=uid)
        else:
            rpu.prof('get', msg="update_queue to AgentUpdateWorker", uid=uid)

        cname = self._session_id + cbase

        with self._lock:

            if not cname in self._cinfo:
                self._cinfo[cname] = {
                        'coll' : self._mongo_db[cname],
                        'bulk' : None,
                        'last' : time.time(),  # time of last push
                        'uids' : list()
                        }

            cinfo = self._cinfo[cname]

            if not cinfo['bulk']:
                cinfo['bulk']  = cinfo['coll'].initialize_ordered_bulk_op()

            cinfo['uids'].append([uid, state])
            cinfo['bulk'].find  (query_dict) \
                         .update(update_dict)

            rpu.prof('unit update bulked (%s)' % state, uid=uid)


    # --------------------------------------------------------------------------
    #
    def updater(self, stop):

        while not stop.is_set():

            # ------------------------------------------------------------------
            try:

                action = 0

                with self._lock:

                    for cname in self._cinfo:
                        cinfo = self._cinfo[cname]

                        if not cinfo['bulk']:
                            return 0

                        now = time.time()
                        age = now - cinfo['last']

                        if cinfo['bulk'] and age > self._cfg['bulk_collection_time']:

                            res  = cinfo['bulk'].execute()
                            self._log.debug("bulk update result: %s", res)

                            rpu.prof('unit update bulk pushed (%d)' % len(cinfo['uids']))
                            for entry in cinfo['uids']:
                                uid   = entry[0]
                                state = entry[1]
                                if state:
                                    rpu.prof('unit update pushed (%s)' % state, uid=uid)
                                else:
                                    rpu.prof('unit update pushed', uid=uid)

                            cinfo['last'] = now
                            cinfo['bulk'] = None
                            cinfo['uids'] = list()
                            action += 1

                if not action:
                    time.sleep(self._cfg['db_poll_sleeptime'])

            except Exception as e:
                self._log.exception("unit update failed (%s)", e)
                # FIXME: should we fail the pilot at this point?
                # FIXME: Are the strategies to recover?


# ==============================================================================
#
class AgentStagingInputComponent(rpu.ComponentBase):
    """
    This component will perform input staging for units in STAGING_INPUT state,
    and will advance them to SCHEDULING state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.ComponentBase.__init__(self, cfg=cfg)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._workdir = self._cfg['workdir']

        self.declare_input ('AGENT_STAGING_INPUT_PENDING', 'agent_staging_input_queue')
        self.declare_worker('AGENT_STAGING_INPUT_PENDING', self.work)

        self.declare_output('AGENT_SCHEDULING_PENDING', 'agent_scheduling_queue')

        self.declare_publisher('state', 'agent_state_pubsub')


    # --------------------------------------------------------------------------
    #
    def work(self, unit):

        if not unit:
            rpu.prof('get_cmd', msg="stagein_queue to StageinWorker (wakeup)")
            return # FXIME

        # FIXME
        unit['state'] = rp.AGENT_STAGING_INPUT
        rpu.prof('get', msg="stagein_queue to StageinWorker (%s)" % unit['state'], uid=unit['_id'])

        sandbox      = os.path.join(self._workdir, unit['_id'])
        staging_area = os.path.join(self._workdir, self._config['staging_area'])

        unit['workdir']     = sandbox
        unit['stdout']      = ''
        unit['stderr']      = ''
        unit['opaque_clot'] = None

        stdout_file = unit['description'].get('stdout')
        if not stdout_file:
            stdout_file = 'STDOUT'
        unit['stdout_file'] = os.path.join(sandbox, stdout_file)

        stderr_file = unit['description'].get('stderr')
        if not stderr_file:
            stderr_file = 'STDERR'
        unit['stderr_file'] = os.path.join(sandbox, stderr_file)

        # create unit sandbox
        rec_makedir(sandbox)
        rpu.prof('unit mkdir', uid=unit['_id'])

        for directive in unit['Agent_Input_Directives']:

            rpu.prof('agent_input_staging ', uid=unit['_id'],
                     msg="%s -> %s" % (str(directive['source']), str(directive['target'])))

            if directive['state'] != rp.PENDING :

                # we ignore directives which need no action
                # FIXME: can this still happen?  Who would set them to PENDING?
                rpu.prof('Agent input_staging queue', uid=unit['_id'], msg='ignored')
                continue


            # Perform input staging
            self._log.info("unit input staging directives %s for unit: %s to %s",
                           directive, unit['_id'], sandbox)

            # Convert the source_url into a SAGA Url object
            source_url = rs.Url(directive['source'])

            # Handle special 'staging' scheme
            if source_url.scheme == self._config['staging_scheme']:
                self._log.info('Operating from staging')
                # Remove the leading slash to get a relative path from the staging area
                rel2staging = source_url.path.split('/',1)[1]
                source = os.path.join(staging_area, rel2staging)
            else:
                self._log.info('Operating from absolute path')
                source = source_url.path

            # Get the target from the directive and convert it to the location
            # in the sandbox
            target = directive['target']
            abs_target = os.path.join(sandbox, target)

            # Create output directory in case it doesn't exist yet
            rec_makedir(os.path.dirname(abs_target))

            self._log.info("Going to '%s' %s to %s", directive['action'], source, abs_target)

            if   directive['action'] == LINK: os.symlink     (source, abs_target)
            elif directive['action'] == COPY: shutil.copyfile(source, abs_target)
            elif directive['action'] == MOVE: shutil.move    (source, abs_target)
            else:
                # FIXME: implement TRANSFER mode
                raise NotImplementedError('Action %s not supported' % directive['action'])

            self._log.info("%s'ed %s to %s - success", \
                    (directive['action'], source, abs_target))

        # Agent staging is all done, unit can go to ALLOCATING
        self.advance(unit, AGENT_SCHEDULING_PENDING, publish=True, push=True)


# ==============================================================================
#
class AgentSchedulingComponent(rpu.ComponentBase):
    """
    This component will assign a limited, shared resource (cores) to units it
    receives.  It will do so asynchronously, ie. whenever it receives either
    a new unit, or a notification that a unit finished, ie. the cores it got
    assigned can be freed.  When a unit gets a core assigned, only then it will
    get advanced from SCHEDULING to EXECUTING state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.ComponentBase.__init__(self, cfg=cfg)

        # schedulers need LRMS
        self._lrms = LRMS.create(
                name            = lrms_name,
                cfg             = self._cfg,
                logger          = self._log,
                requested_cores = requested_cores)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._cores     = 500        # limited shared resource
        self._wait_pool = list()     # set of units which wait for the resource
        self._wait_lock = mt.RLock() # look on the above set

        self.declare_input ('SCHEDULING', 'agent_scheduling_queue')
        self.declare_worker('SCHEDULING', self.work_schedule)

        self.declare_output('EXECUTING',  'agent_executing_queue')

        # we need unschedule updates to learn about units which free their
        # allocated cores.  Those updates need to be issued after execution, ie.
        # by the AgentExecutionComponent.
        self.declare_publisher ('state',      'agent_state_pubsub')
        self.declare_subscriber('unschedule', 'agent_unschedule_pubsub', self.unschedule_cb)


    # --------------------------------------------------------------------------
    #
    def _alloc(self):
        """
        Check if a core is available for a unit.
        """

        # find a free core
        if self._cores > 0:
            self._cores -= 1
            self._log('---> %d' % self._cores)
            return True
        return False


    # --------------------------------------------------------------------------
    #
    def _dealloc(self):
        """
        Make a formerly assigned core available for new units.
        """

        self._cores += 1
        self._log('===> %d' % self._cores)


    # --------------------------------------------------------------------------
    #
    def work_schedule(self, unit):
        """
        When receiving units, place them in the wait pool and trigger
        a reschedule.
        """

        with self._wait_lock:
            self._wait_pool.append(unit)
        self._reschedule()


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, unit):
        """
        This method gets called when a unit frees its allocated core.  We
        deallocate it, and attempt to reschedule() waiting units. 
        """

        if unit['state'] in ['STAGING_OUTPUT', 'DONE', 'FAILED', 'CANCELED']:
            self._dealloc()
            self._reschedule()


    # --------------------------------------------------------------------------
    #
    def _reschedule(self):
        """
        If any resources are available, assign those to waiting units.  Any unit
        which gets a core assigned will be advanced to EXECUTING state, and we
        relinguish control.
        """

        with self._wait_lock:
            while len(self._wait_pool):
               if self._alloc():
                   unit = self._wait_pool[0]
                   self._wait_pool.remove(unit)
                   unit['state'] = 'EXECUTING'
                   # advance unit
                   self.advance(unit)
               else:
                    # don't look further through the wait pool for now
                    break



# ==============================================================================
#
class AgentExecutionComponent(rpu.ComponentBase):
    """
    This component expectes scheduled units (in EXECUTING state), and will
    execute their workload.  After execution, it will publish an unschedule
    notification.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.ComponentBase.__init__(self, cfg=cfg)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self.declare_input ('EXECUTING', 'agent_executing_queue')
        self.declare_worker('EXECUTING', self.work)

        self.declare_output('STAGING_OUTPUT', 'agent_staging_output_queue')

        self.declare_publisher('unschedule', 'agent_unschedule_pubsub')
        self.declare_publisher('state',      'agent_state_pubsub')


    # --------------------------------------------------------------------------
    #
    def work(self, unit):
        """
        For each unit we receive, exeute its workload, and then publish an
        unschedule notification.
        """

        # workload
      # time.sleep(1)

        unit['state'] = 'STAGING_OUTPUT'
        self.publish('unschedule', unit)
        self.advance(unit)


# ==============================================================================
#
class AgentStagingOutputComponent(rpu.ComponentBase):
    """
    This component will perform output staging for units in STAGING_OUTPUT state,
    and will advance them to the final DONE state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.ComponentBase.__init__(self, cfg=cfg)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self.declare_input ('STAGING_OUTPUT', 'agent_staging_output_queue')
        self.declare_worker('STAGING_OUTPUT', self.work)

        # we don't need an output queue -- units are advancing to a final state.
        self.declare_output('DONE', None) # drop final units

        self.declare_publisher('state', 'agent_state_pubsub')


    # --------------------------------------------------------------------------
    #
    def work(self, unit):

        unit['state'] = 'DONE'
        self.advance(unit)



# ==============================================================================
#
class Agent(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        rpu.prof('Agent init')

        self._debug_helper          = ru.DebugHelper()

        self.name                   = 'agent.master'
        self._cfg                   = None
        self._log                   = None
        self._pilot_id              = None
        self._session_id            = None
        self._runtime               = None

        self._terminate             = mt.Event()
        self._starttime             = time.time()
        self._workdir               = os.getcwd()

        self._initialize()  # get config, init up logger, db connection
        self._setup()       # create components


    # --------------------------------------------------------------------------
    #
    def _initialize(self):
        """
        Read the configuration file, setup logging and mongodb connection.
        This prepares the stage for the component setup (self._setup()).
        """

        # find out what agent instance name we have
        if len(sys.argv) == 1:
            # we are master
            self.name = 'agent.master'

        # configure the agent logger
        self._log = rpu.get_logger(name='rp.agent', target='agent.log', level='INFO')
        self._log.info('git ident: %s' % git_ident)

        # --------------------------------------------------------------------------
        # load the agent config, and overload the config dicts

        if not 'RADICAL_PILOT_CFG' in os.environ:
            raise RuntimeError('RADICAL_PILOT_CFG is not set - abort')

        self._log.info ("load config file %s" % os.environ['RADICAL_PILOT_CFG'])
        self._cfg = ru.read_json_str(os.environ['RADICAL_PILOT_CFG'])

        self._log.info("\Agent config:\n%s\n\n" % pprint.pformat(self._cfg))

        self._cfg['workdir'] = os.getcwd()

        if not 'cores'               in self._cfg: raise ValueError("Missing number of cores")
        if not 'debug'               in self._cfg: raise ValueError("Missing DEBUG level")
        if not 'lrms'                in self._cfg: raise ValueError("Missing LRMS")
        if not 'mongodb_url'         in self._cfg: raise ValueError("Missing MongoDB URL")
        if not 'pilot_id'            in self._cfg: raise ValueError("Missing pilot id")
        if not 'runtime'             in self._cfg: raise ValueError("Missing or zero agent runtime")
        if not 'scheduler'           in self._cfg: raise ValueError("Missing agent scheduler")
        if not 'session_id'          in self._cfg: raise ValueError("Missing session id")
        if not 'spawner'             in self._cfg: raise ValueError("Missing agent spawner")
        if not 'mpi_launch_method'   in self._cfg: raise ValueError("Missing mpi launch method")
        if not 'task_launch_method'  in self._cfg: raise ValueError("Missing unit launch method")
        if not 'agent_layout'        in self._cfg: raise ValueError("Missing agent layout")

        # prepare profiler
        rpu.prof_init('agent.prof', 'start', uid=self._cfg['pilot_id'])

        # configure the agent logger
        self._log.setLevel(self._cfg['debug'])

        # set up db connection
        _, mongo_db, _, _, _  = ru.mongodb_connect(self._cfg['mongodb_url'])

        self._p  = mongo_db["%s.p"  % self._session_id]
        self._cu = mongo_db["%s.cu" % self._session_id]


    # --------------------------------------------------------------------------
    #
    def _setup(self):
        """
        This method will instantiate all communitation and notification channels,
        and all components.  It will then feed a set of units to the lead-in queue
        (staging_input).  A state notification callback will then register all units
        which reached a final state (DONE).  Once all units are accounted for, it
        will tear down all created objects.

        The agent accepts a config, which will specifcy 
          - what bridges should be started
          - what components should be started
          - what are the endpoints for bridges which are not started
        """

        # NOTE: we don't do sanity checks on the agent layout (too lazy) -- but
        #       we would hiccup badly over ill-formatted or incomplete layouts...
        # we pick the layout according to our role (name)
        if not self.name in self._cfg['agent_layout']:
            raise RuntimeError("no agent layout section for %s" % self.name)

        try:
            layout = self._cfg['agent_layout'][self.name]

            start_components     = layout.get('components', {})
            start_bridges        = layout.get('bridges',    {})
            start_queue_bridges  = start_bridges.get('queue',    {})
            start_pubsub_bridges = start_bridges.get('pubsub',   {})

            # keep track of objects we need to close in the finally clause
            self._bridges    = list()
            self._components = list()

            # two shortcuts for bridge creation
            def _create_queue_bridge(qname):
                return rpu.Queue.create(rpu.QUEUE_ZMQ, qname, rpu.QUEUE_BRIDGE)

            def _create_pubsub_bridge(pname):
                return rpu.Pubsub.create(rpu.PUBSUB_ZMQ, pname, rpu.PUBSUB_BRIDGE)

            # create all queue bridges we need.  Use the default addresses,
            # ie. they will bind to localhost to ports 10.000++.  We don't start
            # bridges where the config contains an address, but instead pass that
            # address to the component configuration.
            bridge_addresses = dict() 
            for qname, qaddr in start_queue_bridges.iteritems():
                if qaddr == 'local':
                    # start new bridge locally
                    bridge = _create_queue_bridge(qname)
                    bridge_addresses[qname] = bridge.addr
                    self._bridges.append(bridge)
                else:
                    # use the bridge at the given address
                    bridge_addresses[qname] = qaddr

            # same procedure for the pubsub bridges
            for pname, paddr in start_pubsub_bridges.iteritems():
                if paddr == 'local':
                    # start new bridge locally
                    bridge = _create_pubsub_bridge(pname)
                    bridge_addresses[pname] = bridge.addr
                    self._bridges.append(bridge)
                else:
                    # use the bridge at the given address
                    bridge_addresses[pname] = paddr

            # FIXME: we now have the bridge addresses.  Those need to be corrected
            #        in the agent config before we pass that config on to (a) the
            #        components we create below, and (b) to any other agent instance
            #        we intent to spawn.

            # Based on the bridge setup, we pass a component config to the
            # components we create -- they need that config to set up communication
            # channels correctly.
            ccfg = {
                    'bridge_addresses' : bridge_addresses
                    }

            # create all the component types we need. create n instances for each
            # type.
            # We use a static map from component names to class types for now --
            # a factory might be more appropriate (FIXME)
            cmap = {
                "agent_update_worker"            : AgentUpdateWorker,
                "agent_staging_input_component"  : AgentStagingInputComponent,
                "agent_scheduling_component"     : AgentSchedulingComponent,
                "agent_executing_component"      : AgentExecutionComponent,
                "agent_staging_output_component" : AgentStagingOutputComponent
                }

            for cname, cnum in start_components.iteritems():
                for i in range(cnum):
                    print 'create %s' % cname
                    comp = cmap[cname](ccfg)
                    self._components.append(comp)

            # FIXME: make sure all communication channels are in place.  This could
            # be replaced with a proper barrier, but not sure if that is worth it...
            time.sleep (1)


        except Exception as e:
            raise

        finally:

            # FIXME: let logfiles settle before killing the components
            time.sleep(1)
            os.system('sync')

            # burn the bridges, burn EVERYTHING
            for c in self._components:
                c.close()

            for b in self._bridges:
                b.close()

        # ######################################################################
        # ######################################################################
        # create component instances

        self._scheduler = Scheduler.create(
                name            = scheduler_name,
                cfg             = self._cfg,
                logger          = self._log)
        self.worker_list.append(self._scheduler)

        # FIXME: move to AgentExecutingComponent
        self._task_launcher = LaunchMethod.create(
                name            = task_launch_method,
                cfg             = self._cfg,
                logger          = self._log,
                scheduler       = self._scheduler)

        self._mpi_launcher = LaunchMethod.create(
                name            = mpi_launch_method,
                cfg             = self._cfg,
                logger          = self._log,
                scheduler       = self._scheduler)

        for n in range(self._cfg['number_of_workers'][STAGEIN_WORKER]):
            stagein_worker = StageinWorker(
                name            = "StageinWorker-%d" % n,
                cfg             = self._cfg,
                logger          = self._log,
                agent           = self,
                workdir         = self._workdir
            )
            self.worker_list.append(stagein_worker)


        for n in range(self._cfg['number_of_workers'][EXEC_WORKER]):
            exec_worker = ExecWorker.create(
                name            = "ExecWorker-%d" % n,
                cfg             = self._cfg,
                spawner         = spawner,
                logger          = self._log,
                agent           = self,
                scheduler       = self._scheduler,
                task_launcher   = self._task_launcher,
                mpi_launcher    = self._mpi_launcher,
                pilot_id        = self._pilot_id,
                number          = n,
                session_id      = self._session_id
            )
            self.worker_list.append(exec_worker)


        for n in range(self._cfg['number_of_workers'][STAGEOUT_WORKER]):
            stageout_worker = StageoutWorker(
                name            = "StageoutWorker-%d" % n,
                cfg             = self._cfg,
                agent           = self,
                logger          = self._log,
                workdir         = self._workdir
            )
            self.worker_list.append(stageout_worker)


        for n in range(self._cfg['number_of_workers'][UPDATE_WORKER]):
            update_worker = AgentUpdateWorker(
                name            = "AgentUpdateWorker-%d" % n,
                cfg             = self._cfg,
                logger          = self._log,
                agent           = self,
                session_id      = self._session_id,
                mongodb_url     = mongodb_url
            )
            self.worker_list.append(update_worker)


        hbmon = HeartbeatMonitor(
                name            = "HeartbeatMonitor",
                cfg             = self._cfg,
                logger          = self._log,
                agent           = self,
                p               = self._p,
                starttime       = self._starttime,
                runtime         = self._runtime,
                pilot_id        = self._pilot_id)
        self.worker_list.append(hbmon)

        # FIXME
        rpu.prof('Agent init done')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        """
        Terminate the agent main loop.  The workers will be pulled down once the
        main loop finishes (see run())
        """
        # FIXME: who calls this?  ever?

        rpu.prof ('stop request')
        rpu.flush_prof()
        self.terminate()
        self.wait()

        # FIXME: signal the other agents, and shot down all components and
        #        bridges.


    # --------------------------------------------------------------------------
    #
    def run(self):
        """
        This method will be driving all other agent components, in the sense
        that it will manage the conncection to MongoDB to retrieve units, and
        then feed them to the respective component queues.
        """

        rpu.prof('run')

        # first order of business: set the start time and state of the pilot
        self._log.info("Agent %s for pilot %s starting ...", (self.name, self._pilot_id))
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

        while True:

            try:
                # check for new units
                action = self._check_units()

                # if no units have been seen, then wait for juuuust a little...
                # FIXME: use some mongodb notification mechanism to avoid busy
                # polling.  Tailed cursors or whatever...
                if not action:
                    time.sleep(self._cfg['db_poll_sleeptime'])

            except Exception as e:
                # exception in the main loop is fatal
                pilot_FAILED(self._p, self._pilot_id, self._log,
                    "ERROR in agent main loop: %s. %s" % (e, traceback.format_exc()))
                rpu.flush_prof()
                sys.exit(1)

        # record cancelation state
        pilot_CANCELED(self._p, self._pilot_id, self._log,
                "Terminated (_terminate set).")

        rpu.prof ('stop')
        rpu.flush_prof()
        sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def _check_units(self):

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
            return 0

        # update the unit states to avoid pulling them again next time.
        cu_list = list(cu_cursor)
        cu_uids = [_cu['_id'] for _cu in cu_list]

        self._cu.update(multi    = True,
                        spec     = {"_id"   : {"$in"     : cu_uids}},
                        document = {"$set"  : {"control" : 'agent'}})

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline)
        if cu_list:
            rpu.prof('Agent get units', msg="bulk size: %d" % cu_cursor.count(),
                 logger=self._log.info)

        for cu in cu_list:

            rpu.prof('get', msg="MongoDB to Agent (%s)" % cu['state'], uid=cu['_id'], logger=self._log.info)

            _cu_list, _ = rpu.blowup(self._cfg, cu, AGENT)
            for _cu in _cu_list :

                try:
                    self._stagein_queue.put(__cu)

                except Exception as e:
                    # if any unit sorting step failed, the unit did not end up in
                    # a queue (its always the last step).  We set it to FAILED
                    msg = "could not sort unit (%s)" % e
                    rpu.prof('error', msg=msg, uid=_cu['_id'], logger=self._log.exception)
                    _cu['state'] = rp.FAILED
                    self.update_unit_state(src    = 'Agent',
                                           uid    = _cu['_id'],
                                           state  = rp.FAILED,
                                           msg    = msg)
                    # NOTE: this is final, the unit will not be touched
                    # anymore.
                    _cu = None

        # indicate that we did some work (if we did...)
        return len(cu_uids)



# ==============================================================================
#
# Agent main code
#
# ==============================================================================
def main():
    """
    This method continues where the bootstrapper left off, but will quickly pass
    control to the Agent class which will spawn the functional components.
    """

    # FIXME: signal handlers need mongo_p, but we won't have that until later

    # quickly set up a mongodb handle so that we can report errors.  We need to
    # parse the config to get the url and some IDs.
    # FIXME: should those be the things we pass as arg or env?
    cfg = ru.read_json_str(os.environ['RADICAL_PILOT_CFG'])

    pprint.pprint(cfg)

    mongodb_url = cfg['mongodb_url']
    pilot_id    = cfg['pilot_id']
    session_id  = cfg['session_id']

    _, mongo_db, _, _, _  = ru.mongodb_connect(mongodb_url)
    mongo_p  = mongo_db["%s.p" % cfg['session_id']]

    # set up signal and exit handlers
    def exit_handler():
        print 'exit handler'
        rpu.flush_prof()
    
    def sigint_handler(signum, frame):
        print 'sigint'
        pilot_FAILED('Caught SIGINT. EXITING (%s)' % frame)
        sys.exit(2)

    def sigalarm_handler(signum, frame):
        print 'sigalrm'
        pilot_FAILED('Caught SIGALRM (Walltime limit?). EXITING (%s)' % frame)
        sys.exit(3)
        
    import atexit
    atexit.register(exit_handler)
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGALRM, sigalarm_handler)

    agent = None
    try:
        print 'try'
        agent = Agent()
        agent.run()

    except SystemExit:
        print 'sysexit'
        print ru.get_trace() 
        pilot_FAILED("Caught system exit. EXITING") 
        sys.exit(6)

    except Exception as e:
        print 'exception: %s' % e
        print ru.get_trace()
        pilot_FAILED("Error running agent: %s" % str(e))
        sys.exit(7)

    finally:
        # attempt to shut down the agent
        if agent:
            agent.terminate()
            time.sleep(1)
        rpu.prof('stop', msg='finally clause')
        sys.exit(8)


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
    print "        type  : test"
    print "        gitid : %s" % git_ident
    print "        config: %s" % os.environ.get('RADICAL_PILOT_CFG')
    print
    print "---------------------------------------------------------------------"
    print

    sys.exit(main())

#
# ==============================================================================


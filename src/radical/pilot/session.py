
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import copy
import time

from typing import Optional

import threading as mt

import radical.utils                as ru
import radical.saga                 as rs
import radical.saga.filesystem      as rsfs
import radical.saga.utils.pty_shell as rsup

from . import constants as rpc
from . import utils     as rpu

from .messages        import HeartbeatMessage
from .proxy           import Proxy
from .resource_config import ResourceConfig, ENDPOINTS_DEFAULT


# ------------------------------------------------------------------------------
#
class _CloseOptions(ru.TypedDict):
    """Options and validation for Session.close().

    Arguments:
        download (bool, optional): Fetch pilot profiles and database entries.
            (Default False.)
        terminate (bool, optional): Shut down all pilots associated with the
            session. (Default True.)

    """

    _check = True

    _schema = {
        'download' : bool,
        'terminate': bool,
        'cleanup'  : bool  # FIXME: to be removed
    }

    _defaults = {
        'download' : False,
        'terminate': True,
        'cleanup'  : True  # FIXME: to be removed
    }


# ------------------------------------------------------------------------------
#
class Session(rs.Session):
    """Root of RP object hierarchy for an application instance.

    A Session is the root object of all RP objects in an application instance:
    it holds :class:`radical.pilot.PilotManager` and
    :class:`radical.pilot.TaskManager` instances which in turn hold
    :class:`radical.pilot.Pilot` and :class:`radical.pilot.Task`
    instances, and several other components which operate on those stateful
    entities.
    """

    # In that role, the session will create a special pubsub channel `heartbeat`
    # which is used by all components in its hierarchy to exchange heartbeat
    # messages.  Those messages are used to watch component health - if
    # a (parent or child) component fails to send heartbeats for a certain
    # amount of time, it is considered dead and the process tree will terminate.
    # That heartbeat management is implemented in the `ru.Heartbeat` class.
    # Only primary sessions instantiate a heartbeat channel (i.e., only the root
    # sessions of RP client or agent modules), but all components need to call
    # the sessions `heartbeat()` method at regular intervals.

    # the reporter is an application-level singleton
    _reporter = None

    # a session has one of three possible roles:
    #   - primary: the session is the first explicit session instance created in
    #     an RP application.
    #   - agent: the session is the first session instance created in an RP
    #     agent.
    #   - default: any other session instance, for example such as created by
    #     components in the client or agent module.
    _PRIMARY = 'primary'
    _AGENT_0 = 'agent_0'
    _AGENT_N = 'agent_n'
    _DEFAULT = 'default'


    # --------------------------------------------------------------------------
    #
    def __init__(self, proxy_url: Optional[str ] = None,
                       uid      : Optional[str ] = None,
                       cfg      : Optional[dict] = None,
                       _role    : Optional[str ] = _PRIMARY,
                       _reg_addr: Optional[str ] = None,
                       **close_options):
        """Create a new session.

        A new Session instance is created and stored in the database.

        Any RP Session will require an RP Proxy to facilitate communication
        between the client machine (i.e., the host where the application created
        this Session instance) and the target resource (i.e., the host where the
        pilot agent/s is/are running and where the workload is being executed).

        A `proxy_url` can be specified which then must point to an RP Proxy
        Service instance which this session can use to establish a communication
        proxy.  If `proxy_url` is not specified, the session will check for the
        environment variables `RADICAL_PILOT_PROXY_URL` and will interpret it as
        such above.  If that information is not available, the session will
        instantiate a proxy service on the local host.  Note that any proxy
        service instantiated by the session itself will be terminated once the
        session instance is closed or goes out of scope and is thus garbage
        collected and as such should not be used by other session instances.

        Note: an RP proxy will have to be accessible by both the client and the
              target hosts to facilitate communication between both parties.
              That implies access to the respective ports.  Proxies started by
              the session itself will use the first port larger than 10.000
              which is found to be free.

        Arguments:

            proxy_url (str, optional): proxy service URL - points to an RP
              proxy service which is used to establish an RP communication proxy
              for this session.

            uid (str, optional): Create a session with this UID.  Session UIDs
                MUST be unique - otherwise they will lead to communication
                conflicts, resulting in undefined behaviours.

            cfg (str | dict, optional): a named or instantiated configuration
                to be used for the session.

            _role (`bool`): only `PRIMARY` sessions created by the original
                application process (via `rp.Session()`), will create proxies
                and Registry Services.  `AGENT` sessions will also create
                a Registry but no proxies.  All other `DEFAULT` session
                instances are instantiated internally in processes spawned
                (directly or indirectly) by the initial session, for example in
                some of it's components, or by the RP agent.  Those sessions
                will inherit the original session ID, but will not attempt to
                create a new proxies or registries.

            **close_options (optional): If additional key word arguments are
                provided, they will be used as the default arguments to
                Session.close(). This can be useful when the Session is used as
                a Python context manager, such that close() is called
                automatically at the end of a ``with`` block.

            _reg_addr (str, optional): Non-primary sessions will connect to the
                registry at that endpoint and pull session config and resource
                configurations from there.
        """
        self._t_start = time.time()

        self._role          = _role
        self._uid           = uid
        self._cfg           = ru.Config(cfg=cfg)
        self._reg_addr      = _reg_addr
        self._proxy_url     = proxy_url
        self._proxy_cfg     = None
        self._closed        = False
        self._created       = time.time()
        self._close_options = _CloseOptions(close_options)
        self._close_options.verify()

        self._proxy    = None    # proxy client instance
        self._reg      = None    # registry client instance
        self._pmgrs    = dict()  # map IDs to pmgr instances
        self._tmgrs    = dict()  # map IDs to tmgr instances
        self._cmgr     = None    # only primary sessions have a cmgr
        self._rm       = None    # resource manager (agent_0 sessions)
        self._hb       = None    # heartbeat monitor

        # this session is either living in the client application or lives in
        # the scope of a pilot.  In the latter case we expect `RP_PILOT_ID` to
        # be set - we derive the session module scope from that env variable.
        self._module = os.environ.get('RP_PILOT_ID', 'client')

        # non-primary sessions need a uid!
        if self._role != self._PRIMARY and not self._uid:
            raise ValueError('non-primary session needs UID (%s)' % self._role)

        # initialization is different for each session type
        # NOTE: we could refactor this to session sub-classes
        if   self._role == self._PRIMARY: self._init_primary()
        elif self._role == self._AGENT_0: self._init_agent_0()
        elif self._role == self._AGENT_N: self._init_agent_n()
        else                            : self._init_default()

        # now we have config and uid - initialize base class (saga session)
        rs.Session.__init__(self, uid=self._uid)

        # cache sandboxes etc.
        self._cache_lock = ru.RLock()
        self._cache      = {'endpoint_fs'      : dict(),
                            'resource_sandbox' : dict(),
                            'session_sandbox'  : dict(),
                            'pilot_sandbox'    : dict(),
                            'client_sandbox'   : self._cfg.client_sandbox,
                            'js_shells'        : dict(),
                            'fs_dirs'          : dict()}

        # at this point we have a bridge connection, logger, etc, and are done
        self._prof.prof('session_ok', uid=self._uid)

        if self._role == self._PRIMARY:
            self._rep.ok('>>ok\n')

        assert(self._reg)


    # --------------------------------------------------------------------------
    #
    def _init_primary(self):

        # The primary session
        #   - reads session config files
        #   - reads resource config files
        #   - starts the client side registry service
        #   - pushes the configs into that registry
        #   - pushes bridge and component configs into that registry
        #   - starts a ZMQ proxy (or ensures one is up and running)

        # if user did not set a uid, we need to generate a new ID
        if not self._uid:
            self._uid = ru.generate_id('rp.session', mode=ru.ID_PRIVATE)

        # we still call `_init_cfg` to complete missing config settings
        # FIXME: completion only needed by `PRIMARY`
        self._init_cfg_from_scratch()

        # primary sessions create a registry service
        self._start_registry()
        self._connect_registry()

        # only primary sessions start and initialize the proxy service
        self._start_proxy()

        # start heartbeat channel
        self._start_heartbeat()

        # push the session config into the registry
        self._publish_cfg()

        # start bridges and components
        self._start_components()

        # primary session hooks into the control pubsub
        bcfg = self._reg['bridges.%s' % rpc.CONTROL_PUBSUB]
        self._ctrl_pub = ru.zmq.Publisher(channel=rpc.CONTROL_PUBSUB,
                                          url=bcfg['addr_pub'],
                                          log=self._log,
                                          prof=self._prof)

        # crosswire local channels and proxy channels
        self._crosswire_proxy()


    # --------------------------------------------------------------------------
    #
    def _init_agent_0(self):

        # The agent_0 session expects the `cfg` parameter to contain the
        # complete agent config!
        #
        #   - starts the agent side registry service
        #   - separates
        #     - session config (== agent config)
        #     - bridge configs
        #     - component configs
        #     - resource config
        #   - pushes them all into the registry
        #   - connects to the ZMQ proxy for client/agent communication
        #   - start agent components

        self._init_cfg_from_dict()
        self._start_registry()
        self._connect_registry()
        self._connect_proxy()
        self._start_heartbeat()
        self._publish_cfg()
        self._init_rm()
        self._start_components()
        self._crosswire_proxy()


    # --------------------------------------------------------------------------
    #
    def _init_agent_n(self):

        # The agent_n session fetch their config from agent_0 registry
        #
        #   - connect to registry
        #   - fetch config from registry
        #   - start agent bridges and components

        # the config passed to the session c'tor is the *agent* config - keep it
        a_cfg = self._cfg

        self._connect_registry()
        self._init_cfg_from_registry()

        # merge the agent's config into the session config
        self._cfg.bridges    = ru.Config(cfg=a_cfg.get('bridges',    {}))
        self._cfg.components = ru.Config(cfg=a_cfg.get('components', {}))

        self._start_components()


    # --------------------------------------------------------------------------
    #
    def _init_default(self):

        # sub-agents and components connect to an existing registry (owned by
        # the `primary` session or `agent_0`) and load config settings from
        # there.

        self._connect_registry()
        self._init_cfg_from_registry()


    # --------------------------------------------------------------------------
    #
    def _start_registry(self):

        # make sure that no other registry is used
        if self._reg_addr:
            raise ValueError('cannot start registry when providing `reg_addr`')

        self._reg_service = ru.zmq.Registry(uid='%s.reg' % self._uid,
                                            path=self._cfg.path)
        self._reg_service.start()

        self._cfg.reg_addr = self._reg_service.addr


    # --------------------------------------------------------------------------
    #
    def _connect_registry(self):

        if not self._cfg.reg_addr:
            self._cfg.reg_addr = self._reg_addr

        if not self._cfg.reg_addr:
            raise ValueError('session needs a registry address')

        self._reg = ru.zmq.RegistryClient(url=self._cfg.reg_addr)


    # --------------------------------------------------------------------------
    #
    def _init_cfg_from_scratch(self):

        # A primary session will at this point have a registry client connected
        # to its registry service.  Further, self._cfg will either be a config
        # name to be read from disk (`session_<cfg_name>.json`), or a dictionary
        # with a specific, user provided config.  From this information clean up
        # `self._cfg` and store it in the registry.  Also read resource configs
        # and store the in the registry as well.

        # NOTE: `cfg_name` and `cfg` are overloaded, the user cannot point to
        #       a predefined config and amend it at the same time.  This might
        #       be ok for the session, but introduces an API inconsistency.

        cfg_name = 'default'
        if isinstance(self._cfg, str):
            cfg_name  = self._cfg
            self._cfg = None

        # load the named config, merge provided config
        self._cfg = ru.Config('radical.pilot.session', name=cfg_name,
                                                       cfg=self._cfg)

        rcfgs = ru.Config('radical.pilot.resource', name='*', expand=False)
        self._rcfgs = ru.Config()

        for site in rcfgs:
            self._rcfgs[site] = ru.Config()
            for res, rcfg in rcfgs[site].items():
                self._rcfgs[site][res] = ResourceConfig(rcfg)


        self._rcfg  = ru.Config()  # the local resource config, if known

        # set essential config values for *this* specific session
        self._cfg['sid'] = self._uid

        pwd = os.getcwd()

        if self._cfg.base:
            self._cfg.base = os.path.abspath(self._cfg.base)
        else:
            self._cfg.base = pwd

        if self._cfg.path:
            self._cfg.path = os.path.abspath(self._cfg.path)
        else:
            self._cfg.path = '%s/%s' % (self._cfg.base, self._cfg.sid)

        if self._cfg.client_sandbox:
            self._cfg.client_sandbox = os.path.abspath(self._cfg.client_sandbox)
        else:
            self._cfg.client_sandbox = pwd

        # change RU defaults to point logfiles etc. to the session sandbox
        def_cfg             = ru.DefaultConfig()
        def_cfg.log_dir     = self._cfg.path
        def_cfg.report_dir  = self._cfg.path
        def_cfg.profile_dir = self._cfg.path

        self._prof = self._get_profiler(name=self._uid)
        self._rep  = self._get_reporter(name=self._uid)
        self._log  = self._get_logger  (name=self._uid,
                                        level=self._cfg.get('log_lvl'),
                                        debug=self._cfg.get('debug_lvl'))

        from . import version_detail as rp_version_detail
        self._log.info('radical.pilot version: %s', rp_version_detail)
        self._log.info('radical.saga  version: %s', rs.version_detail)
        self._log.info('radical.utils version: %s', ru.version_detail)

        self._prof.prof('session_start', uid=self._uid)

        self._rep.info ('<<new session: ')
        self._rep.plain('[%s]' % self._uid)


    # --------------------------------------------------------------------------
    #
    def _init_cfg_from_dict(self):

        # A agent_0 session will read the configuration from agent_0.cfg and
        # pass it to the session.

        assert self._role == self._AGENT_0

        self._cfg = ru.Config(cfg=self._cfg)

        # we only have one resource config for the current resource
        self._rcfg  = ru.Config(cfg=self._cfg.resource_cfg)  # local config
        self._rcfgs = ru.Config()

        del self._cfg['resource_cfg']

        # set essential config values for *this* specific session
        pwd = os.getcwd()

        if self._cfg.base:
            self._cfg.base = os.path.abspath(self._cfg.base)
        else:
            self._cfg.base = pwd

        if self._cfg.path:
            self._cfg.path = os.path.abspath(self._cfg.path)
        else:
            self._cfg.path = pwd

        # change RU defaults to point logfiles etc. to the session sandbox
        def_cfg             = ru.DefaultConfig()
        def_cfg.log_dir     = self._cfg.path
        def_cfg.report_dir  = self._cfg.path
        def_cfg.profile_dir = self._cfg.path

        self._prof = self._get_profiler(name=self._uid)
        self._rep  = self._get_reporter(name=self._uid)
        self._log  = self._get_logger  (name=self._uid,
                                        level=self._cfg.get('log_lvl'),
                                        debug=self._cfg.get('debug_lvl'))

        from . import version_detail as rp_version_detail
        self._log.info('radical.pilot version: %s', rp_version_detail)
        self._log.info('radical.saga  version: %s', rs.version_detail)
        self._log.info('radical.utils version: %s', ru.version_detail)

        self._prof.prof('session_start', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _init_cfg_from_registry(self):

        # fetch config settings from the registry
        self._cfg   = ru.Config(cfg=self._reg['cfg'])
        self._rcfg  = ru.Config(cfg=self._reg['rcfg'])
        self._rcfgs = ru.Config(cfg=self._reg['rcfgs'])

        # change RU defaults to point logfiles etc. to the session sandbox
        # NOTE: this is racey: the first session in this process will win
        def_cfg             = ru.DefaultConfig()
        def_cfg.log_dir     = self._cfg.path
        def_cfg.report_dir  = self._cfg.path
        def_cfg.profile_dir = self._cfg.path

        self._prof = self._get_profiler(name=self._uid)
        self._rep  = self._get_reporter(name=self._uid)
        self._log  = self._get_logger  (name=self._uid,
                                        level=self._cfg.get('log_lvl'),
                                        debug=self._cfg.get('debug_lvl'))

        from . import version_detail as rp_version_detail
        self._log.info('radical.pilot version: %s', rp_version_detail)
        self._log.info('radical.saga  version: %s', rs.version_detail)
        self._log.info('radical.utils version: %s', ru.version_detail)

        self._log.debug('Session(%s, %s)', self._uid, self._role)
        self._prof.prof('session_start', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _start_heartbeat(self):

        # only primary and agent_0 sessions manage heartbeats
        assert self._role in [self._PRIMARY, self._AGENT_0]

        # start the embedded heartbeat pubsub bridge
        self._hb_pubsub = ru.zmq.PubSub('heartbeat_pubsub',
                                        cfg={'uid'    : 'heartbeat_pubsub',
                                             'type'   : 'pubsub',
                                             'log_lvl': 'debug',
                                             'path'   : self._cfg.path})
        self._hb_pubsub.start()
        time.sleep(1)

        # re-enable the test below if timing issues crop up
      # ru.zmq.test_pubsub(self._hb_pubsub.channel,
      #                    self._hb_pubsub.addr_pub,
      #                    self._hb_pubsub.addr_sub),

        # fill 'cfg.heartbeat' section
        self._cfg.heartbeat.addr_pub = str(self._hb_pubsub.addr_pub)
        self._cfg.heartbeat.addr_sub = str(self._hb_pubsub.addr_sub)

        # create a publisher for that channel to publish own heartbeat
        self._hb_pub = ru.zmq.Publisher(channel='heartbeat_pubsub',
                                        url=self._cfg.heartbeat.addr_pub,
                                        log=self._log,
                                        prof=self._prof)


        # --------------------------------------
        # start the heartbeat monitor, but first
        # define its callbacks
        def _hb_beat_cb():
            # called on every heartbeat: cfg.heartbeat.interval`
            # publish own heartbeat
            self._hb_pub.put('heartbeat', HeartbeatMessage(uid=self._uid))

            # also update proxy heartbeat
            if self._proxy:
                self._proxy.request('heartbeat', {'sid': self._uid})
        # --------------------------------------

        # --------------------------------------
        # called when some entity misses
        # heartbeats: `cfg.heartbeat.timeout`
        def _hb_term_cb(hb_uid):
            if self._cmgr:
                self._cmgr.close()
            return False
        # --------------------------------------

        # create heartbeat manager which monitors all components in this session
      # self._log.debug('hb %s from session', self._uid)
        self._hb = ru.Heartbeat(uid=self._uid,
                                timeout=self._cfg.heartbeat.timeout,
                                interval=self._cfg.heartbeat.interval,
                                beat_cb=_hb_beat_cb,
                                term_cb=_hb_term_cb,
                                log=self._log)
        self._hb.start()

        # --------------------------------------
        # subscribe to heartbeat msgs and inform
        # self._hb about every heartbeat
        def _hb_msg_cb(topic, msg):

            hb_msg = HeartbeatMessage(from_dict=msg)

            if hb_msg.uid != self._uid:
                self._hb.beat(uid=hb_msg.uid)
        # --------------------------------------

        ru.zmq.Subscriber(channel='heartbeat_pubsub',
                          topic='heartbeat',
                          url=self._cfg.heartbeat.addr_sub,
                          cb=_hb_msg_cb,
                          log=self._log,
                          prof=self._prof)


    # --------------------------------------------------------------------------
    #
    def _publish_cfg(self):

        # The primary session and agent_0 push their configs into the registry.
        # NOTE: proxy channels populate the registrie's `bridges` section

        assert self._role in [self._PRIMARY, self._AGENT_0]

        # push proxy, bridges, components and heartbeat subsections separately
        flat_cfg = copy.deepcopy(self._cfg)

        del flat_cfg['heartbeat']
        del flat_cfg['bridges']
        del flat_cfg['components']

        self._reg['cfg']        = flat_cfg
        self._reg['heartbeat']  = self._cfg.heartbeat
        self._reg['bridges']    = self._cfg.bridges  # proxy bridges
        self._reg['components'] = {}

        # if we have proxy channels, publish them in the bridges configs too
        if self._proxy_cfg:
            for channel in self._proxy_cfg:
                self._reg['bridges.%s' % channel] = self._proxy_cfg[channel]

        # primary sessions publish all known resource configs under `rcfgs`, the
        # agent_0 only publishes the *current* resource config under `rcfg`.
        if self._role == self._PRIMARY:
            self._reg['rcfg']  = dict()
            self._reg['rcfgs'] = self._rcfgs

        elif self._role == self._AGENT_0:
            self._reg['rcfg']  = self._rcfg
            self._reg['rcfgs'] = dict()


    # --------------------------------------------------------------------------
    #
    def _start_proxy(self):

        # A primary session will start a ZMQ proxy via which agents can connect
        # to the client.  It is also possible a proxy already exists and a proxy
        # address was passed to the ctor - in that case we will, obviously, not
        # start a proxy but just make sure the given one is being used.

        assert self._role == self._PRIMARY

        # check if an external proxy is being used.  If so we are done.
        if self._proxy_url:
            self._log.debug('use proxy at %s' % self._proxy_url)
            return

        self._proxy_url = os.environ.get('RADICAL_PILOT_PROXY_URL')
        if self._proxy_url:
            self._log.debug('found proxy at %s' % self._proxy_url)
            return

        # no luck - start an embedded proxy
        self._proxy_event  = mt.Event()
        self._proxy_thread = mt.Thread(target=self._run_proxy)
        self._proxy_thread.daemon = True
        self._proxy_thread.start()

        self._proxy_event.wait()
        assert self._proxy_url

        # the proxy url becomes part of the session cfg
        self._cfg.proxy_url = self._proxy_url

        self._rep.info ('<<zmq proxy  : ')
        self._rep.plain('[%s]' % self._proxy_url)

        # configure proxy channels
        try:
            self._proxy = ru.zmq.Client(url=self._cfg.proxy_url)
            self._proxy_cfg = self._proxy.request('register', {'sid':self._uid})

        except:
            self._log.exception('%s: failed to start proxy', self._role)
            raise


    # --------------------------------------------------------------------------
    #
    def _connect_proxy(self):

        assert self._role == self._AGENT_0

        # make sure we have a proxy address to use
        assert self._cfg.proxy_url

        # query the proxy service to fetch proxy cfg created by primary session
        self._proxy = ru.zmq.Client(url=self._cfg.proxy_url)
        self._proxy_cfg = self._proxy.request('lookup', {'sid': self._uid})
        self._log.debug('proxy response: %s', self._proxy_cfg)


    # ----------------------------------------------------------------------
    def crosswire_pubsub(self, src, tgt, from_proxy):

        # we only forward messages which have either no origin set (in this case
        # this method sets the origin), or whose origin is the same as
        # configured when crosswiring the channels (either 'client' or the pilot
        # ID).  Also, the messages need to have the `forward` flag set.

        path = self._cfg.path
        reg  = self._reg

        url_sub = reg['bridges.%s.addr_sub' % src.lower()]
        url_pub = reg['bridges.%s.addr_pub' % tgt.lower()]

        self._log.debug('XXX cfg fwd for topic:%s to %s', src, tgt)
        self._log.debug('XXX cfg fwd for %s to %s', url_sub, url_pub)

        publisher = ru.zmq.Publisher(channel=tgt, path=path, url=url_pub,
                                     log=self._log, prof=self._prof)

        def pubsub_fwd(topic, msg):

            if 'origin' not in msg:
                msg['origin'] = self._module

            if from_proxy:

                # all messages *from* the proxy are forwarded - but not the ones
                # which originated in *this* module in the first place.

                if msg['origin'] == self._module:
                  # self._log.debug('XXX >=! fwd %s to topic:%s: %s', src, tgt, msg)
                    return

              # self._log.debug('XXX >=> fwd %s to topic:%s: %s', src, tgt, msg)
                publisher.put(tgt, msg)

            else:

                # only forward messages which have the respective flag set
                if not msg.get('fwd'):
                  # self._log.debug('XXX =>! fwd %s to %s: %s [%s - %s]', src,
                  #                 tgt, msg, msg['origin'], self._module)
                    return

                # avoid message loops (forward only once)
                msg['fwd'] = False

                # only forward all messages which originated in *this* module.

                if not msg['origin'] == self._module:
                  # self._log.debug('XXX =>| fwd %s to topic:%s: %s', src, tgt, msg)
                    return

              # self._log.debug('XXX =>> fwd %s to topic:%s: %s', src, tgt, msg)
                publisher.put(tgt, msg)


        ru.zmq.Subscriber(channel=src, topic=src, path=path, cb=pubsub_fwd,
                          url=url_sub, log=self._log, prof=self._prof)


    # --------------------------------------------------------------------------
    #
    def _crosswire_proxy(self):

        # - forward local ctrl messages to control proxy
        # - forward local state updates to state proxy
        # - forward local task queue to proxy task queue
        #
        # - forward proxy ctrl messages to local control pubsub
        # - forward proxy state updates to local state pubsub
        # - forward proxy task queue messages to local task queue
        #
        # The local task queue endpoints differ for primary session and agent_0
        #
        # - primary:
        #   - forward from AGENT_STAGING_INPUT_PENDING_QUEUE
        #   - forward to   TMGR_STAGING_OUTPUT_PENDING_QUEUE
        #
        # - agent_0:
        #   - forward to   AGENT_STAGING_INPUT_PENDING_QUEUE
        #   - forward from TMGR_STAGING_OUTPUT_PENDING_QUEUE
        #
        # NOTE: the primary session task queues don't live in the session itself
        #       but are owned by the task manager instead - it will trigger the
        #       crosswire once the queues are created.

        assert self._role in [self._PRIMARY, self._AGENT_0]

        self.crosswire_pubsub(src=rpc.CONTROL_PUBSUB,
                              tgt=rpc.PROXY_CONTROL_PUBSUB,
                              from_proxy=False)
        self.crosswire_pubsub(src=rpc.PROXY_CONTROL_PUBSUB,
                              tgt=rpc.CONTROL_PUBSUB,
                              from_proxy=True)

        self.crosswire_pubsub(src=rpc.STATE_PUBSUB,
                              tgt=rpc.PROXY_STATE_PUBSUB,
                              from_proxy=False)
        self.crosswire_pubsub(src=rpc.PROXY_STATE_PUBSUB,
                              tgt=rpc.STATE_PUBSUB,
                              from_proxy=True)


    # --------------------------------------------------------------------------
    #
    def _init_rm(self):

        # import locally to avoid circular imports
        from .agent.resource_manager import ResourceManager

        rname    = self._rcfg.resource_manager
        self._rm = ResourceManager.create(name=rname,
                                          cfg=self._cfg,
                                          rcfg=self._rcfg,
                                          log=self._log, prof=self._prof)

        import pprint
        self._log.debug(pprint.pformat(self._rm.info))


    # --------------------------------------------------------------------------
    #
    def get_rm(self):

        return self._rm


    # --------------------------------------------------------------------------
    #
    def _start_components(self):

        assert self._role in [self._PRIMARY, self._AGENT_0, self._AGENT_N]

        # primary sessions and agents have a component manager which also
        # manages heartbeat.  'self._cmgr.close()` should be called during
        # termination
        self._cmgr = rpu.ComponentManager(self.uid, self.reg_addr, self._uid)
        self._cmgr.start_bridges(self._cfg.bridges)
        self._cmgr.start_components(self._cfg.components)


    # --------------------------------------------------------------------------
    # context manager `with` clause
    #
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    # --------------------------------------------------------------------------
    #
    def close(self, **kwargs):
        """Close the session.

        All subsequent attempts access objects attached to the session will
        result in an error. If cleanup is set to True, the session data is
        removed from the database.

        Arguments:
            terminate (bool, optional): Shut down all pilots associated with the
                session.
            download (bool, optional): Fetch pilot profiles and database
                entries.
        """

        # close only once
        if self._closed:
            return

        if self._role == self._PRIMARY:
            self._rep.info('closing session %s' % self._uid)

        self._log.debug("session %s closing", self._uid)
        self._prof.prof("session_close", uid=self._uid)

        # Merge kwargs with current defaults stored in self._close_options
        self._close_options.update(kwargs)
        self._close_options.verify()

        # to call for `_verify` method and to convert attributes
        # to their types if needed (but None value will stay if it is set)

        options = self._close_options

        if options.terminate:
            # terminate all components
            if self._role == self._PRIMARY:
                self._ctrl_pub.put(rpc.CONTROL_PUBSUB, {'cmd': 'terminate',
                                                        'arg': None})

        for tmgr_uid, tmgr in self._tmgrs.items():
            self._log.debug("session %s closes tmgr   %s", self._uid, tmgr_uid)
            tmgr.close()
            self._log.debug("session %s closed tmgr   %s", self._uid, tmgr_uid)

        for pmgr_uid, pmgr in self._pmgrs.items():
            self._log.debug("session %s closes pmgr   %s", self._uid, pmgr_uid)
            pmgr.close(terminate=options.terminate)
            self._log.debug("session %s closed pmgr   %s", self._uid, pmgr_uid)

        if self._cmgr:
            self._cmgr.close()

        # stop heartbeats
        if self._hb:
            self._hb.stop()
            self._hb_pubsub.stop()

        if self._proxy:

            if self._role == self._PRIMARY:
                try:
                    self._log.debug('session %s closes service', self._uid)
                    self._proxy.request('unregister', {'sid': self._uid})
                except:
                    pass

            if self._role in [self._PRIMARY, self._AGENT_0]:
                self._proxy.close()
                self._proxy = None

        self._log.debug("session %s closed", self._uid)
        self._prof.prof("session_stop", uid=self._uid)
        self._prof.close()

        self._closed = True

        # after all is said and done, we attempt to download the pilot log- and
        # profiles, if so wanted
        if options.download:

            self._prof.prof("session_fetch_start", uid=self._uid)
            self._log.debug('start download')
            tgt = self._cfg.base
          # # FIXME: MongoDB
          # self.fetch_json    (tgt='%s/%s' % (tgt, self.uid))
            self.fetch_profiles(tgt=tgt)
            self.fetch_logfiles(tgt=tgt)

            self._prof.prof("session_fetch_stop", uid=self._uid)

        if self._role == self._PRIMARY:

            # stop registry
            self._reg.close()
            self._reg_service.stop()  # this will dump registry

            self._t_stop = time.time()
            self._rep.info('<<session lifetime: %.1fs'
                          % (self._t_stop - self._t_start))
            self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def _run_proxy(self):

        proxy = Proxy(path=self._cfg.path)

        try:
            proxy.start()

            self._proxy_url = proxy.addr
            self._proxy_event.set()

            # run forever until process is interrupted or killed
            proxy.wait()

        finally:
            proxy.stop()
            proxy.wait()


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object."""

        object_dict = {
            'uid'      : self._uid,
            'proxy_url': str(self.proxy_url),
            'cfg'      : copy.deepcopy(self._cfg)
        }
        return object_dict


    # --------------------------------------------------------------------------
    #
    @property
    def reg_addr(self):
        return self._cfg.reg_addr


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def path(self):
        return self._cfg.path


    # --------------------------------------------------------------------------
    #
    @property
    def base(self):
        return self._cfg.base


    # --------------------------------------------------------------------------
    #
    @property
    def proxy_url(self):
        return self._cfg.proxy_url


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):
        return self._cfg


    # --------------------------------------------------------------------------
    #
    @property
    def rcfgs(self):
        return self._rcfgs


    # --------------------------------------------------------------------------
    #
    @property
    def rcfg(self):
        return self._rcfg


    # --------------------------------------------------------------------------
    #
    @property
    def cmgr(self):
        return self._cmgr


    # --------------------------------------------------------------------------
    #
    def _get_logger(self, name, level=None, debug=None):
        """Get the Logger instance.

        This is a thin wrapper around `ru.Logger()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """
        log = ru.Logger(name=name, ns='radical.pilot', path=self._cfg.path,
                         targets=['.'], level=level, debug=debug)

        return log


    # --------------------------------------------------------------------------
    #
    def _get_reporter(self, name):
        """Get the Reporter instance.

        This is a thin wrapper around `ru.Reporter()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        if not self._reporter:

            enabled = ru.get_env_ns('report', 'radical.pilot', 'false').lower()

            if enabled in ['1', 'true', 'on']:
                enabled = True
            else:
                enabled = False

            self._reporter = ru.Reporter(name=name, ns='radical.pilot',
                                         path=self._cfg.path, enabled=enabled)
        return self._reporter


    # --------------------------------------------------------------------------
    #
    def _get_profiler(self, name):
        """Get the Profiler instance.

        This is a thin wrapper around `ru.Profiler()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        prof = ru.Profiler(name=name, ns='radical.pilot', path=self._cfg.path)

        return prof


    # --------------------------------------------------------------------------
    #
    def inject_metadata(self, metadata):
        """Insert (experiment) metadata into an active session.

        RP stack version info always get added.
        """

        if not isinstance(metadata, dict):
            raise Exception("Session metadata should be a dict!")

        # FIXME MONGODB: to json
      # if self._dbs and self._dbs._c:
      #     self._dbs._c.update({'type'  : 'session',
      #                          "uid"   : self.uid},
      #                         {"$push" : {"metadata": metadata}})


    # --------------------------------------------------------------------------
    #
    def _register_pmgr(self, pmgr):

        self._pmgrs[pmgr.uid] = pmgr


  # # --------------------------------------------------------------------------
  # #
  # def _reconnect_pmgr(self, pmgr):
  #
  #     if not self._dbs.get_pmgrs(pmgr_ids=pmgr.uid):
  #         raise ValueError('could not reconnect to pmgr %s' % pmgr.uid)
  #
  #     self._pmgrs[pmgr.uid] = pmgr
  #
  #
    # --------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """Get PilotManager instances.

        Lists the unique identifiers of all :class:`radical.pilot.PilotManager`
        instances associated with this session.

        Returns:
            list[str]: A list of :class:`radical.pilot.PilotManager` uids.

        """

        return list(self._pmgrs.keys())


    # --------------------------------------------------------------------------
    #
    def get_pilot_managers(self, pmgr_uids=None):
        """Get known PilotManager(s).

        Arguments:
            pmgr_uids (str | Iterable[str], optional): uids of the PilotManagers
                we want.

        Returns:
            radical.pilot.PilotManager | list[radical.pilot.PilotManager]: One
                or more `radical.pilot.PilotManager` objects.

        """

        return_scalar = False
        if isinstance(pmgr_uids, str):
            pmgr_uids     = [pmgr_uids]
            return_scalar = True

        if pmgr_uids: pmgrs = [self._pmgrs[uid] for uid in pmgr_uids]
        else        : pmgrs =  list(self._pmgrs.values())

        if return_scalar: return pmgrs[0]
        else            : return pmgrs


    # --------------------------------------------------------------------------
    #
    def _register_tmgr(self, tmgr):

        self._tmgrs[tmgr.uid] = tmgr


  # # --------------------------------------------------------------------------
  # #
  # def _reconnect_tmgr(self, tmgr):
  #
  #     if not self._dbs.get_tmgrs(tmgr_ids=tmgr.uid):
  #         raise ValueError('could not reconnect to tmgr %s' % tmgr.uid)
  #
  #     self._tmgrs[tmgr.uid] = tmgr
  #
  #
    # --------------------------------------------------------------------------
    #
    def list_task_managers(self):
        """Get TaskManager identifiers.

        Lists the unique identifiers of all :class:`radical.pilot.TaskManager`
        instances associated with this session.

        Returns:
            list[str]: A list of :class:`radical.pilot.TaskManager` uids.

        """

        return list(self._tmgrs.keys())


    # --------------------------------------------------------------------------
    #
    def get_task_managers(self, tmgr_uids=None):
        """Get known TaskManager(s).

        Arguments:
            tmgr_uids (str | list[str]): uids of the TaskManagers we want

        Returns:
            radical.pilot.TaskManager | list[radical.pilot.TaskManager]:
                One or more `radical.pilot.TaskManager` objects.

        """

        return_scalar = False
        if not isinstance(tmgr_uids, list):
            tmgr_uids     = [tmgr_uids]
            return_scalar = True

        if tmgr_uids: tmgrs = [self._tmgrs[uid] for uid in tmgr_uids]
        else        : tmgrs =  list(self._tmgrs.values())

        if return_scalar: return tmgrs[0]
        else            : return tmgrs


    # --------------------------------------------------------------------------
    #
    def list_resources(self):
        """Get list of known resource labels.

        Returns a list of known resource labels which can be used in a pilot
        description.
        """

        resources = list()
        for domain in self._rcfgs:
            for host in self._rcfgs[domain]:
                resources.append('%s.%s' % (domain, host))

        return sorted(resources)


    # --------------------------------------------------------------------------
    #
    def get_resource_config(self, resource, schema=None):
        """Returns a dictionary of the requested resource config."""

        site, res = resource.split('.', 1)
        if site not in self._rcfgs:
            raise RuntimeError("Resource site '%s' is unknown." % site)

        if res not in self._rcfgs[site]:
            raise RuntimeError("Resource label '%s' unknown." % res)

        if not schema:
            schema = self._rcfgs[site][res]['default_schema']

        if not schema:
            from_dict = self._rcfgs[site][res]
            from_dict.label = resource
            return ResourceConfig(from_dict=from_dict)

        if schema not in self._rcfgs[site][res]['schemas']:
            raise RuntimeError("schema %s unknown for resource %s"
                              % (schema, resource))

        rcfg = ResourceConfig(from_dict=self._rcfgs[site][res])
        scfg = rcfg['schemas'][schema]

        ru.dict_merge(rcfg, scfg, ru.OVERWRITE)

        if 'resource_manager' in rcfg:
            # import locally to avoid circular imports
            from .agent.resource_manager import ResourceManager

            rm = ResourceManager.get_manager(rcfg['resource_manager'])
            if rm and rm.batch_started():
                rcfg.update(ENDPOINTS_DEFAULT)

        rcfg.label = resource

        rcfg.verify()

        return rcfg



  # # --------------------------------------------------------------------------
  # #
  # def fetch_json(self, tgt=None):
  #
  #     return rpu.fetch_json(self._uid, tgt=tgt, session=self,
  #                           skip_existing=True)
  #
  #
    # --------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None):

        return rpu.fetch_profiles(self._uid, tgt=tgt, session=self,
                                  skip_existing=True)


    # --------------------------------------------------------------------------
    #
    def fetch_logfiles(self, tgt=None):

        return rpu.fetch_logfiles(self._uid, tgt=tgt, session=self,
                                  skip_existing=True)


    # --------------------------------------------------------------------------
    #
    def _get_client_sandbox(self):
        """Client sandbox path.

        For the session in the client application, this is `os.getcwd()`.  For
        the session in any other component, specifically in pilot components,
        the client sandbox needs to be read from the session config (or pilot
        config).  The latter is not yet implemented, so the pilot can not yet
        interpret client sandboxes.  Since pilot-side staging to and from the
        client sandbox is not yet supported anyway, this seems acceptable
        (FIXME).
        """

        return self._cache['client_sandbox']


    # --------------------------------------------------------------------------
    #
    def _get_resource_sandbox(self, pilot):
        """Global RP sandbox.

        For a given pilot dict, determine the global RP sandbox, based on the
        pilot's 'resource' attribute.
        """

        # FIXME: this should get 'resource, schema=None' as parameters

        resource = pilot['description'].get('resource')
        schema   = pilot['description'].get('access_schema')

        if not resource:
            raise ValueError('Cannot get pilot sandbox w/o resource target')

        # the global sandbox will be the same for all pilots on any resource, so
        # we cache it
        with self._cache_lock:

            if resource not in self._cache['resource_sandbox']:

                # cache miss -- determine sandbox and fill cache
                rcfg   = self.get_resource_config(resource, schema)
                fs_url = rs.Url(rcfg['filesystem_endpoint'])

                # Get the sandbox from either the pilot_desc or resource conf
                sandbox_raw = pilot['description'].get('sandbox')
                if not sandbox_raw:
                    sandbox_raw = rcfg.get('default_remote_workdir', "$PWD")


                # we may need to replace pat elements with data from the pilot
                # description
                if '%' in sandbox_raw:
                    # expand from pilot description
                    expand = dict()
                    for k, v in pilot['description'].items():
                        if v is None:
                            v = ''
                        if k == 'project':
                            if '_' in v and 'ornl' in resource:
                                v = v.split('_')[0]
                            elif '-' in v and 'ncsa' in resource:
                                v = v.split('-')[0]
                        expand['pd.%s' % k] = v
                        if isinstance(v, str):
                            expand['pd.%s' % k.upper()] = v.upper()
                            expand['pd.%s' % k.lower()] = v.lower()
                        else:
                            expand['pd.%s' % k.upper()] = v
                            expand['pd.%s' % k.lower()] = v
                    sandbox_raw = sandbox_raw % expand


                # If the sandbox contains expandables, we need to resolve those
                # remotely.
                #
                # NOTE: this will only work for (gsi)ssh or similar shell
                #       based access mechanisms
                if '$' not in sandbox_raw:
                    # no need to expand further
                    sandbox_base = sandbox_raw

                else:
                    shell = self.get_js_shell(resource, schema)
                    ret, out, _ = shell.run_sync(' echo "WORKDIR: %s"' %
                                                 sandbox_raw)
                    if ret or 'WORKDIR:' not in out:
                        raise RuntimeError("Couldn't get remote workdir.")

                    sandbox_base = out.split(":")[1].strip()
                    self._log.debug("sandbox base %s", sandbox_base)

                # at this point we have determined the remote 'pwd' - the
                # global sandbox is relative to it.
                fs_url.path = "%s/radical.pilot.sandbox" % sandbox_base

                # before returning, keep the URL string in cache
                self._cache['resource_sandbox'][resource] = fs_url

            return self._cache['resource_sandbox'][resource]


    # --------------------------------------------------------------------------
    #
    def get_js_shell(self, resource, schema):

        if resource not in self._cache['js_shells']:
            self._cache['js_shells'][resource] = dict()

        if schema not in self._cache['js_shells'][resource]:

            rcfg   = self.get_resource_config(resource, schema)

            js_url = rcfg['job_manager_endpoint']
            js_url = rcfg.get('job_manager_hop', js_url)
            js_url = rs.Url(js_url)

            elems  = js_url.schema.split('+')

            if   'ssh'    in elems: js_url.schema = 'ssh'
            elif 'gsissh' in elems: js_url.schema = 'gsissh'
            elif 'fork'   in elems: js_url.schema = 'fork'
            elif len(elems) == 1  : js_url.schema = 'fork'
            else: raise Exception("invalid schema: %s" % js_url.schema)

            if js_url.schema == 'fork':
                js_url.host = 'localhost'

            self._log.debug("rsup.PTYShell('%s')", js_url)
            shell = rsup.PTYShell(js_url, self)
            self._cache['js_shells'][resource][schema] = shell

        return self._cache['js_shells'][resource][schema]


    # --------------------------------------------------------------------------
    #
    def get_fs_dir(self, url):

        if url not in self._cache['fs_dirs']:
            self._cache['fs_dirs'][url] = rsfs.Directory(url,
                                               flags=rsfs.CREATE_PARENTS)

        return self._cache['fs_dirs'][url]


    # --------------------------------------------------------------------------
    #
    def _get_session_sandbox(self, pilot):

        # FIXME: this should get 'resource, schema=None' as parameters

        resource = pilot['description'].get('resource')

        if not resource:
            raise ValueError('Cannot get session sandbox w/o resource target')

        with self._cache_lock:

            if resource not in self._cache['session_sandbox']:

                # cache miss
                resource_sandbox      = self._get_resource_sandbox(pilot)
                session_sandbox       = rs.Url(resource_sandbox)
                session_sandbox.path += '/%s' % self.uid

                self._cache['session_sandbox'][resource] = session_sandbox

            return self._cache['session_sandbox'][resource]


    # --------------------------------------------------------------------------
    #
    def _get_pilot_sandbox(self, pilot):

        # FIXME: this should get 'pid, resource, schema=None' as parameters

        pilot_sandbox = pilot.get('pilot_sandbox')
        if str(pilot_sandbox):
            return rs.Url(pilot_sandbox)

        pid = pilot['uid']
        with self._cache_lock:

            if pid not in self._cache['pilot_sandbox']:

                # cache miss
                session_sandbox     = self._get_session_sandbox(pilot)
                pilot_sandbox       = rs.Url(session_sandbox)
                pilot_sandbox.path += '/%s/' % pilot['uid']

                self._cache['pilot_sandbox'][pid] = pilot_sandbox

            return self._cache['pilot_sandbox'][pid]


    # --------------------------------------------------------------------------
    #
    def _get_endpoint_fs(self, pilot):

        # FIXME: this should get 'resource, schema=None' as parameters

        resource = pilot['description'].get('resource')

        if not resource:
            raise ValueError("Can't get fs-endpoint w/o resource target")

        with self._cache_lock:

            if resource not in self._cache['endpoint_fs']:

                # cache miss
                resource_sandbox  = self._get_resource_sandbox(pilot)
                endpoint_fs       = rs.Url(resource_sandbox)
                endpoint_fs.path  = ''

                self._cache['endpoint_fs'][resource] = endpoint_fs

            return self._cache['endpoint_fs'][resource]


    # --------------------------------------------------------------------------
    #
    def _get_task_sandbox(self, task, pilot):

        # If a sandbox is specified in the task description, then interpret
        # relative paths as relativet to the pilot sandbox.

        # task sandboxes are cached in the task dict
        task_sandbox = task.get('task_sandbox')
        if task_sandbox:
            return task_sandbox

        # specified in description?
        if not task_sandbox:
            sandbox  = task['description'].get('sandbox')
            if sandbox:
                task_sandbox = ru.Url(self._get_pilot_sandbox(pilot))
                if sandbox[0] == '/':
                    task_sandbox.path = sandbox
                else:
                    task_sandbox.path += '/%s/' % sandbox

        # default
        if not task_sandbox:
            task_sandbox = ru.Url(self._get_pilot_sandbox(pilot))
            task_sandbox.path += "/%s/" % task['uid']

        # cache
        task['task_sandbox'] = str(task_sandbox)

        return task_sandbox


    # --------------------------------------------------------------------------
    #
    def _get_jsurl(self, pilot):
        """Get job service endpoint and hop URL for pilot's target resource."""

        resrc   = pilot['description']['resource']
        schema  = pilot['description']['access_schema']
        rcfg    = self.get_resource_config(resrc, schema)

        js_url  = rs.Url(rcfg.get('job_manager_endpoint'))
        js_hop  = rs.Url(rcfg.get('job_manager_hop', js_url))

        # make sure the js_hop url points to an interactive access
        # TODO: this is an unreliable heuristics - we should require the js_hop
        #       URL to be specified in the resource configs.
        if   '+gsissh' in js_hop.schema or \
             'gsissh+' in js_hop.schema    : js_hop.schema = 'gsissh'
        elif '+ssh'    in js_hop.schema or \
             'ssh+'    in js_hop.schema    : js_hop.schema = 'ssh'
        else                               : js_hop.schema = 'fork'

        return js_url, js_hop


# ------------------------------------------------------------------------------



__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import copy
import time

from typing import Optional

import threading        as mt

import radical.utils    as ru

from . import constants as rpc
from . import utils     as rpu

from .proxy           import Proxy
from .resource_config import ResourceConfig, ENDPOINTS_DEFAULT


try   : LOG_ENABLED = ru.zmq.utils.LOG_ENABLED
except: LOG_ENABLED = False


# ------------------------------------------------------------------------------
#
class _CloseOptions(rpu.FastTypedDict):
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
class Session(object):
    """Root of RP object hierarchy for an application instance.

    A Session is the root object of all RP objects in an application instance:
    it holds :class:`radical.pilot.PilotManager` and
    :class:`radical.pilot.TaskManager` instances which in turn hold
    :class:`radical.pilot.Pilot` and :class:`radical.pilot.Task`
    instances, and several other components which operate on those stateful
    entities.
    """

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
    _CLIENT  = 'client'
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
                a Registry but no proxies.  `CLIENT` sessions will be limited to
                allow client interactions with RP components.
                All other `DEFAULT` session instances are instantiated
                internally in processes spawned (directly or indirectly) by the
                initial session, for example in some of it's components, or by
                the RP agent.  Those sessions will inherit the original session
                ID, but will not attempt to create a new proxies or registries.

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
        self._proxy_url     = proxy_url
        self._proxy_cfg     = None
        self._closed        = False
        self._created       = time.time()
        self._to_stop       = list()
        self._close_options = _CloseOptions(close_options)
        self._close_options.verify()

        self._proxy        = None    # proxy server instance
        self._proxy_client = None    # proxy client instance
        self._reg          = None    # registry client instance
        self._pmgrs        = dict()  # map IDs to pmgr instances
        self._tmgrs        = dict()  # map IDs to tmgr instances
        self._cmgr         = None    # only primary sessions have a cmgr
        self._rm           = None    # resource manager (agent_0 sessions)

        if _reg_addr:

            if self._cfg.reg_addr:
                if self._cfg.reg_addr != _reg_addr:
                    raise ValueError('session config and ctor arg mismatch')
            else:
                self._cfg.reg_addr = _reg_addr

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
        elif self._role == self._CLIENT : self._init_client()
        else                            : self._init_default()

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

        assert self._reg


    # --------------------------------------------------------------------------
    #
    def _init_primary(self):

        assert self._role == self._PRIMARY

        # The primary session
        #   - reads session config files
        #   - reads resource config files
        #   - starts the client side registry service
        #   - pushes the configs into that registry
        #   - pushes bridge and component configs into that registry
        #   - starts a ZMQ proxy (or ensures one is up and running)

        # if user did not set a uid, we need to generate a new ID
        if not self._uid:
            base = os.environ.get('RADICAL_PILOT_SID_BASE')
            if base:
                self._uid = ru.generate_id(base, mode=ru.SIMPLE)
            else:
                self._uid = ru.generate_id('rp.session', mode=ru.ID_PRIVATE)


        # we still call `_init_cfg` to complete missing config settings
        # FIXME: completion only needed by `PRIMARY`
        self._init_cfg_from_scratch()

        # primary sessions create a registry service
        self._start_registry()
        self._connect_registry()

        # only primary sessions start and initialize the proxy service
        self._start_proxy()

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
        self._ctrl_sub = ru.zmq.Subscriber(channel=rpc.CONTROL_PUBSUB,
                                           url=bcfg['addr_sub'],
                                           log=self._log,
                                           prof=self._prof,
                                           cb=self._control_cb,
                                           topic=rpc.CONTROL_PUBSUB)

        # crosswire local channels and proxy channels
        self._crosswire_proxy()

        self.dump('init')


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        self._log.debug('control msg: %s', msg)

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if not cmd:
            return

        if cmd == 'dump':
            self.dump(arg.get('name'))


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

        assert self._role == self._AGENT_0

        self._init_cfg_from_dict()
        self._start_registry()
        self._connect_registry()
        self._connect_proxy()
        self._publish_cfg()
        self._init_rm()
        self._start_components()
        self._crosswire_proxy()

        self._reg.dump(self._role)


    # --------------------------------------------------------------------------
    #
    def _init_agent_n(self):

        # The agent_n session fetch their config from agent_0 registry
        #
        #   - connect to registry
        #   - fetch config from registry
        #   - start agent bridges and components

        assert self._role == self._AGENT_N

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
    def _init_client(self):

        # clients connect to an existing registry (owned by the `primary`
        # session or `agent_0`) and load config settings from there.

        assert self._role == self._CLIENT

        self._connect_registry()
        self._init_cfg_from_registry()


    # --------------------------------------------------------------------------
    #
    def _init_default(self):

        # sub-agents and components connect to an existing registry (owned by
        # the `primary` session or `agent_0`) and load config settings from
        # there.

        assert self._role == self._DEFAULT

        self._connect_registry()
        self._init_cfg_from_registry()


    # --------------------------------------------------------------------------
    #
    def _start_registry(self):

        # make sure that no other registry is used
        if self.reg_addr:
            raise ValueError('cannot start registry when providing `reg_addr`')

        self._reg_service = ru.zmq.Registry(uid='%s.reg' % self._uid,
                                            path=self._cfg.path)
        self._reg_service.start()

        self._cfg.reg_addr = self._reg_service.addr


    # --------------------------------------------------------------------------
    #
    def _connect_registry(self):

        if not self.reg_addr:
            raise ValueError('session needs a registry address')

        self._reg = ru.zmq.RegistryClient(url=self.reg_addr)


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

        self._log_version()

        self._prof.prof('session_start', uid=self._uid)

        self._rep.info ('<<new session: ')
        self._rep.plain('[%s]' % self._uid)


    # --------------------------------------------------------------------------
    #
    def _log_version(self):

        from . import version        as rp_version
        from . import version_detail as rp_version_detail

        self._log.info('Session(%s, %s)', self._uid, self._role)
        self._log.info('radical.pilot version: %s (%s)', rp_version,
                                                         rp_version_detail)
        self._log.info('radical.utils version: %s (%s)', ru.version,
                                                         ru.version_detail)


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

        self._log_version()

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

        self._log_version()

        self._prof.prof('session_start', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _publish_cfg(self):

        # The primary session and agent_0 push their configs into the registry.
        # NOTE: proxy channels populate the registrie's `bridges` section

        assert self._role in [self._PRIMARY, self._AGENT_0]

        # push proxy, bridges, and components subsections separately
        flat_cfg = copy.deepcopy(self._cfg)

        del flat_cfg['bridges']
        del flat_cfg['components']

        self._reg['cfg']        = flat_cfg
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

        # check if an external proxy was epcified for the session.
        if self._proxy_url:
            self._log.debug('use proxy at %s' % self._proxy_url)
            return

       # check if an external proxy was specified in the environment
        elif 'RADICAL_PILOT_PROXY_URL' in os.environ:
            self._proxy_url = os.environ['RADICAL_PILOT_PROXY_URL']
            self._log.debug('found proxy at %s' % self._proxy_url)

        # no luck - start an embedded proxy
        else:
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
            self._proxy_client = ru.zmq.Client(url=self._cfg.proxy_url, log=self._log)
            self._proxy_cfg = self._proxy_client.request('register', {'sid':self._uid})

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
        self._proxy_client = ru.zmq.Client(url=self._cfg.proxy_url)
        self._proxy_cfg = self._proxy_client.request('lookup', {'sid': self._uid})
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

        if LOG_ENABLED:
            self._log.debug_5('XXX cfg fwd for topic:%s to %s', src, tgt)
            self._log.debug_5('XXX cfg fwd for %s to %s', url_sub, url_pub)

        publisher = ru.zmq.Publisher(channel=tgt, path=path, url=url_pub,
                                     log=self._log, prof=self._prof)

        def pubsub_fwd(topic, msg):

            if 'origin' not in msg:
                msg['origin'] = self._module

            if from_proxy:

                # all messages *from* the proxy are forwarded - but not the ones
                # which originated in *this* module in the first place.

                if msg['origin'] == self._module:
                    if LOG_ENABLED:
                        self._log.debug_9('XXX >=! fwd %s to topic:%s: %s',
                                          src, tgt, msg)
                    return

                if LOG_ENABLED:
                    self._log.debug_9('XXX >=> fwd %s to topic:%s: %s',
                                      src, tgt, msg)
                publisher.put(tgt, msg)

            else:

                # only forward messages which have the respective flag set
                if not msg.get('fwd'):
                    if LOG_ENABLED:
                        self._log.debug_9('XXX =>! fwd %s to %s: %s [%s - %s]',
                                          src, tgt, msg, msg['origin'],
                                          self._module)
                    return

                # only forward all messages which originated in *this* module.
                if not msg['origin'] == self._module:
                    if LOG_ENABLED:
                        self._log.debug_9('XXX =>| fwd %s to topic:%s: %s',
                                          src, tgt, msg)
                    return

                self._log.debug_9('XXX =>> fwd %s to topic:%s: %s', src, tgt, msg)

                # avoid message loops (forward only once)
                msg['fwd'] = False

                if LOG_ENABLED:
                    self._log.debug_3('XXX =>> fwd %s to topic:%s: %s',
                                      src, tgt, msg)
                publisher.put(tgt, msg)


        sub = ru.zmq.Subscriber(channel=src, topic=src, path=path,
                                cb=pubsub_fwd, url=url_sub,
                                log=self._log, prof=self._prof)

        self._to_stop.append(sub)


    # --------------------------------------------------------------------------
    #
    def _crosswire_proxy(self):

        # - forward local ctrl  pubsub messages to proxy control pubsub
        # - forward local state pubsub messages to proxy state   pubsub
        # - forward local task  queue  messages to proxy task    queue
        #
        # - forward proxy ctrl  pubsub messages to local control pubsub
        # - forward proxy state pubsub messages to local state   pubsub
        # - forward proxy task  queue  messages to local task    queue
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


    # --------------------------------------------------------------------------
    #
    def get_rm(self):

        return self._rm


    # --------------------------------------------------------------------------
    #
    def _start_components(self):

        assert self._role in [self._PRIMARY, self._AGENT_0, self._AGENT_N]

        # primary sessions and agents have a component manager
        # 'self._cmgr.close()` should be called during termination
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
    def dump(self, name=None):

        self._reg.dump(name)

        for tmgr in self._tmgrs.values():
            tmgr.dump(name)

        for pmgr in self._pmgrs.values():
            pmgr.dump(name)


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
                                                        'arg': None,
                                                        'fwd': True})

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

        if self._proxy_client:

            if self._role == self._PRIMARY:
                try:
                    self._log.debug('session %s closes service', self._uid)
                    self._proxy_client.request('unregister', {'sid': self._uid})
                except:
                    pass

            if self._role in [self._PRIMARY, self._AGENT_0]:
                self._proxy_client.close()
                self._proxy_client = None

        if self._proxy:

            if self._role in [self._PRIMARY, self._AGENT_0]:

                if self._proxy:
                    self._proxy.stop()
                    self._proxy.wait()
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
            self.fetch_profiles(tgt=tgt)
            self.fetch_logfiles(tgt=tgt)

            self._prof.prof("session_fetch_stop", uid=self._uid)

        if self._role == self._PRIMARY:

            # stop registry
            self._reg.dump()
            self._reg.close()
            self._reg_service.stop()

            self._ctrl_sub.stop()

            self._t_stop = time.time()
            self._rep.info('<<session lifetime: %.1fs'
                          % (self._t_stop - self._t_start))
            self._rep.ok('>>ok\n')


        for thing in self._to_stop:
            thing.stop()


    # --------------------------------------------------------------------------
    #
    def _run_proxy(self):

        self._proxy = Proxy(path=self._cfg.path)

        try:
            self._proxy.start()

            self._proxy_url = self._proxy.addr
            self._proxy_event.set()

            # run forever until process is interrupted or killed
            self._proxy.wait()

        finally:
            if self._proxy:
                self._proxy.stop()


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
        path = self._cfg.path or os.getcwd()
        log = ru.Logger(name=name, ns='radical.pilot', path=path,
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

            path = self._cfg.path or os.getcwd()
            self._reporter = ru.Reporter(name=name, ns='radical.pilot',
                                         path=path, enabled=enabled)
        return self._reporter


    # --------------------------------------------------------------------------
    #
    def _get_profiler(self, name):
        """Get the Profiler instance.

        This is a thin wrapper around `ru.Profiler()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        path = self._cfg.path or os.getcwd()
        prof = ru.Profiler(name=name, ns='radical.pilot', path=path)

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



    # --------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None):

        return rpu.fetch_profiles(self._uid, tgt=tgt, skip_existing=True,
                                  rep=self._rep)


    # --------------------------------------------------------------------------
    #
    def fetch_logfiles(self, tgt=None):

        return rpu.fetch_logfiles(self._uid, tgt=tgt, skip_existing=True,
                                  rep=self._rep)


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
                fs_url = ru.Url(rcfg['filesystem_endpoint'])

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
                    # FIXME "remote sandbox expansion not yet supported"
                    out, err, ret = ru.sh_callout(' echo "WORKDIR: %s"' %
                                                 sandbox_raw, shell=True)
                    if ret or 'WORKDIR:' not in out:
                        raise RuntimeError("workdir expansion failed: %s [%s]"
                                           % (out, err))

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
    def _get_session_sandbox(self, pilot):

        # FIXME: this should get 'resource, schema=None' as parameters

        resource = pilot['description'].get('resource')

        if not resource:
            raise ValueError('Cannot get session sandbox w/o resource target')

        with self._cache_lock:

            if resource not in self._cache['session_sandbox']:

                # cache miss
                resource_sandbox      = self._get_resource_sandbox(pilot)
                session_sandbox       = ru.Url(resource_sandbox)
                session_sandbox.path += '/%s' % self.uid

                self._cache['session_sandbox'][resource] = session_sandbox

            return self._cache['session_sandbox'][resource]


    # --------------------------------------------------------------------------
    #
    def _get_pilot_sandbox(self, pilot):

        # FIXME: this should get 'pid, resource, schema=None' as parameters

        pilot_sandbox = pilot.get('pilot_sandbox')
        if str(pilot_sandbox):
            return ru.Url(pilot_sandbox)

        pid = pilot['uid']
        with self._cache_lock:

            if pid not in self._cache['pilot_sandbox']:

                # cache miss
                session_sandbox     = self._get_session_sandbox(pilot)
                pilot_sandbox       = ru.Url(session_sandbox)
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
                endpoint_fs       = ru.Url(resource_sandbox)
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

        js_url  = ru.Url(rcfg.get('job_manager_endpoint'))
        js_hop  = ru.Url(rcfg.get('job_manager_hop') or js_url)

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


# pylint: disable=protected-access,unused-argument

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import copy
import glob
import time

import threading as mt

import radical.utils                as ru
import radical.saga                 as rs
import radical.saga.filesystem      as rsfs
import radical.saga.utils.pty_shell as rsup

from . import constants as rpc
from . import utils     as rpu


# ------------------------------------------------------------------------------
#
class Session(rs.Session):
    '''
    A Session is the root object of all RP objects in an application instance:
    it holds :class:`radical.pilot.PilotManager` and
    :class:`radical.pilot.TaskManager` instances which in turn hold
    :class:`radical.pilot.Pilot` and :class:`radical.pilot.Task`
    instances, and several other components which operate on those stateful
    entities.
    '''

    # In that role, the session will create a special pubsub channel `heartbeat`
    # which is used by all components in its hierarchy to exchange heartbeat
    # messages.  Those messages are used to watch component health - if
    # a (parent or child) component fails to send heartbeats for a certain
    # amount of time, it is considered dead and the process tree will terminate.
    # That heartbeat management is implemented in the `ru.Heartbeat` class.
    # Only primary sessions instantiate a heartbeat channel (i.e., only the root
    # sessions of RP client or agent modules), but all components need to call
    # the sessions `heartbeat()` method at regular intervals.

    # the reporter is an applicataion-level singleton
    _reporter = None

    # --------------------------------------------------------------------------
    #
    def __init__(self, service_url=None, uid=None, cfg=None, _primary=True,
                 **close_options):
        '''
        Creates a new session.  A new Session instance is created and
        stored in the database.

        **Arguments:**
            * **service_url** (`string`): The Bridge Service URL.
              If none is given, RP uses the environment variable
              RADICAL_PILOT_SERVICE_URL.  If that is not set, an error will be
              raised.

            * **cfg** (`str` or `dict`): a named or instantiated configuration
              to be used for the session.

            * **uid** (`string`): Create a session with this UID.  Session UIDs
              MUST be unique - otherwise they will lead to conflicts in the
              underlying database, resulting in undefined behaviours (or worse).

            * **_primary** (`bool`): only sessions created by the original
              application process (via `rp.Session()`, will create comm bridges
              Secondary session instances are instantiated internally in
              processes spawned (directly or indirectly) by the initial session,
              for example in some of it's components.  A secondary session will
              inherit the original session ID, but will not attempt to create
              a new comm bridge - if such a bridge connection is needed, the
              component will connect to the one created by the primary session.

        If additional key word arguments are provided, they will be used as the
        default arguments to Session.close(). (This can be useful when the
        Session is used as a Python context manager, such that close() is called
        automatically at the end of a ``with`` block.)
        '''
        self._close_options = _CloseOptions(close_options)
        # NOTE: `name` and `cfg` are overloaded, the user cannot point to
        #       a predefined config and amend it at the same time.  This might
        #       be ok for the session, but introduces a minor API inconsistency.
        name = 'default'
        if isinstance(cfg, str):
            name = cfg
            cfg  = None

        self._service = None
        self._closed  = False
        self._primary = _primary
        self._t_start = time.time()

        self._pmgrs   = dict()  # map IDs to pmgr instances
        self._tmgrs   = dict()  # map IDs to tmgr instances
        self._cmgr    = None    # only primary sessions have a cmgr

        self._cfg     = ru.Config('radical.pilot.session',  name=name, cfg=cfg)
        self._rcfgs   = ru.Config('radical.pilot.resource', name='*', expand=False)

        pwd = os.getcwd()

        if not self._cfg.sid:
            if uid:
                self._cfg.sid = uid
            else:
                self._cfg.sid = ru.generate_id('rp.session',
                                               mode=ru.ID_PRIVATE)
        if not self._cfg.base:
            self._cfg.base = pwd

        if not self._cfg.path:
            self._cfg.path = '%s/%s' % (self._cfg.base, self._cfg.sid)

        if not self._cfg.client_sandbox:
            self._cfg.client_sandbox = pwd

        # change RU defaults to point logfiles etc. to the session sandbox
        def_cfg             = ru.DefaultConfig()
        def_cfg.log_dir     = self._cfg.path
        def_cfg.report_dir  = self._cfg.path
        def_cfg.profile_dir = self._cfg.path

        self._uid  = self._cfg.sid

        self._prof = self._get_profiler(name=self._uid)
        self._rep  = self._get_reporter(name=self._uid)
        self._log  = self._get_logger  (name=self._uid,
                                        level=self._cfg.get('debug'))

        self._prof.prof('session_start', uid=self._uid)

        # now we have config and uid - initialize base class (saga session)
        rs.Session.__init__(self, uid=self._uid)

        # cache sandboxes etc.
        self._cache_lock = ru.RLock()
        self._cache      = {'resource_sandbox' : dict(),
                            'session_sandbox'  : dict(),
                            'pilot_sandbox'    : dict(),
                            'client_sandbox'   : self._cfg.client_sandbox,
                            'js_shells'        : dict(),
                            'fs_dirs'          : dict()}

        if self._primary:
            self._rep.info ('<<new session: ')
            self._rep.plain('[%s]' % self._uid)

        # need a service_url to connect to
        if not service_url:
            service_url = self._cfg.service_url

        if not service_url:
            service_url = os.environ.get('RADICAL_PILOT_SERVICE_URL')

        if not service_url:
            # FIXME MongoDB: in this case, start an embedded service
            raise RuntimeError("no service url (set RADICAL_PILOT_SERVICE_URL)")


        self._cfg.service_url = service_url

        if self._primary:

            self._connect_proxy()

        else:
            # a non-primary session will query the same service url to obtain
            # information about the comm channels created by the primary session
            if not self._cfg.proxy:
                self._service = ru.zmq.Client(url=self._cfg.service_url)
                response      = self._service.request('client_lookup',
                                                      {'sid': self._uid})
                self._cfg.proxy = response
                self._log.debug('=== %s: %s', self._primary, self._cfg.proxy)


        # for mostly debug purposes, dump the used session config

        ru.write_json(self._cfg, '%s/%s.cfg' % (self._cfg.path, self._uid))

        # at this point we have a bridge connection, logger, etc, and are done
        self._prof.prof('session_ok', uid=self._uid)

        if self._primary:
            self._rep.ok('>>ok\n')


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
        '''
        Closes the session.  All subsequent attempts access objects attached to
        the session will result in an error.

        **Arguments:**
            * **terminate** (`bool`):
              Shut down all pilots associated with the session.
            * **download** (`bool`):
              Fetch pilot profiles and database entries.

        '''

        # close only once
        if self._closed:
            return

        if self._primary:
            self._rep.info('closing session %s' % self._uid)

        self._log.debug("session %s closing", self._uid)
        self._prof.prof("session_close", uid=self._uid)

        # Merge kwargs with current defaults stored in self._close_options
        self._close_options.update(kwargs)
        self._close_options.verify()

        # to call for `_verify` method and to convert attributes
        # to their types if needed (but None value will stay if it is set)

        options = self._close_options

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

        if self._service:
            try:
                self._log.debug("session %s closes service", self._uid)
                self._service.request('client_unregister',
                                      {'sid': self._uid})
            except:
                pass

        self._log.debug("session %s closed", self._uid)
        self._prof.prof("session_stop", uid=self._uid)
        self._prof.close()

        self._closed = True

        # after all is said and done, we attempt to download the pilot log- and
        # profiles, if so wanted
        if options.download:

            self._prof.prof("session_fetch_start", uid=self._uid)
            self._log.debug('start download')
            tgt = os.getcwd()
            # FIXME: MongoDB
          # self.fetch_json    (tgt='%s/%s' % (tgt, self.uid))
            self.fetch_profiles(tgt=tgt)
            self.fetch_logfiles(tgt=tgt)

            self._prof.prof("session_fetch_stop", uid=self._uid)

        if self._primary:
            self._t_stop = time.time()
            self._rep.info('<<session lifetime: %.1fs'
                          % (self._t_stop - self._t_start))
            self._rep.ok('>>ok\n')

            # dump json
            json = {'session' : self.as_dict(),
                    'pmgr'    : list(),
                    'pilot'   : list(),
                    'tmgr'    : list(),
                    'task'    : list()}

          # json['session']['_id']      = self.uid
            json['session']['type']     = 'session'
            json['session']['uid']      = self.uid
            json['session']['metadata'] = self._metadata

            for fname in glob.glob('%s/pmgr.*.json' % self.path):
                json['pmgr'].append(ru.read_json(fname))

            for fname in glob.glob('%s/pilot.*.json' % self.path):
                json['pilot'].append(ru.read_json(fname))

            for fname in glob.glob('%s/tmgr.*.json' % self.path):
                json['tmgr'].append(ru.read_json(fname))

            for fname in glob.glob('%s/tasks.*.json' % self.path):
                json['task'] += ru.read_json(fname)

            tgt = '%s/%s.json' % (self.path, self.uid)
            ru.write_json(json, tgt)


    # --------------------------------------------------------------------------
    #
    def _connect_proxy(self):

        assert(self._primary)

        # a primary session will create proxy comm channels
        self._rep.info ('<<bridge     : ')
        self._rep.plain('[%s]' % self._cfg.service_url)

        # create/connect bridge handle on primary sessions
        self._service = ru.zmq.Client(url=self._cfg.service_url)
        response      = self._service.request('client_register',
                                              {'sid': self._uid})

        self._cfg.proxy = response
        self._log.debug('=== %s: %s', self._primary, self._cfg.proxy)

        # now that the proxy bridges have been created on the service host,
        # write config files for them so that all components can use them
        for p in self._cfg.proxy:
            ru.write_json('%s.cfg' % p, self._cfg.proxy[p])

        # primary sessions have a component manager which also manages
        # heartbeat.  'self._cmgr.close()` should be called during
        # termination
        self._cmgr = rpu.ComponentManager(self._cfg)
        self._cmgr.start_bridges()
        self._cmgr.start_components()

        # expose the cmgr's heartbeat channel to anyone who wants to use it
        self._cfg.heartbeat = self._cmgr.cfg.heartbeat

        # make sure we send heartbeats to the proxy
        self._run_proxy_hb()

        from . import version_detail as rp_version_detail
        self._log.info('radical.pilot version: %s' % rp_version_detail)
        self._log.info('radical.saga  version: %s' % rs.version_detail)
        self._log.info('radical.utils version: %s' % ru.version_detail)

      # FIXME MONGODB: to json
        self._metadata = {'radical_stack':
                                     {'rp': rp_version_detail,
                                      'rs': rs.version_detail,
                                      'ru': ru.version_detail}}
                                    # 'py': py_version_detail}}

        pwd = self._cfg.path

        # forward any control messages to the proxy
        def fwd_control(topic, msg):
            self._log.debug('=== fwd control %s: %s', topic, msg)
            self._proxy_ctrl_pub.put(rpc.PROXY_CONTROL_PUBSUB, msg)

        self._proxy_ctrl_pub = ru.zmq.Publisher(rpc.PROXY_CONTROL_PUBSUB, path=pwd)
        self._ctrl_sub = ru.zmq.Subscriber(rpc.CONTROL_PUBSUB, path=pwd)
        self._ctrl_sub.subscribe(rpc.CONTROL_PUBSUB, fwd_control)


        # collect any state updates from the proxy
        def fwd_state(topic, msg):
            self._log.debug('=== fwd state   %s: %s', topic, msg)
            self._state_pub.put(topic, msg)

        self._state_pub = ru.zmq.Publisher(rpc.STATE_PUBSUB, path=pwd)
        self._proxy_state_sub = ru.zmq.Subscriber(rpc.PROXY_STATE_PUBSUB, path=pwd)
        self._proxy_state_sub.subscribe(rpc.PROXY_STATE_PUBSUB, fwd_state)


    # --------------------------------------------------------------------------
    #
    def _run_proxy_hb(self):

        self._proxy_heartbeat_thread = mt.Thread(target=self._proxy_hb)
        self._proxy_heartbeat_thread.daemon = True
        self._proxy_heartbeat_thread.start()


    # --------------------------------------------------------------------------
    #
    def _proxy_hb(self):

        while True:

            self._service.request('client_heartbeat', {'sid': self._uid})
            time.sleep(20)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        '''
        Returns a Python dictionary representation of the object.
        '''

        object_dict = {
            "uid"        : self._uid,
          # "created"    : self.created,
          # "connected"  : self.connected,
          # "closed"     : self.closed,
            "service_url": str(self.service_url),
            "cfg"        : copy.deepcopy(self._cfg)
        }
        return object_dict


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        '''Returns a string representation of the object.
        '''
        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    @property
    def primary(self):
        return self._primary


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def base(self):
        return self._cfg.base


    # --------------------------------------------------------------------------
    #
    @property
    def path(self):
        return self._cfg.path


    # --------------------------------------------------------------------------
    #
    @property
    def service_url(self):
        return self._cfg.service_url


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):
        return self._cfg


    # --------------------------------------------------------------------------
    #
    @property
    def cmgr(self):
        return self._cmgr


    # --------------------------------------------------------------------------
    #
    def _get_logger(self, name, level=None):
        '''
        This is a thin wrapper around `ru.Logger()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        '''
        return ru.Logger(name=name, ns='radical.pilot', path=self._cfg.path,
                         targets=['.'], level=level)


    # --------------------------------------------------------------------------
    #
    def _get_reporter(self, name):
        '''
        This is a thin wrapper around `ru.Reporter()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        '''

        if not self._reporter:
            self._reporter = ru.Reporter(name=name, ns='radical.pilot',
                                         path=self._cfg.path)
        return self._reporter


    # --------------------------------------------------------------------------
    #
    def _get_profiler(self, name):
        '''
        This is a thin wrapper around `ru.Profiler()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        '''

        prof = ru.Profiler(name=name, ns='radical.pilot', path=self._cfg.path)

        return prof


    # --------------------------------------------------------------------------
    #
    def inject_metadata(self, metadata):
        '''
        Insert (experiment) metadata into an active session
        RP stack version info always get added.
        '''

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


    # --------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        '''
        Lists the unique identifiers of all :class:`radical.pilot.PilotManager`
        instances associated with this session.

        **Returns:**
            * A list of :class:`radical.pilot.PilotManager` uids
              (`list` of `strings`).
        '''

        return list(self._pmgrs.keys())


    # --------------------------------------------------------------------------
    #
    def get_pilot_managers(self, pmgr_uids=None):
        '''
        returns known PilotManager(s).

        **Arguments:**

            * **pmgr_uids** [`string`]:
              unique identifier of the PilotManager we want

        **Returns:**
            * One or more [:class:`radical.pilot.PilotManager`] objects.
        '''

        return_scalar = False
        if not isinstance(pmgr_uids, list):
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


    # --------------------------------------------------------------------------
    #
    def list_task_managers(self):
        '''
        Lists the unique identifiers of all :class:`radical.pilot.TaskManager`
        instances associated with this session.

        **Returns:**
            * A list of :class:`radical.pilot.TaskManager` uids (`list` of `strings`).
        '''

        return list(self._tmgrs.keys())


    # --------------------------------------------------------------------------
    #
    def get_task_managers(self, tmgr_uids=None):
        '''
        returns known TaskManager(s).

        **Arguments:**

            * **tmgr_uids** [`string`]:
              unique identifier of the TaskManager we want

        **Returns:**
            * One or more [:class:`radical.pilot.TaskManager`] objects.
        '''

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
        '''
        Returns a list of known resource labels which can be used in a pilot
        description.
        '''

        resources = list()
        for domain in self._rcfgs:
            for host in self._rcfgs[domain]:
                resources.append('%s.%s' % (domain, host))

        return sorted(resources)


    # --------------------------------------------------------------------------
    #
    def get_resource_config(self, resource, schema=None):
        '''
        Returns a dictionary of the requested resource config
        '''

        domain, host = resource.split('.', 1)
        if domain not in self._rcfgs:
            raise RuntimeError("Resource domain '%s' is unknown." % domain)

        if host not in self._rcfgs[domain]:
            raise RuntimeError("Resource host '%s' unknown." % host)

        resource_cfg = copy.deepcopy(self._rcfgs[domain][host])

        if  not schema:
            if 'schemas' in resource_cfg:
                schema = resource_cfg['schemas'][0]

        if  schema:
            if  schema not in resource_cfg:
                raise RuntimeError("schema %s unknown for resource %s"
                                  % (schema, resource))

            for key in resource_cfg[schema]:
                # merge schema specific resource keys into the
                # resource config
                resource_cfg[key] = resource_cfg[schema][key]

        resource_cfg.label = resource
        return resource_cfg


    # --------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_profiles(self._uid, tgt=tgt, session=self)


    # --------------------------------------------------------------------------
    #
    def fetch_logfiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_logfiles(self._uid, tgt=tgt, session=self)


    # --------------------------------------------------------------------------
    #
    def fetch_json(self, tgt=None, fetch_client=False):

        return rpu.fetch_json(self._uid, tgt=tgt, session=self)


    # --------------------------------------------------------------------------
    #
    def _get_client_sandbox(self):
        '''
        For the session in the client application, this is os.getcwd().  For the
        session in any other component, specifically in pilot components, the
        client sandbox needs to be read from the session config (or pilot
        config).  The latter is not yet implemented, so the pilot can not yet
        interpret client sandboxes.  Since pilot-side stagting to and from the
        client sandbox is not yet supported anyway, this seems acceptable
        (FIXME).
        '''

        return self._cache['client_sandbox']


    # --------------------------------------------------------------------------
    #
    def _get_resource_sandbox(self, pilot):
        '''
        for a given pilot dict, determine the global RP sandbox, based on the
        pilot's 'resource' attribute.
        '''

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
                    for k,v in pilot['description'].items():
                        if v is None:
                            v = ''
                        expand['pd.%s' % k] = v
                        if isinstance(v, str):
                            expand['pd.%s' % k.upper()] = v.upper()
                            expand['pd.%s' % k.lower()] = v.lower()
                        else:
                            expand['pd.%s' % k.upper()] = v
                            expand['pd.%s' % k.lower()] = v
                    sandbox_raw = sandbox_raw % expand

                if '_' in sandbox_raw and 'ornl' in resource:
                    sandbox_raw = sandbox_raw.split('_')[0]

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
                js_url.hostname = 'localhost'

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
            if  pid in self._cache['pilot_sandbox']:
                return self._cache['pilot_sandbox'][pid]

        # cache miss
        session_sandbox     = self._get_session_sandbox(pilot)
        pilot_sandbox       = rs.Url(session_sandbox)
        pilot_sandbox.path += '/%s/' % pilot['uid']

        with self._cache_lock:
            self._cache['pilot_sandbox'][pid] = pilot_sandbox

        return pilot_sandbox


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
        '''
        get job service endpoint and hop URL for the pilot's target resource.
        '''

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


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def autopilot(user, passwd):

        try:
            import github3
        except ImportError:
            print('ERROR: github3 library is not available')
            return
        import random

        labels = 'type:autopilot'
        titles = ['+++ Out of Cheese Error +++',
                  '+++ Redo From Start! +++',
                  '+++ Mr. Jelly! Mr. Jelly! +++',
                  '+++ Melon melon melon',
                  '+++ Wahhhhhhh! Mine! +++',
                  '+++ Divide By Cucumber Error +++',
                  '+++ Please Reinstall Universe And Reboot +++',
                  '+++ Whoops! Here comes the cheese! +++',
                  '+++ End of Cheese Error +++',
                  '+++ Can Not Find Drive Z: +++',
                  '+++ Unknown Application Error +++',
                  '+++ Please Reboot Universe +++',
                  '+++ Year Of The Sloth +++',
                  '+++ error of type 5307 has occured +++',
                  '+++ Eternal domain error +++',
                  '+++ Error at Address Number 6, Treacle Mine Road +++']

        def excuse():
            cmd_fetch  = "telnet bofh.jeffballard.us 666 2>&1 "
            cmd_filter = "grep 'Your excuse is:' | cut -f 2- -d :"
            out        = ru.sh_callout("%s | %s" % (cmd_fetch, cmd_filter),
                                       shell=True)[0]
            return out.strip()

        github = github3.login(user, passwd)
        repo   = github.repository("radical-cybertools", "radical.pilot")

        title = 'autopilot: %s' % titles[random.randint(0, len(titles) - 1)]

        print('----------------------------------------------------')
        print('autopilot')

        for issue in repo.issues(labels=labels, state='open'):
            if issue.title == title:
                reply = 'excuse: %s' % excuse()
                issue.create_comment(reply)
                print('  resolve: %s' % reply)
                return

        # issue not found - create
        body  = 'problem: %s' % excuse()
        issue = repo.create_issue(title=title, body=body, labels=[labels],
                                  assignee=user)
        print('  issue  : %s' % title)
        print('  problem: %s' % body)
        print('----------------------------------------------------')


# ------------------------------------------------------------------------------
#
class _CloseOptions(ru.TypedDict):
    '''
    Options and validation for Session.close().

    **Arguments:**
        * **download** (`bool`):
          Fetch pilot profiles and database entries. (default False)
        * **terminate** (`bool`):
          Shut down all pilots associated with the session. (default True)
    '''

    _schema = {
        'download' : bool,
        'terminate': bool
    }

    _defaults = {
        'download' : False,
        'terminate': True
    }


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        if self.get('cleanup') and not self.get('terminate'):
            self.terminate = True


# ------------------------------------------------------------------------------


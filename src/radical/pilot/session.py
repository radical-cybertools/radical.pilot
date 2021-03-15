# pylint: disable=protected-access,unused-argument

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import copy

import radical.utils                as ru
import radical.saga                 as rs
import radical.saga.filesystem      as rsfs
import radical.saga.utils.pty_shell as rsup

from .constants import RESOURCE_CONFIG_LABEL_DEFAULT
from .db        import DBSession
from .          import utils as rpu


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
    def __init__(self, dburl=None, uid=None, cfg=None, _primary=True):
        '''
        Creates a new session.  A new Session instance is created and
        stored in the database.

        **Arguments:**
            * **dburl** (`string`): The MongoDB URL.  If none is given,
              RP uses the environment variable RADICAL_PILOT_DBURL.  If that is
              not set, an error will be raised.

            * **cfg** (`str` or `dict`): a named or instantiated configuration
              to be used for the session.

            * **uid** (`string`): Create a session with this UID.  Session UIDs
              MUST be unique - otherwise they will lead to conflicts in the
              underlying database, resulting in undefined behaviours (or worse).

            * **_primary** (`bool`): only sessions created by the original
              application process (via `rp.Session()`, will connect to the  DB.
              Secondary session instances are instantiated internally in
              processes spawned (directly or indirectly) by the initial session,
              for example in some of it's components.  A secondary session will
              inherit the original session ID, but will not attempt to create
              a new DB collection - if such a DB connection is needed, the
              component needs to establish that on its own.
        '''

        # NOTE: `name` and `cfg` are overloaded, the user cannot point to
        #       a predefined config and amend it at the same time.  This might
        #       be ok for the session, but introduces a minor API inconsistency.
        name = 'default'
        if isinstance(cfg, str):
            name = cfg
            cfg  = None

        self._dbs     = None
        self._closed  = False
        self._primary = _primary

        self._pmgrs   = dict()  # map IDs to pmgr instances
        self._tmgrs   = dict()  # map IDs to tmgr instances
        self._cmgr    = None    # only primary sessions have a cmgr

        self._cfg     = ru.Config('radical.pilot.session',  name=name, cfg=cfg)
        self._rcfgs   = ru.Config('radical.pilot.resource', name='*', expand=False)

        if _primary:

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

        else:
            for k in ['sid', 'base', 'path']:
                assert(k in self._cfg), 'non-primary session misses %s' % k

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

        from . import version_detail as rp_version_detail
        self._log.info('radical.pilot version: %s' % rp_version_detail)
        self._log.info('radical.saga  version: %s' % rs.version_detail)
        self._log.info('radical.utils version: %s' % ru.version_detail)

        self._prof.prof('session_start', uid=self._uid, msg=int(_primary))

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

        if _primary:
            self._initialize_primary(dburl)

        # at this point we have a DB connection, logger, etc, and are done
        self._prof.prof('session_ok', uid=self._uid, msg=int(_primary))


    # --------------------------------------------------------------------------
    def _initialize_primary(self, dburl):

        self._rep.info ('<<new session: ')
        self._rep.plain('[%s]' % self._uid)

        # create db connection - need a dburl to connect to
        if not dburl: dburl = self._cfg.dburl
        if not dburl: dburl = self._cfg.default_dburl
        if not dburl: raise RuntimeError("no db URL (set RADICAL_PILOT_DBURL)")

        self._cfg.dburl = dburl

        dburl_no_passwd = ru.Url(dburl)
        if dburl_no_passwd.get_password():
            dburl_no_passwd.set_password('****')

        self._rep.info ('<<database   : ')
        self._rep.plain('[%s]'    % dburl_no_passwd)
        self._log.info('dburl %s' % dburl_no_passwd)

        # create/connect database handle on primary sessions
        try:
            self._dbs = DBSession(sid=self.uid, dburl=dburl,
                                  cfg=self._cfg, log=self._log)

            py_version_detail = sys.version.replace("\n", " ")
            from . import version_detail as rp_version_detail

            self.inject_metadata({'radical_stack':
                                         {'rp': rp_version_detail,
                                          'rs': rs.version_detail,
                                          'ru': ru.version_detail,
                                          'py': py_version_detail}})
        except Exception as e:
            self._rep.error(">>err\n")
            self._log.exception('session create failed [%s]' %
                    dburl_no_passwd)
            raise RuntimeError ('session create failed [%s]' %
                    dburl_no_passwd) from e

        # primary sessions have a component manager which also manages
        # heartbeat.  'self._cmgr.close()` should be called during termination
        self._cmgr = rpu.ComponentManager(self._cfg)
        self._cmgr.start_bridges()
        self._cmgr.start_components()

        # expose the cmgr's heartbeat channel to anyone who wants to use it
        self._cfg.heartbeat = self._cmgr.cfg.heartbeat

        self._rec = False
        if self._cfg.record:

            # append session ID to recording path
            self._rec = "%s/%s" % (self._rec, self._uid)

            # create recording path and record session
            os.system('mkdir -p %s' % self._rec)
            ru.write_json({'dburl': str(self.dburl)},
                          "%s/session.json" % self._rec)
            self._log.info("recording session in %s" % self._rec)

        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    # context manager `with` clause
    # FIXME: cleanup_on_close, terminate_on_close attributes?
    #
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close(download=True)


    # --------------------------------------------------------------------------
    #
    def close(self, cleanup=False, terminate=True, download=False):
        '''

        Closes the session.  All subsequent attempts access objects attached to
        the session will result in an error. If cleanup is set to True,
        the session data is removed from the database.

        **Arguments:**
            * **cleanup**   (`bool`):
              Remove session from MongoDB (implies * terminate)
            * **terminate** (`bool`):
              Shut down all pilots associated with the session.
            * **download** (`bool`):
              Fetch pilot profiles and database entries.

        '''

        # close only once
        if self._closed:
            return

        self._rep.info('closing session %s' % self._uid)
        self._log.debug("session %s closing", self._uid)
        self._prof.prof("session_close", uid=self._uid)

        # set defaults
        if cleanup   is None: cleanup   = True
        if terminate is None: terminate = True

        if  cleanup:
            # cleanup implies terminate
            terminate = True

        for tmgr_uid, tmgr in self._tmgrs.items():
            self._log.debug("session %s closes tmgr   %s", self._uid, tmgr_uid)
            tmgr.close()
            self._log.debug("session %s closed tmgr   %s", self._uid, tmgr_uid)

        for pmgr_uid, pmgr in self._pmgrs.items():
            self._log.debug("session %s closes pmgr   %s", self._uid, pmgr_uid)
            pmgr.close(terminate=terminate)
            self._log.debug("session %s closed pmgr   %s", self._uid, pmgr_uid)

        if self._cmgr:
            self._cmgr.close()

        if self._dbs:
            self._log.debug("session %s closes db (%s)", self._uid, cleanup)
            self._dbs.close(delete=cleanup)

        self._log.debug("session %s closed (delete=%s)", self._uid, cleanup)
        self._prof.prof("session_stop", uid=self._uid)
        self._prof.close()

        self._closed = True

        # after all is said and done, we attempt to download the pilot log- and
        # profiles, if so wanted
        if download:

            self._prof.prof("session_fetch_start", uid=self._uid)
            self._log.debug('start download')
            tgt = os.getcwd()
            self.fetch_json    (tgt='%s/%s' % (tgt, self.uid))
            self.fetch_profiles(tgt=tgt)
            self.fetch_logfiles(tgt=tgt)

            self._prof.prof("session_fetch_stop", uid=self._uid)

        self._rep.info('<<session lifetime: %.1fs' % (self.closed - self.created))
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        '''
        Returns a Python dictionary representation of the object.
        '''

        object_dict = {
            "uid"       : self._uid,
            "created"   : self.created,
            "connected" : self.connected,
            "closed"    : self.closed,
            "dburl"     : str(self.dburl),
            "cfg"       : copy.deepcopy(self._cfg)
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
    def dburl(self):
        return self._cfg.dburl


    # --------------------------------------------------------------------------
    #
    def get_db(self):

        if self._dbs: return self._dbs.get_db()
        else        : return None


    # --------------------------------------------------------------------------
    #
    @property
    def primary(self):
        return self._primary


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):
        return self._cfg


    # --------------------------------------------------------------------------
    #
    @property
    def cmgr(self):
        assert(self._primary)
        return self._cmgr


    # --------------------------------------------------------------------------
    #
    @property
    def created(self):
        '''Returns the UTC date and time the session was created.
        '''
        if self._dbs: return self._dbs.created
        else        : return None


    # --------------------------------------------------------------------------
    #
    @property
    def connected(self):
        '''
        Return time when the session connected to the DB
        '''

        if self._dbs: return self._dbs.connected
        else        : return None


    # --------------------------------------------------------------------------
    #
    @property
    def is_connected(self):

        return self._dbs.is_connected


    # --------------------------------------------------------------------------
    #
    @property
    def closed(self):
        '''
        Returns the time of closing
        '''
        if self._dbs: return self._dbs.closed
        else        : return None


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

        if self._dbs and self._dbs._c:
            self._dbs._c.update({'type'  : 'session',
                                 "uid"   : self.uid},
                                {"$push" : {"metadata": metadata}})


    # --------------------------------------------------------------------------
    #
    def _register_pmgr(self, pmgr):

        self._dbs.insert_pmgr(pmgr.as_dict())
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

        self._dbs.insert_tmgr(tmgr.as_dict())
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
    def add_resource_config(self, resource_config):
        '''
        Adds a new :class:`ru.Config` to the session's dictionary of known
        resources, or accept a string which points to a configuration file.

        For example::

               rc = ru.Config(path='./mycluster.json')
               rc.label                = 'local.mycluster'
               rc.job_manager_endpoint = 'ssh+pbs://mycluster'
               rc.filesystem_endpoint  = 'sftp://mycluster'
               rc.default_queue        = 'private'

               session = rp.Session()
               session.add_resource_config(rc)

               pd = rp.PilotDescription()
               pd.resource = 'local.mycluster'
               pd.cores    = 16
               pd.runtime  = 5 # minutes

               pilot = pm.submit_pilots(pd)

        NOTE:  if <resource_config>.label is not set, then the default value
               is assigned - `rp.RESOURCE_CONFIG_LABEL_DEFAULT`
        '''

        if isinstance(resource_config, str):

            rcs = ru.Config('radical.pilot.resource', name=resource_config)
            domain = os.path.splitext(os.path.basename(resource_config))[0]
            if domain not in self._rcfgs:
                self._rcfgs[domain] = {}

            for rc in rcs:
                self._log.info('load rcfg for "%s.%s"' % (domain, rc))
                self._rcfgs[domain][rc] = rcs[rc].as_dict()

        else:

            if not resource_config.label:
                resource_config.label = RESOURCE_CONFIG_LABEL_DEFAULT

            elif '.' not in resource_config.label:
                raise ValueError('Resource config label format should be '
                                 '"<domain>.<host>"')

            domain, host = resource_config.label.split('.', 1)
            self._log.debug('load rcfg for "%s.%s"', (domain, host))
            self._rcfgs.setdefault(domain, {})[host] = resource_config.as_dict()


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

        return resource_cfg


    # --------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_profiles(self._uid, dburl=self.dburl, tgt=tgt,
                                  session=self)


    # --------------------------------------------------------------------------
    #
    def fetch_logfiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_logfiles(self._uid, dburl=self.dburl, tgt=tgt,
                                  session=self)


    # --------------------------------------------------------------------------
    #
    def fetch_json(self, tgt=None, fetch_client=False):

        return rpu.fetch_json(self._uid, dburl=self.dburl, tgt=tgt,
                              session=self)


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

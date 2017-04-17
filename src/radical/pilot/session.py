
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import glob
import copy
import pprint
import threading

import radical.utils        as ru
import saga                 as rs
import saga.utils.pty_shell as rsup

from . import utils         as rpu
from . import states        as rps
from . import constants     as rpc
from . import types         as rpt

from .unit_manager    import UnitManager
from .pilot_manager   import PilotManager
from .resource_config import ResourceConfig
from .db              import DBSession

from .utils import version_detail as rp_version_detail


# ------------------------------------------------------------------------------
#
class Session(rs.Session):
    """
    A Session encapsulates a RADICAL-Pilot instance and is the *root* object
    for all other RADICAL-Pilot objects. 

    A Session holds :class:`radical.pilot.PilotManager` and
    :class:`radical.pilot.UnitManager` instances which in turn hold
    :class:`radical.pilot.ComputePilot` and :class:`radical.pilot.ComputeUnit`
    instances.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, dburl=None, uid=None, cfg=None, _connect=True):
        """
        Creates a new session.  A new Session instance is created and 
        stored in the database.

        **Arguments:**
            * **dburl** (`string`): The MongoDB URL.  If none is given,
              RP uses the environment variable RADICAL_PILOT_DBURL.  If that is
              not set, an error will be raises.

            * **uid** (`string`): Create a session with this UID.  
              *Only use this when you know what you are doing!*

        **Returns:**
            * A new Session instance.

        **Raises:**
            * :class:`radical.pilot.DatabaseError`

        """

        self._dh        = ru.DebugHelper()
        self._valid     = True

        # class state
        self._dbs         = None
        self._uid         = None
        self._dburl       = None
        self._controller  = None
        self._reconnected = False

        self._cache       = dict()  # cache sandboxes etc.
        self._cache_lock  = threading.RLock()

        self._cache['global_sandbox']  = dict()
        self._cache['session_sandbox'] = dict()
        self._cache['pilot_sandbox']   = dict()

        # before doing anything else, set up the debug helper for the lifetime
        # of the session.
        self._debug_helper = ru.DebugHelper()

        # Dictionaries holding all manager objects created during the session.
        self._pmgrs = dict()
        self._umgrs = dict()

        # The resource configuration dictionary associated with the session.
        self._resource_configs = {}

        # initialize the base class (saga session)
        rs.Session.__init__(self)

        # if a config is given, us its values:
        if cfg:
            self._cfg = copy.deepcopy(cfg)
        else:
            # otherwise we need a config
            self._cfg = ru.read_json("%s/configs/session_%s.json" \
                    % (os.path.dirname(__file__),
                       os.environ.get('RADICAL_PILOT_SESSION_CFG', 'default')))

        # fall back to config data where possible
        # sanity check on parameters
        if not uid : uid = self._cfg.get('session_id')
        if uid:
            self._uid         = uid
            self._reconnected = True
        else:
            # generate new uid, reset all other ID counters
            # FIXME: this will screw up counters for *concurrent* sessions, 
            #        as the ID generation is managed in a process singleton.
            self._uid = ru.generate_id('rp.session',  mode=ru.ID_PRIVATE)
            ru.reset_id_counters(prefix='rp.session', reset_all_others=True)

        if not self._cfg.get('owner'):
            self._cfg['owner'] = self._uid

        if not self._cfg.get('logdir'):
            self._cfg['logdir'] = '%s/%s' % (os.getcwd(), self._uid)

        self._logdir = self._cfg['logdir']
        self._log    = self._get_logger(self._cfg['owner'], self._cfg.get('debug'))

        if _connect:
            # we need a dburl to connect to.
        
            if not dburl:
                dburl = os.environ.get("RADICAL_PILOT_DBURL")

            if not dburl:
                dburl = self._cfg.get('default_dburl')

            if not dburl:
                dburl = self._cfg.get('dburl')

            if not dburl:
                # we forgive missing dburl on reconnect, but not otherwise
                raise RuntimeError("no database URL (set RADICAL_PILOT_DBURL)")  


        self._dburl = ru.Url(dburl)

        # ----------------------------------------------------------------------
        # create new session
        if _connect:
            self._log.info("using database %s" % self._dburl)

            # if the database url contains a path element, we interpret that as
            # database name (without the leading slash)
            if  not self._dburl.path         or \
                self._dburl.path[0]   != '/' or \
                len(self._dburl.path) <=  1  :
                if not uid:
                    # we fake reconnnect if no DB is available -- but otherwise we
                    # really really need a db connection...
                    raise ValueError("incomplete DBURL '%s' no db name!" % self._dburl)

        # initialize profiling
        self.prof = self._get_profiler(self._cfg['owner'])

        if self._reconnected:
            self.prof.prof('reconnect session', uid=self._uid)

        else:
            self.prof.prof('start session', uid=self._uid)
            self._log.report.info ('<<new session: ')
            self._log.report.plain('[%s]' % self._uid)
            self._log.report.info ('<<database   : ')
            self._log.report.plain('[%s]' % self._dburl)

        self._load_resource_configs()

        self._rec = os.environ.get('RADICAL_PILOT_RECORD_SESSION')
        if self._rec:
            # NOTE: Session recording cannot handle reconnected sessions, yet.
            #       We thus turn it off here with a warning
            if self._reconnected:
                self._log.warn("no session recording on reconnected session")

            else:
                # append session ID to recording path
                self._rec = "%s/%s" % (self._rec, self._uid)

                # create recording path and record session
                os.system('mkdir -p %s' % self._rec)
                ru.write_json({'dburl': str(self.dburl)}, 
                              "%s/session.json" % self._rec)
                self._log.info("recording session in %s" % self._rec)


        # create/connect database handle
        try:
            self._dbs = DBSession(sid=self.uid, dburl=str(self._dburl),
                                  cfg=self._cfg, logger=self._log, 
                                  connect=_connect)

            # from here on we should be able to close the session again
            self._log.info("New Session created: %s." % self.uid)

        except Exception, ex:
            self._log.report.error(">>err\n")
            self._log.exception('session create failed')
            raise RuntimeError("Couldn't create new session (database URL '%s' incorrect?): %s" \
                            % (dburl, ex))  

        # FIXME: make sure the above code results in a usable session on
        #        reconnect
        self._log.report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    @property
    def ctrl_cfg(self):

        if not self._controller:
            self._create_controller()

        cfg = self._controller.ctrl_cfg  # this is a deep copy
        cfg['session_id'] = self._uid
        cfg['dburl']      = str(self._dburl)

        return cfg


    # ---------------------------------------------------------------------------
    #
    def _create_controller(self):

        # not all sessions need a controller for bridges and components.  Its
        # really only required once we (i) create a unit manager, (ii) create
        # a pilot manager, or (iii) create an agent instance.  All other
        # sessions will not start any bridges etc.  Thus we make the startup of
        # the controller explicit.  Once the controller is up, we merge the
        # bridge addresses etc. into the session config.

        if not self._controller:
            self._cfg['session_id'] = self._uid
            self._cfg['dburl']      = str(self._dburl)
            self._controller = rpu.Controller(cfg=self._cfg, session=self)
          # ru.dict_merge(self._cfg, self._controller.ctrl_cfg, ru.PRESERVE)

        # we pass session_id and db_url as part of the controller cfg


    # --------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __enter__(self):
        return self


    # --------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __exit__(self, type, value, traceback):
        self.close()


    # --------------------------------------------------------------------------
    #
    def _is_valid(self):
        if not self._valid:
            raise RuntimeError("instance was closed")


    # --------------------------------------------------------------------------
    #
    def _load_resource_configs(self):

        # Loading all "default" resource configurations
        module_path  = os.path.dirname(os.path.abspath(__file__))
        default_cfgs = "%s/configs/resource_*.json" % module_path
        config_files = glob.glob(default_cfgs)

        for config_file in config_files:

            try:
                self._log.info("Load resource configurations from %s" % config_file)
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e:
                self._log.exception("skip config file %s: %s" % (config_file, e))
                raise RuntimeError('config error (%s) - abort' % e)

            for rc in rcs:
                self._log.info("Load resource configurations for %s" % rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        home         = os.environ.get('HOME', '')
        user_cfgs    = "%s/.radical/pilot/configs/resource_*.json" % home
        config_files = glob.glob(user_cfgs)

        for config_file in config_files:

            try:
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e:
                self._log.exception("skip config file %s: %s" % (config_file, e))
                raise RuntimeError('config error (%s) - abort' % e)

            for rc in rcs:
                self._log.info("Load resource configurations for %s" % rc)

                if rc in self._resource_configs:
                    # config exists -- merge user config into it
                    ru.dict_merge(self._resource_configs[rc],
                                  rcs[rc].as_dict(),
                                  policy='overwrite')
                else:
                    # new config -- add as is
                    self._resource_configs[rc] = rcs[rc].as_dict() 

        default_aliases = "%s/configs/resource_aliases.json" % module_path
        self._resource_aliases = ru.read_json_str(default_aliases)['aliases']

        # check if we have aliases to merge
        usr_aliases = '%s/.radical/pilot/configs/resource_aliases.json' % home
        if os.path.isfile(usr_aliases):
            ru.dict_merge(self._resource_aliases,
                          ru.read_json_str(usr_aliases).get('aliases', {}),
                          policy='overwrite')

        self.prof.prof('configs parsed', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def close(self, cleanup=None, terminate=None, delete=None):
        """Closes the session.

        All subsequent attempts access objects attached to the session will 
        result in an error. If cleanup is set to True (default) the session
        data is removed from the database.

        **Arguments:**
            * **cleanup** (`bool`): Remove session from MongoDB (implies * terminate)
            * **terminate** (`bool`): Shut down all pilots associated with the session. 

        **Raises:**
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """

        if not self._valid:
            return

        self._log.report.info('closing session %s' % self._uid)
        self._log.debug("session %s closing" % (str(self._uid)))
        self.prof.prof("close", uid=self._uid)

        # set defaults
        if cleanup   == None: cleanup   = True
        if terminate == None: terminate = True

        # we keep 'delete' for backward compatibility.  If it was set, and the
        # other flags (cleanup, terminate) are as defaulted (True), then delete
        # will supercede them.  Delete is considered deprecated though, and
        # we'll thus issue a warning.
        if delete != None:
            if  cleanup == True and terminate == True:
                cleanup   = delete
                terminate = delete
                self._log.warning("'delete' flag on session is deprecated. " \
                             "Please use 'cleanup' and 'terminate' instead!")

        if  cleanup:
            # cleanup implies terminate
            terminate = True

        for umgr_uid, umgr in self._umgrs.iteritems():
            self._log.debug("session %s closes umgr   %s", self._uid, umgr_uid)
            umgr.close()
            self._log.debug("session %s closed umgr   %s", self._uid, umgr_uid)

        for pmgr_uid, pmgr in self._pmgrs.iteritems():
            self._log.debug("session %s closes pmgr   %s", self._uid, pmgr_uid)
            pmgr.close(terminate=terminate)
            self._log.debug("session %s closed pmgr   %s", self._uid, pmgr_uid)

        # stop the controller
        if self._controller:
            self._log.debug("session %s closes ctrl   %s", self._uid, self._controller.uid)
            self._controller.stop()  
            self._log.debug("session %s closed ctrl   %s", self._uid, self._controller.uid)

        self.prof.prof("closing", msg=cleanup, uid=self._uid)
        if self._dbs:
            self._log.debug("session %s closes db (%s)", self._uid, cleanup)
            self._dbs.close(delete=cleanup)
        self._log.debug("session %s closed (delete=%s)", self._uid, cleanup)
        self.prof.prof("closed", uid=self._uid)
        self.prof.close()

        self._valid = False
        self._log.report.info('<<session lifetime: %.1fs' % (self.closed - self.created))
        self._log.report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """

        self._is_valid()

        object_dict = {
            "uid"       : self._uid,
            "created"   : self.created,
            "connected" : self.connected,
            "closed"    : self.closed,
            "dburl"     : str(self.dburl),
            "cfg"       : copy.deepcopy(self._cfg),
            "ctrl_cfg"  : self.ctrl_cfg # this is a deep copy
        }
        return object_dict


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def dburl(self):
        return self._dburl


    # --------------------------------------------------------------------------
    #
    def get_db(self):

        if self._dbs: return self._dbs.get_db()
        else        : return None


    
    # --------------------------------------------------------------------------
    #
    @property
    def created(self):
        """Returns the UTC date and time the session was created.
        """
        if self._dbs: return self._dbs.created
        else        : return None


    # --------------------------------------------------------------------------
    #
    @property
    def connected(self):
        """Returns the most recent UTC date and time the session was
        reconnected to.
        """
        if self._dbs: return self._dbs.connected
        else        : return None


    # -------------------------------------------------------------------------
    #
    @property
    def is_connected(self):

        return self._dbs.is_connected


    # --------------------------------------------------------------------------
    #
    @property
    def closed(self):
        """
        Returns the time of closing
        """
        if self._dbs: return self._dbs.closed
        else        : return None


    # --------------------------------------------------------------------------
    #
    def _get_logger(self, name, level=None):
        """
        This is a thin wrapper around `ru.get_logger()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        # FIXME: this is only needed because components may use a different
        #        logger namespace - which they should not I guess?
        if not level: level = os.environ.get('RADICAL_PILOT_VERBOSE')
        if not level: level = os.environ.get('RADICAL_VERBOSE', 'REPORT')

        log = ru.get_logger(name, target='.', level=level, path=self._logdir)
        log.info('radical.pilot        version: %s' % rp_version_detail)

        return log


    # --------------------------------------------------------------------------
    #
    def _get_profiler(self, name, level=None):
        """
        This is a thin wrapper around `ru.Profiler()` which makes sure that
        profiles end up in a separate directory with the name of `session.uid`.
        """

        return ru.Profiler(name, path=self._logdir)


    # --------------------------------------------------------------------------
    #
    def inject_metadata(self, metadata):
        """
        Insert (experiment) metadata into an active session
        RP stack version info always get added.
        """

        if not isinstance(metadata, dict):
            raise Exception("Session metadata should be a dict!")

        # Always record the radical software stack
        metadata['radical_stack'] = {'rp': rp_version_detail,
                                     'rs': rs.version_detail,
                                     'ru': ru.version_detail}

        result = self._dbs._c.update({'type' : 'session', 
                                      "uid"  : self.uid},
                                     {"$set" : {"metadata": metadata}})


    # --------------------------------------------------------------------------
    #
    def _register_pmgr(self, pmgr):

        self._dbs.insert_pmgr(pmgr.as_dict())
        self._pmgrs[pmgr.uid] = pmgr


    # --------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """
        Lists the unique identifiers of all :class:`radical.pilot.PilotManager` 
        instances associated with this session.

        **Returns:**
            * A list of :class:`radical.pilot.PilotManager` uids (`list` of `strings`).
        """

        self._is_valid()
        return self._pmgrs.keys()


    # --------------------------------------------------------------------------
    #
    def get_pilot_managers(self, pmgr_uids=None):
        """ 
        returns known PilotManager(s).

        **Arguments:**

            * **pmgr_uids** [`string`]: 
              unique identifier of the PilotManager we want

        **Returns:**
            * One or more [:class:`radical.pilot.PilotManager`] objects.
        """

        self._is_valid()

        return_scalar = False
        if not isinstance(pmgr_uids, list):
            pmgr_uids     = [pmgr_uids]
            return_scalar = True

        if pmgr_uids: pmgrs = [self._pmgrs[uid] for uid in pmgr_uids]
        else        : pmgrs =  self._pmgrs.values()

        if return_scalar: return pmgrs[0]
        else            : return pmgrs


    # --------------------------------------------------------------------------
    #
    def _register_umgr(self, umgr):

        self._dbs.insert_umgr(umgr.as_dict())
        self._umgrs[umgr.uid] = umgr


    # --------------------------------------------------------------------------
    #
    def list_unit_managers(self):
        """
        Lists the unique identifiers of all :class:`radical.pilot.UnitManager` 
        instances associated with this session.

        **Returns:**
            * A list of :class:`radical.pilot.UnitManager` uids (`list` of `strings`).
        """

        self._is_valid()
        return self._umgrs.keys()


    # --------------------------------------------------------------------------
    #
    def get_unit_managers(self, umgr_uids=None):
        """ 
        returns known UnitManager(s).

        **Arguments:**

            * **umgr_uids** [`string`]: 
              unique identifier of the UnitManager we want

        **Returns:**
            * One or more [:class:`radical.pilot.UnitManager`] objects.
        """

        self._is_valid()

        return_scalar = False
        if not isinstance(umgr_uids, list):
            umgr_uids     = [umgr_uids]
            return_scalar = True

        if umgr_uids: umgrs = [self._umgrs[uid] for uid in umgr_uids]
        else        : umgrs =  self._umgrs.values()

        if return_scalar: return umgrs[0]
        else            : return umgrs


    # -------------------------------------------------------------------------
    #
    def add_resource_config(self, resource_config):
        """Adds a new :class:`radical.pilot.ResourceConfig` to the PilotManager's 
           dictionary of known resources, or accept a string which points to
           a configuration file.

           For example::

                  rc = radical.pilot.ResourceConfig(label="mycluster")
                  rc.job_manager_endpoint = "ssh+pbs://mycluster
                  rc.filesystem_endpoint  = "sftp://mycluster
                  rc.default_queue        = "private"
                  rc.bootstrapper         = "default_bootstrapper.sh"

                  pm = radical.pilot.PilotManager(session=s)
                  pm.add_resource_config(rc)

                  pd = radical.pilot.ComputePilotDescription()
                  pd.resource = "mycluster"
                  pd.cores    = 16
                  pd.runtime  = 5 # minutes

                  pilot = pm.submit_pilots(pd)
        """
        if isinstance(resource_config, basestring):

            # let exceptions fall through
            rcs = ResourceConfig.from_file(resource_config)

            for rc in rcs:
                self._log.info("Loaded resource configurations for %s" % rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        else:
            self._resource_configs[resource_config.label] = resource_config.as_dict()

    # -------------------------------------------------------------------------
    #
    def get_resource_config(self, resource, schema=None):
        """
        Returns a dictionary of the requested resource config
        """

        if  resource in self._resource_aliases:
            self._log.warning("using alias '%s' for deprecated resource key '%s'" \
                              % (self._resource_aliases[resource], resource))
            resource = self._resource_aliases[resource]

        if  resource not in self._resource_configs:
            raise RuntimeError("Resource '%s' is not known." % resource)

        resource_cfg = copy.deepcopy(self._resource_configs[resource])

        if  not schema:
            if 'schemas' in resource_cfg:
                schema = resource_cfg['schemas'][0]

        if  schema:
            if  schema not in resource_cfg:
                raise RuntimeError("schema %s unknown for resource %s" \
                                  % (schema, resource))

            for key in resource_cfg[schema]:
                # merge schema specific resource keys into the
                # resource config
                resource_cfg[key] = resource_cfg[schema][key]

        return resource_cfg


    # -------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None):
        return rpu.fetch_profiles(self._uid, dburl=self.dburl, tgt=tgt, session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_json(self, tgt=None):
        return rpu.fetch_json(self._uid, dburl=self.dburl, tgt=tgt)


    # -------------------------------------------------------------------------
    #
    def _get_global_sandbox(self, pilot):
        """
        for a given pilot dict, determine the global RP sandbox, based on the
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

            if resource not in self._cache['global_sandbox']:

                # cache miss -- determine sandbox and fill cache
                rcfg   = self.get_resource_config(resource, schema)
                fs_url = rs.Url(rcfg['filesystem_endpoint'])
        
                # Get the sandbox from either the pilot_desc or resource conf
                sandbox_raw = pilot['description'].get('sandbox')
                if not sandbox_raw:
                    sandbox_raw = rcfg.get('default_remote_workdir', "$PWD")
        
                # If the sandbox contains expandables, we need to resolve those remotely.
                # NOTE: Note that this will only work for (gsi)ssh or shell based access mechanisms
                if '$' not in sandbox_raw and '`' not in sandbox_raw:
                    # no need to expand further
                    sandbox_base = sandbox_raw
        
                else:
                    js_url = rs.Url(rcfg['job_manager_endpoint'])
        
                    if 'ssh' in js_url.schema.split('+'):
                        js_url.schema = 'ssh'
                    elif 'gsissh' in js_url.schema.split('+'):
                        js_url.schema = 'gsissh'
                    elif 'fork' in js_url.schema.split('+'):
                        js_url.schema = 'fork'
                    elif '+' not in js_url.schema:
                        # For local access to queueing systems use fork
                        js_url.schema = 'fork'
                    else:
                        raise Exception("unsupported access schema: %s" % js_url.schema)
        
                    self._log.debug("rsup.PTYShell('%s')" % js_url)
                    shell = rsup.PTYShell(js_url, self)
        
                    ret, out, err = shell.run_sync(' echo "WORKDIR: %s"' % sandbox_raw)
                    if ret == 0 and 'WORKDIR:' in out:
                        sandbox_base = out.split(":")[1].strip()
                        self._log.debug("sandbox base %s: '%s'" % (js_url, sandbox_base))
                    else:
                        raise RuntimeError("Couldn't get remote working directory.")
        
                # at this point we have determined the remote 'pwd' - the global sandbox
                # is relative to it.
                fs_url.path = "%s/radical.pilot.sandbox" % sandbox_base
        
                # before returning, keep the URL string in cache
                self._cache['global_sandbox'][resource] = fs_url

            return self._cache['global_sandbox'][resource]


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
                global_sandbox  = self._get_global_sandbox(pilot)
                session_sandbox = rs.Url(global_sandbox)
                session_sandbox.path += '/%s' % self.uid

                with self._cache_lock:
                    self._cache['session_sandbox'][resource] = session_sandbox

            return self._cache['session_sandbox'][resource]


    # --------------------------------------------------------------------------
    #
    def _get_pilot_sandbox(self, pilot):

        # FIXME: this should get 'pid, resource, schema=None' as parameters

        pid = pilot['uid']
        with self._cache_lock:
            if  pid in self._cache['pilot_sandbox']:
                return self._cache['pilot_sandbox'][pid]

        # cache miss
        session_sandbox = self._get_session_sandbox(pilot)
        pilot_sandbox  = rs.Url (session_sandbox)
        pilot_sandbox.path += '/%s/' % pilot['uid']

        with self._cache_lock:
            self._cache['pilot_sandbox'][pid] = pilot_sandbox

        return pilot_sandbox


    # --------------------------------------------------------------------------
    #
    def _get_unit_sandbox(self, unit, pilot):

        # we don't cache unit sandboxes, they are just a string concat.
        pilot_sandbox = self._get_pilot_sandbox(pilot)
        return "%s/%s/" % (pilot_sandbox, unit['uid'])


# -----------------------------------------------------------------------------


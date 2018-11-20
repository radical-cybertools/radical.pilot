
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

# the session needs to get rid of child process handles after forks, as Python's
# multiprocessing module does not allow to check for child process health from
# processes which did not originally spawn the children.  For htis we use
# `at_fork`, which monkeypatches `os.fork()` to support prepare, parent and
# child hooks.  We then register a child hook during session initialization.
#
# Since the monkeypatch needs to be applies before `os` is imported, we do that
# right here, in the (probably vain) hope that os was not imported before.  If
# it was, at_fork will raise an error.
#


import os
import sys
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

    A Session holds :class:`radical.pilot.PilotManager` and
    :class:`radical.pilot.UnitManager` instances which in turn hold
    :class:`radical.pilot.ComputePilot` and :class:`radical.pilot.ComputeUnit`
    instances.
    """

    # the reporter is an applicataion-level singleton
    _reporter = None

    # We keep a static typemap for component startup. If we ever want to
    # become reeeealy fancy, we can derive that typemap from rp module
    # inspection.
    #
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

        if os.uname()[0] == 'Darwin':
            # on MacOS, we are running out of file descriptors soon.  The code
            # below attempts to increase the limit of open files - but any error
            # is silently ignored, so this is an best-effort, no guarantee.  We
            # leave responsibility for system limits with the user.
            try:
                import resource
                limits    = list(resource.getrlimit(resource.RLIMIT_NOFILE))
                limits[0] = 512
                resource.setrlimit(resource.RLIMIT_NOFILE, limits)
            except:
                pass

        self._dh          = ru.DebugHelper()
        self._valid       = True
        self._closed      = False
        self._valid_iter  = 0  # detect recursive calls of `is_valid()`

        # class state
        self._dbs         = None
        self._uid         = None
        self._dburl       = None
        self._reconnected = False

        self._cache       = dict()  # cache sandboxes etc.
        self._cache_lock  = threading.RLock()

        self._cache['resource_sandbox'] = dict()
        self._cache['session_sandbox']  = dict()
        self._cache['pilot_sandbox']    = dict()

        # before doing anything else, set up the debug helper for the lifetime
        # of the session.
        self._debug_helper = ru.DebugHelper()

        # Dictionaries holding all manager objects created during the session.
        # NOTE: should this also include agents?
        self._pmgrs      = dict()
        self._umgrs      = dict()
        self._bridges    = list()
        self._components = list()

        # FIXME: we work around some garbage collection issues we don't yet
        #        understand: instead of relying on the GC to eventually collect
        #        some stuff, we actively free those on `session.close()`, at
        #        least for the current process.  Usually, all resources get
        #        nicely collected on process termination - but not when we
        #        create many sessions (one after the other) in the same
        #        application instance (ie. the same process).  This workarounf
        #        takes care of that use case.
        #        The clean solution would be to ensure clean termination
        #        sequence, something which I seem to be unable to implement...
        #        :/
        self._to_close   = list()
        self._to_stop    = list()
        self._to_destroy = list()

        # cache the client sandbox
        # FIXME: this needs to be overwritten if configured differently in the
        #        session config, as should be the case for any agent side
        #        session instance.
        self._client_sandbox = os.getcwd()

        # The resource configuration dictionary associated with the session.
        self._resource_configs = {}

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
        if not uid : 
            uid = self._cfg.get('session_id')

        if uid:
            self._uid         = uid
            self._reconnected = True
        else:
            # generate new uid, reset all other ID counters
            # FIXME: this will screw up counters for *concurrent* sessions, 
            #        as the ID generation is managed in a process singleton.
            self._uid = ru.generate_id('rp.session',  mode=ru.ID_PRIVATE)
            ru.reset_id_counters(prefix='rp.session', reset_all_others=True)

        if not self._cfg.get('session_id'): self._cfg['session_id'] = self._uid 
        if not self._cfg.get('owner')     : self._cfg['owner']      = self._uid 
        if not self._cfg.get('logdir')    : self._cfg['logdir']     = '%s/%s' \
                                                     % (os.getcwd(), self._uid)
        self._logdir = self._cfg['logdir']
        self._prof   = self._get_profiler(name=self._cfg['owner'])
        self._rep    = self._get_reporter(name=self._cfg['owner'])
        self._log    = self._get_logger  (name=self._cfg['owner'],
                                          level=self._cfg.get('debug'))

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
        self._cfg['dburl'] = str(self._dburl)

        # now we have config and uid - initialize base class (saga session)
        rs.Session.__init__(self, uid=self._uid)


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

        if not self._reconnected:
            self._prof.prof('session_start', uid=self._uid)
            self._rep.info ('<<new session: ')
            self._rep.plain('[%s]' % self._uid)
            self._rep.info ('<<database   : ')
            self._rep.plain('[%s]' % self._dburl)

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
            self._rep.error(">>err\n")
            self._log.exception('session create failed')
            raise RuntimeError("Couldn't create new session (database URL '%s' incorrect?): %s" \
                            % (dburl, ex))  

        # the session must not carry bridge and component handles across forks
        ru.atfork(self._atfork_prepare, self._atfork_parent, self._atfork_child)

        # if bridges and components are specified in the config, start them
        ruc = rpu.Component
        self._bridges    = ruc.start_bridges   (self._cfg, self, self._log)
        self._components = ruc.start_components(self._cfg, self, self._log)
        self.is_valid()

        # at this point we have a DB connection, logger, etc, and can record
        # some metadata
        self._log.info('radical.pilot version: %s' % rp_version_detail)
        self._log.info('radical.saga  version: %s' % rs.version_detail)
        self._log.info('radical.utils version: %s' % ru.version_detail)

        py_version_detail = sys.version.replace("\n", " ")
        self.inject_metadata({'radical_stack' : {'rp': rp_version_detail,
                                                 'rs': rs.version_detail,
                                                 'ru': ru.version_detail,
                                                 'py': py_version_detail}})


        # FIXME: make sure the above code results in a usable session on
        #        reconnect
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def _atfork_prepare(self): 
        pass

    def _atfork_parent(self) :
        pass

    def _atfork_child(self)  : 
        self._components = list()
        self._bridges    = list()
        self._to_close   = list()
        self._to_stop    = list()
        self._to_destroy = list()

    
    # --------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __enter__(self):
        return self


    # --------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __exit__(self, type, value, traceback):

        # FIXME: use cleanup_on_close, terminate_on_close attributes
        self.close()


    # --------------------------------------------------------------------------
    #
    def is_valid(self, term=True):

        # don't check validity during termination
        if self._closed:
            return True

        # if we check any manager or agent, it will likely also check the
        # session in turn.  We break that loop here.
        self._valid_iter += 1

        try:
            if self._valid_iter >= 2:
                # we are too deep - abort this line or tests
                return True

            if self._valid:
                for _,umgr in self._umgrs.iteritems():
                    if not umgr.is_valid(term):
                        self._valid = False
                        break

            if self._valid:
                for _,pmgr in self._pmgrs.iteritems():
                    if not pmgr.is_valid(term):
                        self._valid = False
                        break

            if self._valid:
                for bridge in self._bridges:
                    if not bridge.is_valid(term):
                        self._valid = False
                        break

            if self._valid:
                for component in self._components:
                    if not component.is_valid(term):
                        self._valid = False
                        break

        finally:
            pass

        if not self._valid and term:
            self._log.warn("session %s is invalid" % self.uid)
            self.close()
          # raise RuntimeError("session %s is invalid" % self.uid)

        return self._valid


    # --------------------------------------------------------------------------
    #
    def _load_resource_configs(self):

        self.is_valid()

        self._prof.prof('config_parser_start', uid=self._uid)

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
                self._log.debug('read rcfg for %s (%s)', 
                        rc, self._resource_configs[rc].get('cores_per_node'))

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

                self._log.debug('fix  rcfg for %s (%s)', 
                        rc, self._resource_configs[rc].get('cores_per_node'))

        default_aliases = "%s/configs/resource_aliases.json" % module_path
        self._resource_aliases = ru.read_json_str(default_aliases)['aliases']

        # check if we have aliases to merge
        usr_aliases = '%s/.radical/pilot/configs/resource_aliases.json' % home
        if os.path.isfile(usr_aliases):
            ru.dict_merge(self._resource_aliases,
                          ru.read_json_str(usr_aliases).get('aliases', {}),
                          policy='overwrite')

        self._prof.prof('config_parser_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def close(self, cleanup=False, terminate=True, download=False):
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

        # close only once
        if self._closed:
            return

        self._rep.info('closing session %s' % self._uid)
        self._log.debug("session %s closing", self._uid)
        self._prof.prof("session_close", uid=self._uid)

        # set defaults
        if cleanup   == None: cleanup   = True
        if terminate == None: terminate = True

        if  cleanup:
            # cleanup implies terminate
            terminate = True

        for umgr_uid,umgr in self._umgrs.iteritems():
            self._log.debug("session %s closes umgr   %s", self._uid, umgr_uid)
            umgr.close()
            self._log.debug("session %s closed umgr   %s", self._uid, umgr_uid)

        for pmgr_uid,pmgr in self._pmgrs.iteritems():
            self._log.debug("session %s closes pmgr   %s", self._uid, pmgr_uid)
            pmgr.close(terminate=terminate)
            self._log.debug("session %s closed pmgr   %s", self._uid, pmgr_uid)

        for comp in self._components:
            self._log.debug("session %s closes comp   %s", self._uid, comp.uid)
            comp.stop()
            comp.join()
            self._log.debug("session %s closed comp   %s", self._uid, comp.uid)

        for bridge in self._bridges:
            self._log.debug("session %s closes bridge %s", self._uid, bridge.uid)
            bridge.stop()
            bridge.join()
            self._log.debug("session %s closed bridge %s", self._uid, bridge.uid)

        if self._dbs:
            self._log.debug("session %s closes db (%s)", self._uid, cleanup)
            self._dbs.close(delete=cleanup)

        self._log.debug("session %s closed (delete=%s)", self._uid, cleanup)
        self._prof.prof("session_stop", uid=self._uid)
        self._prof.close()

        # support GC
        for x in self._to_close: 
            try:    x.close()
            except: pass
        for x in self._to_stop:
            try:    x.stop()
            except: pass
        for x in self._to_destroy:
            try:    x.destroy()
            except: pass

        self._closed = True
        self._valid = False

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
        """Returns a Python dictionary representation of the object.
        """

        self.is_valid()

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
    def logdir(self):
        return self._logdir


    # --------------------------------------------------------------------------
    #
    @property
    def dburl(self):
        return self._dburl


    # --------------------------------------------------------------------------
    #
    def get_db(self):

        self.is_valid()

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

        self.is_valid()

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
        This is a thin wrapper around `ru.Logger()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """
        return ru.Logger(name=name, ns='radical.pilot', targets=['.'], 
                         path=self._logdir, level=level)


    # --------------------------------------------------------------------------
    #
    def _get_reporter(self, name):
        """
        This is a thin wrapper around `ru.Reporter()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        return ru.Reporter(name=name, ns='radical.pilot', targets=['stdout'],
                           path=self._logdir)


    # --------------------------------------------------------------------------
    #
    def _get_profiler(self, name):
        """
        This is a thin wrapper around `ru.Profiler()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        prof = ru.Profiler(name=name, ns='radical.pilot', path=self._logdir)

        return prof


    # --------------------------------------------------------------------------
    #
    def _get_reporter(self, name):
        """
        This is a thin wrapper around `ru.Reporter()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        if not self._reporter:
            self._reporter = ru.Reporter(name=name, ns='radical.pilot',
                                         targets=['stdout'], path=self._logdir)
        return self._reporter


    # --------------------------------------------------------------------------
    #
    def _get_profiler(self, name):
        """
        This is a thin wrapper around `ru.Profiler()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        prof = ru.Profiler(name=name, ns='radical.pilot', path=self._logdir)

        return prof


    # --------------------------------------------------------------------------
    #
    def inject_metadata(self, metadata):
        """
        Insert (experiment) metadata into an active session
        RP stack version info always get added.
        """

        self.is_valid()

        if not isinstance(metadata, dict):
            raise Exception("Session metadata should be a dict!")

        if self._dbs and self._dbs._c:
            self._dbs._c.update({'type'  : 'session',
                                 "uid"   : self.uid},
                                {"$push" : {"metadata": metadata}})


    # --------------------------------------------------------------------------
    #
    def _register_pmgr(self, pmgr):

        self.is_valid()

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

        self.is_valid()
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

        self.is_valid()

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

        self.is_valid()

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

        self.is_valid()
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

        self.is_valid()

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
    def list_resources(self):
        '''
        Returns a list of known resource labels which can be used in a pilot
        description.  Not that resource aliases won't be listed.
        '''

        return sorted(self._resource_configs.keys())


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

        self.is_valid()

        if isinstance(resource_config, basestring):

            # let exceptions fall through
            rcs = ResourceConfig.from_file(resource_config)

            for rc in rcs:
                self._log.info("Loaded resource configurations for %s" % rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 
                self._log.debug('add  rcfg for %s (%s)', 
                        rc, self._resource_configs[rc].get('cores_per_node'))

        else:
            self._resource_configs[resource_config.label] = resource_config.as_dict()
            self._log.debug('Add  rcfg for %s (%s)', 
                    resource_config.label, 
                    self._resource_configs[resource_config.label].get('cores_per_node'))

    # -------------------------------------------------------------------------
    #
    def get_resource_config(self, resource, schema=None):
        """
        Returns a dictionary of the requested resource config
        """

        self.is_valid()

        if  resource in self._resource_aliases:
            self._log.warning("using alias '%s' for deprecated resource key '%s'" \
                              % (self._resource_aliases[resource], resource))
            resource = self._resource_aliases[resource]

        if  resource not in self._resource_configs:
            raise RuntimeError("Resource '%s' is not known." % resource)

        resource_cfg = copy.deepcopy(self._resource_configs[resource])
        self._log.debug('get rcfg 1 for %s (%s)',  resource, resource_cfg.get('cores_per_node'))

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

        self._log.debug('get rcfg 2 for %s (%s)',  resource, resource_cfg.get('cores_per_node'))

        return resource_cfg


    # -------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_profiles(self._uid, dburl=self.dburl, tgt=tgt, 
                                  session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_logfiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_logfiles(self._uid, dburl=self.dburl, tgt=tgt, 
                                  session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_json(self, tgt=None, fetch_client=False):

        return rpu.fetch_json(self._uid, dburl=self.dburl, tgt=tgt,
                              session=self)



    # -------------------------------------------------------------------------
    #
    def _get_client_sandbox(self):
        """
        For the session in the client application, this is os.getcwd().  For the
        session in any other component, specifically in pilot components, the
        client sandbox needs to be read from the session config (or pilot
        config).  The latter is not yet implemented, so the pilot can not yet
        interpret client sandboxes.  Since pilot-side stagting to and from the
        client sandbox is not yet supported anyway, this seems acceptable
        (FIXME).
        """

        return self._client_sandbox


    # -------------------------------------------------------------------------
    #
    def _get_resource_sandbox(self, pilot):
        """
        for a given pilot dict, determine the global RP sandbox, based on the
        pilot's 'resource' attribute.
        """

        self.is_valid()

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
        
                    self._log.debug("rsup.PTYShell('%s')", js_url)
                    shell = rsup.PTYShell(js_url, self)
        
                    ret, out, err = shell.run_sync(' echo "WORKDIR: %s"' % sandbox_raw)
                    if ret == 0 and 'WORKDIR:' in out:
                        sandbox_base = out.split(":")[1].strip()
                        self._log.debug("sandbox base %s: '%s'", js_url, sandbox_base)
                    else:
                        raise RuntimeError("Couldn't get remote working directory.")
        
                # at this point we have determined the remote 'pwd' - the global sandbox
                # is relative to it.
                fs_url.path = "%s/radical.pilot.sandbox" % sandbox_base
        
                # before returning, keep the URL string in cache
                self._cache['resource_sandbox'][resource] = fs_url

            return self._cache['resource_sandbox'][resource]


    # --------------------------------------------------------------------------
    #
    def _get_session_sandbox(self, pilot):

        self.is_valid()

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

        self.is_valid()

        # FIXME: this should get 'pid, resource, schema=None' as parameters

        self.is_valid()

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
    def _get_unit_sandbox(self, unit, pilot):

        self.is_valid()

        # we don't cache unit sandboxes, they are just a string concat.
        pilot_sandbox = self._get_pilot_sandbox(pilot)
        return "%s/%s/" % (pilot_sandbox, unit['uid'])


    # --------------------------------------------------------------------------
    #
    def _get_jsurl(self, pilot):
        '''
        get job service endpoint and hop URL for the pilot's target resource.
        '''

        self.is_valid()

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

        import github3
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

        title = 'autopilot: %s' % titles[random.randint(0, len(titles)-1)]

        print '----------------------------------------------------'
        print 'autopilot'

        for issue in repo.issues(labels=labels, state='open'):
            if issue.title == title:
                reply = 'excuse: %s' % excuse()
                issue.create_comment(reply)
                print '  resolve: %s' % reply
                return

        # issue not found - create
        body  = 'problem: %s' % excuse()
        issue = repo.create_issue(title=title, body=body, labels=[labels],
                                  assignee=user)
        print '  issue  : %s' % title
        print '  problem: %s' % body
        print '----------------------------------------------------'


# -----------------------------------------------------------------------------


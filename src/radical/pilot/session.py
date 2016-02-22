
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import glob
import copy
import threading
import radical.utils as ru
import saga          as rs

from .  import utils     as rpu
from .  import states    as rps
from .  import constants as rpc
from .  import types     as rpt

from .unit_manager    import UnitManager
from .pilot_manager   import PilotManager
from .resource_config import ResourceConfig
from .db              import DBSession


# ------------------------------------------------------------------------------
#
class Session (rs.Session):
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
    def __init__ (self, dburl=None, uid=None, database_url=None, _connect=True):
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

        self._log = ru.get_logger('radical.pilot')

        # init the base class inits
        rs.Session.__init__ (self)
        self._dh        = ru.DebugHelper()
        self._valid     = False
        self._terminate = threading.Event()
        self._terminate.clear()

        # the session manages the communication bridges
        self._cfg              = None
        self._components       = None
        self._bridges          = None
        self._bridge_addresses = dict()
        # before doing anything else, set up the debug helper for the lifetime
        # of the session.
        self._debug_helper = ru.DebugHelper ()

        # Dictionaries holding all manager objects created during the session.
        self._pilot_manager_objects = dict()
        self._unit_manager_objects  = dict()

        # The resource configuration dictionary associated with the session.
        self._resource_configs = {}

        if database_url and not dburl:
            self._log.warning('"database_url" for session is deprectaed, use dburl')
            dburl = database_url

        if not dburl:
            dburl = os.getenv ("RADICAL_PILOT_DBURL", None)

        if not dburl and not uid:
            # we forgive missing dburl on reconnect, but not otherwise
            raise RuntimeError("no database URL (set RADICAL_PILOT_DBURL)")  

        self._dburl = None
        if dburl:
            self._dburl = ru.Url(dburl)

            # if the database url contains a path element, we interpret that as
            # database name (without the leading slash)
            if  not self._dburl.path         or \
                self._dburl.path[0]   != '/' or \
                len(self._dburl.path) <=  1  :
                if not uid:
                    # we fake reconnnect if no DB is available -- but otherwise we
                    # really really need a db connection...
                    raise ValueError("incomplete DBURL '%s' no db name!" % self._dburl)

            self._log.info("using database %s" % self._dburl)


        # create database handle
        self._dbs         = None
        self._uid         = None
        self._reconnected = False

        try:
            if uid:
                self._uid         = uid
                self._reconnected = True
            else:
                # generate new uid
                self._uid = ru.generate_id ('rp.session', mode=ru.ID_PRIVATE)
                ru.reset_id_counters(prefix=['pmgr', 'umgr', 'pilot', 'unit', 'unit.%(counter)06d'])


            # initialize profiling
            self.prof = rpu.Profiler('%s' % self._uid)

            if self._reconnected:
                self.prof.prof('reconnect session', uid=self._uid)

            else:
                self.prof.prof('start session', uid=self._uid)
                self._log.report.info ('<<new session: ')
                self._log.report.plain('[%s]' % self._uid)
                self._log.report.info ('<<database   : ')
                self._log.report.plain('[%s]' % self._dburl)

            if self._dburl:
                if connect:
                    self._dbs = DBSession(sid   = self._uid,
                                          dburl = self._dburl)

            # from here on we should be able to close the session again
            self._valid = True
            self._log.info("New Session created: %s." % str(self))

        except Exception, ex:
            self._log.report.error(">>err\n")
            self._log.exception('session create failed')
            raise RuntimeError("Couldn't create new session (database URL '%s' incorrect?): %s" \
                            % (self._dburl, ex))  

        # Loading all "default" resource configurations
        module_path  = os.path.dirname(os.path.abspath(__file__))
        default_cfgs = "%s/configs/resource_*.json" % module_path
        config_files = glob.glob(default_cfgs)

        for config_file in config_files:

            try :
                self._log.info("Load resource configurations from %s" % config_file)
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e :
                self._log.error ("skip config file %s: %s" % (config_file, e))
                continue

            for rc in rcs:
                self._log.info("Load resource configurations for %s" % rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        user_cfgs    = "%s/.radical/pilot/configs/resource_*.json" % os.environ.get ('HOME')
        config_files = glob.glob(user_cfgs)

        for config_file in config_files:

            try:
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e:
                self._log.error ("skip config file %s: %s" % (config_file, e))
                continue

            for rc in rcs:
                self._log.info("Load resource configurations for %s" % rc)

                if rc in self._resource_configs:
                    # config exists -- merge user config into it
                    ru.dict_merge (self._resource_configs[rc],
                                   rcs[rc].as_dict(),
                                   policy='overwrite')
                else:
                    # new config -- add as is
                    self._resource_configs[rc] = rcs[rc].as_dict() 

        default_aliases = "%s/configs/resource_aliases.json" % module_path
        self._resource_aliases = ru.read_json_str(default_aliases)['aliases']

        self.prof.prof('configs parsed', uid=self._uid)

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
                ru.write_json({'dburl': str(self._dburl)}, 
                              "%s/session.json" % self._rec)
                self._log.info("recording session in %s" % self._rec)


        # create communication bridges for umgr and pmgr instances to use
        # NOTE:  sessions can be reconnected in a different host context,
        #        specifically in the agent.  In that case the bridge
        #        addresses would be useless.  We thus record the bridge
        #        addresses in a kind of namespace, refering to 'client' as
        #        the original session (which lives in the client module), 
        #        and to pilot level bridges via the pilot uid.  
        # FIXME: For now, we don't use the DB, but only create bridges in
        #        new sessions.
        try:
            # load the session config
            self._cfg = ru.read_json("%s/configs/session_%s.json" \
                    % (os.path.dirname(__file__),
                       os.environ.get('RADICAL_PILOT_SESSION_CONFIG', 'default')))
            bridges       = self._cfg.get('bridges', [])

            # new session start bridges, reconnected sessions get bridge
            # addresses from the DB
            if not self._reconnected:
                self._bridges = rpu.Component.start_bridges(bridges, session=self)

                # get bridge addresses from our bridges, and append them to the
                # config, so that we can pass those addresses to the umgr and pmgr 
                # components
                self._bridge_addresses = dict()

                for b in self._bridges:

                    # to avoid confusion with component input and output, we call bridge
                    # input a 'sink', and a bridge output a 'source' (from the component
                    # perspective)
                    sink   = ru.Url(self._bridges[b]['in'])
                    source = ru.Url(self._bridges[b]['out'])

                    # for the unit manager, we assume all bridges to be local, so we
                    # really are only interested in the ports for now...
                    sink.host   = '127.0.0.1'
                    source.host = '127.0.0.1'

                    # keep the resultin URLs as strings, to be used as addresses
                    self._bridge_addresses[b] = dict()
                    self._bridge_addresses[b]['sink']   = str(sink)
                    self._bridge_addresses[b]['source'] = str(source)

                # FIXME: make sure all communication channels are in place.  This could
                # be replaced with a proper barrier, but not sure if that is worth it...
                time.sleep(1)

        except Exception as e:
            self._log.report.error(">>err\n")
            self._log.exception('session create failed')
            raise RuntimeError("Couldn't create bridges): %s" % e)  


        # create update and heartbeat worker components
        #
        # NOTE: reconnected sessions will not start components
        try:

            if not self._reconnected:
                components = self._cfg.get('components', [])

                from .. import pilot as rp

                # we also need a map from component names to class types
                typemap = {
                    rpc.UPDATE_WORKER    : rp.worker.Update,
                    rpc.HEARTBEAT_WORKER : rp.worker.Heartbeat
                    }

                # get addresses from the bridges, and append them to the
                # config, so that we can pass those addresses to the components
                self._cfg['bridge_addresses'] = copy.deepcopy(self._bridge_addresses)

                # give some more information to the workers
                self._cfg['owner']       = self._uid
                self._cfg['session_id']  = self._uid
                self._cfg['mongodb_url'] = self._dburl

                # the bridges are known, we can start to connect the components to them
                self._components = rpu.Component.start_components(components,
                        typemap, cfg=self._cfg, session=self)

        except Exception as e:
            self._log.report.error(">>err\n")
            self._log.exception('session create failed')
            raise RuntimeError("Couldn't create worker components): %s" % e)  

        # FIXME: make sure the above code results in a usable session on
        #        reconnect
        self._log.report.ok('>>ok\n')


    #---------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __enter__ (self):
        return self


    #---------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __exit__ (self, type, value, traceback) :
        self.close()


    #---------------------------------------------------------------------------
    #
    def __del__ (self) :
        pass
      # self.close ()


    #---------------------------------------------------------------------------
    #
    def _is_valid(self):
        if not self._valid:
            raise RuntimeError("instance was closed")


    #---------------------------------------------------------------------------
    #
    def start_bridges(self):

        raise NotImplementedError('not implemented')


    #---------------------------------------------------------------------------
    #
    def use_bridges(self):

        raise NotImplementedError('not implemented')


    #---------------------------------------------------------------------------
    #
    def start_components(self):

        if not self._bridge_addresses:
            raise RuntimeError("can't start components w/o bridges")

        raise NotImplementedError('not implemented')


    #---------------------------------------------------------------------------
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

        self._is_valid()

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

            if  cleanup == True and terminate == True :
                cleanup   = delete
                terminate = delete
                self._log.warning("'delete' flag on session is deprecated. " \
                             "Please use 'cleanup' and 'terminate' instead!")

        if  cleanup :
            # cleanup implies terminate
            terminate = True

        if terminate:
            self._terminate.set()

        for pmgr_uid, pmgr in self._pilot_manager_objects.iteritems():
            self._log.debug("session %s closes   pmgr   %s" % (str(self._uid), pmgr_uid))
            pmgr.close (terminate=terminate)
            self._log.debug("session %s closed   pmgr   %s" % (str(self._uid), pmgr_uid))

        for umgr_uid, umgr in self._unit_manager_objects.iteritems():
            self._log.debug("session %s closes   umgr   %s" % (str(self._uid), umgr._uid))
            umgr.close()
            self._log.debug("session %s closed   umgr   %s" % (str(self._uid), umgr._uid))

        if  cleanup :
            self.prof.prof("cleaning", uid=self._uid)
            if self._dbs:
                self._dbs.delete()
            self.prof.prof("cleaned", uid=self._uid)
        else:
            if self._dbs:
                self._dbs.close()

        self._log.debug("session %s closed" % (str(self._uid)))
        self.prof.prof("closed", uid=self._uid)
        self.prof.close()

        self._valid = False

        self._log.report.info('<<session lifetime: %.1fs' % (self.closed - self.created))
        self._log.report.ok('>>ok\n')


    #---------------------------------------------------------------------------
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
            "dburl"     : str(self._dburl)
        }
        return object_dict


    #---------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    #---------------------------------------------------------------------------
    #
    @property
    def dburl(self):
        self._is_valid()
        return self._dburl


    #---------------------------------------------------------------------------
    #
    def get_db(self):

        self._is_valid()

        if self._dbs: return self._dbs.get_db()
        else        : return None


    
    #---------------------------------------------------------------------------
    #
    @property
    def created(self):
        """Returns the UTC date and time the session was created.
        """
        if self._dbs: return self._dbs.created
        else        : return None


    #---------------------------------------------------------------------------
    #
    @property
    def connected(self):
        """Returns the most recent UTC date and time the session was
        reconnected to.
        """
        if self._dbs: return self._dbs.connected
        else        : return None


    #---------------------------------------------------------------------------
    #
    @property
    def closed(self):
        """
        Returns the time of closing
        """
        if self._dbs: return self._dbs.closed
        else        : return None


    #---------------------------------------------------------------------------
    #
    def inject_metadata(self, metadata):
        """
        Insert (experiment) metadata into an active session
        RP stack version info always get added.
        """

        if not isinstance(metadata, dict):
            raise Exception("Session metadata should be a dict!")

        from .utils import version_detail as rp_version_detail

        # Always record the radical software stack
        metadata['radical_stack'] = {'rp': rp_version_detail,
                                     'rs': rs.version_detail,
                                     'ru': ru.version_detail}

        result = self._dbs._c.update({'type' : 'session', 
                                      "uid"  : self.uid},
                                     {"$set" : {"metadata": metadata}})


    #---------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """
        Lists the unique identifiers of all :class:`radical.pilot.PilotManager` 
        instances associated with this session.

        **Example**::

            s = radical.pilot.Session(dburl=DBURL)
            for pm_uid in s.list_pilot_managers():
                pm = radical.pilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`radical.pilot.PilotManager` uids (`list` of strings`).

        **Raises:**
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        self._is_valid()
        return self._pilot_manager_objects.keys()


    # --------------------------------------------------------------------------
    #
    def get_pilot_managers(self, pilot_manager_ids=None) :
        """ Re-connects to and returns one or more existing PilotManager(s).

        **Arguments:**

            * **session** [:class:`radical.pilot.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * One or more new [:class:`radical.pilot.PilotManager`] objects.

        **Raises:**

            * :class:`radical.pilot.pilotException` if a PilotManager with 
              `pilot_manager_uid` doesn't exist in the database.
        """
        self._is_valid()

        return_scalar = False

        if pilot_manager_ids is None:
            pilot_manager_ids = self.list_pilot_managers()

        elif not isinstance(pilot_manager_ids, list):
            pilot_manager_ids = [pilot_manager_ids]
            return_scalar = True

        pilot_manager_objects = list()
        for pid in pilot_manager_ids:
            pilot_manager_objects.append (self._pilot_manager_objects[pid])

        if return_scalar is True:
            pilot_manager_objects = pilot_manager_objects[0]

        return pilot_manager_objects

    #---------------------------------------------------------------------------
    #
    def list_unit_managers(self):
        """Lists the unique identifiers of all :class:`radical.pilot.UnitManager` 
        instances associated with this session.

        **Example**::

            s = radical.pilot.Session(dburl=DBURL)
            for pm_uid in s.list_unit_managers():
                pm = radical.pilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`radical.pilot.UnitManager` uids (`list` of `strings`).

        **Raises:**
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        self._is_valid()
        return self._unit_manager_objects.keys()

    # --------------------------------------------------------------------------
    #
    def get_unit_managers(self, unit_manager_ids=None) :
        """ Re-connects to and returns one or more existing UnitManager(s).

        **Arguments:**

            * **session** [:class:`radical.pilot.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * One or more new [:class:`radical.pilot.PilotManager`] objects.

        **Raises:**

            * :class:`radical.pilot.pilotException` if a PilotManager with 
              `pilot_manager_uid` doesn't exist in the database.
        """
        self._is_valid()

        return_scalar = False
        if unit_manager_ids is None:
            unit_manager_ids = self.list_unit_managers()

        elif not isinstance(unit_manager_ids, list):
            unit_manager_ids = [unit_manager_ids]
            return_scalar = True

        unit_manager_objects = list()
        for uid in unit_manager_ids:
            unit_manager_objects.append (self._unit_manager_objects[uid])

        if return_scalar is True:
            unit_manager_objects = unit_manager_objects[0]

        return unit_manager_objects

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
        if  isinstance (resource_config, basestring) :

            # let exceptions fall through
            rcs = ResourceConfig.from_file(resource_config)

            for rc in rcs:
                self._log.info("Loaded resource configurations for %s" % rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        else :
            self._resource_configs[resource_config.label] = resource_config.as_dict()

    # -------------------------------------------------------------------------
    #
    def get_resource_config (self, resource_key, schema=None):
        """Returns a dictionary of the requested resource config
        """

        if  resource_key in self._resource_aliases :
            self._log.warning ("using alias '%s' for deprecated resource key '%s'" \
                         % (self._resource_aliases[resource_key], resource_key))
            resource_key = self._resource_aliases[resource_key]

        if  resource_key not in self._resource_configs:
            raise RuntimeError("Resource '%s' is not known." % resource_key)

        resource_cfg = copy.deepcopy (self._resource_configs[resource_key])

        if  not schema :
            if 'schemas' in resource_cfg :
                schema = resource_cfg['schemas'][0]

        if  schema:
            if  schema not in resource_cfg :
                raise RuntimeError("schema %s unknown for resource %s" \
                                  % (schema, resource_key))

            for key in resource_cfg[schema] :
                # merge schema specific resource keys into the
                # resource config
                resource_cfg[key] = resource_cfg[schema][key]

        return resource_cfg


    # -------------------------------------------------------------------------
    #
    def fetch_profiles (self, tgt=None):
        return rpu.fetch_profiles (self._uid, dburl=self.dburl, tgt=tgt, session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_json (self, tgt=None):
        return rpu.fetch_json (self._uid, dburl=self.dburl, tgt=tgt)



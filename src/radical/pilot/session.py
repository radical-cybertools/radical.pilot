#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.session
   :platform: Unix
   :synopsis: Implementation of the Session class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import bson
import glob
import copy
import saga
import radical.utils as ru

from radical.pilot.object          import Object
from radical.pilot.unit_manager    import UnitManager
from radical.pilot.pilot_manager   import PilotManager
from radical.pilot.utils.logger    import logger
from radical.pilot.resource_config import ResourceConfig
from radical.pilot.exceptions      import PilotException

from radical.pilot.db              import Session as dbSession
from radical.pilot.db              import DBException


# ------------------------------------------------------------------------------
#
class _ProcessRegistry(object):
    """A _ProcessRegistry contains a dictionary of all worker processes 
    that are currently active.
    """
    def __init__(self):
        self._dict = dict()

    def register(self, key, process):
        """Add a new process to the registry.
        """
        if key not in self._dict:
            self._dict[key] = process

    def retrieve(self, key):
        """Retrieve a process from the registry.
        """
        if key not in self._dict:
            return None
        else:
            return self._dict[key]

    def keys(self):
        """List all keys of all process in the registry.
        """
        return self._dict.keys()

    def remove(self, key):
        """Remove a process from the registry.
        """
        if key in self._dict:
            del self._dict[key]

# ------------------------------------------------------------------------------
#
class Session (saga.Session, Object):
    """A Session encapsulates a RADICAL-Pilot instance and is the *root* object
    for all other RADICAL-Pilot objects. 

    A Session holds :class:`radical.pilot.PilotManager` and :class:`radical.pilot.UnitManager`
    instances which in turn hold  :class:`radical.pilot.Pilot` and
    :class:`radical.pilot.ComputeUnit` instances.

    Each Session has a unique identifier :data:`radical.pilot.Session.uid` that can be
    used to re-connect to a RADICAL-Pilot instance in the database.

    **Example**::

        s1 = radical.pilot.Session(database_url=DBURL)
        s2 = radical.pilot.Session(database_url=DBURL, uid=s1.uid)

        # s1 and s2 are pointing to the same session
        assert s1.uid == s2.uid
    """

    #---------------------------------------------------------------------------
    #
    def __init__ (self, database_url=None, database_name="radicalpilot",
                  uid=None, name=None):
        """Creates a new or reconnects to an exising session.

        If called without a uid, a new Session instance is created and 
        stored in the database. If uid is set, an existing session is 
        retrieved from the database. 

        **Arguments:**
            * **database_url** (`string`): The MongoDB URL.  If none is given,
              RP uses the environment variable RADICAL_PILOT_DBURL.  If that is
              not set, an error will be raises.

            * **database_name** (`string`): An alternative database name 
              (default: 'radicalpilot').

            * **uid** (`string`): If uid is set, we try 
              re-connect to an existing session instead of creating a new one.

            * **name** (`string`): An optional human readable name.

        **Returns:**
            * A new Session instance.

        **Raises:**
            * :class:`radical.pilot.DatabaseError`

        """

        # init the base class inits
        saga.Session.__init__ (self)
        Object.__init__ (self)

        # before doing anything else, set up the debug helper for the lifetime
        # of the session.
        self._debug_helper = ru.DebugHelper ()

        # Dictionaries holding all manager objects created during the session.
        self._pilot_manager_objects = list()
        self._unit_manager_objects = list()

        # Create a new process registry. All objects belonging to this 
        # session will register their worker processes (if they have any)
        # in this registry. This makes it easier to shut down things in 
        # a more coordinate fashion. 
        self._process_registry = _ProcessRegistry()

        # The resource configuration dictionary associated with the session.
        self._resource_configs = {}

        self._database_url  = database_url
        self._database_name = database_name 

        if  not self._database_url :
            self._database_url = os.getenv ("RADICAL_PILOT_DBURL", None)

        if  not self._database_url :
            raise PilotException ("no database URL (set RADICAL_PILOT_DBURL)")  

        logger.info("using database url  %s" % self._database_url)

        # if the database url contains a path element, we interpret that as
        # database name (without the leading slash)
        tmp_url = ru.Url (self._database_url)
        if  tmp_url.path            and \
            tmp_url.path[0]  == '/' and \
            len(tmp_url.path) >  1  :
            self._database_name = tmp_url.path[1:]
            logger.info("using database path %s" % self._database_name)
        else :
            logger.info("using database name %s" % self._database_name)

        # Loading all "default" resource configurations
        module_path   = os.path.dirname(os.path.abspath(__file__))
        default_cfgs  = "%s/configs/*.json" % module_path
        config_files  = glob.glob(default_cfgs)

        for config_file in config_files:

            try :
                logger.info("Load resource configurations from %s" % config_file)
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e :
                logger.error ("skip config file %s: %s" % (config_file, e))
                continue

            for rc in rcs:
                logger.info("Load resource configurations for %s" % rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        user_cfgs     = "%s/.radical/pilot/configs/*.json" % os.environ.get ('HOME')
        config_files  = glob.glob(user_cfgs)

        for config_file in config_files:

            try :
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e :
                logger.error ("skip config file %s: %s" % (config_file, e))
                continue

            for rc in rcs:
                logger.info("Loaded resource configurations for %s" % rc)

                if  rc in self._resource_configs :
                    # config exists -- merge user config into it
                    ru.dict_merge (self._resource_configs[rc],
                                   rcs[rc].as_dict(),
                                   policy='overwrite')
                else :
                    # new config -- add as is
                    self._resource_configs[rc] = rcs[rc].as_dict() 

        default_aliases = "%s/configs/aliases.json" % module_path
        self._resource_aliases = ru.read_json_str (default_aliases)['aliases']

        ##########################
        ## CREATE A NEW SESSION ##
        ##########################
        if uid is None:
            try:
                self._connected  = None

                if name :
                    self._name = name
                    self._uid  = name
                  # self._uid  = ru.generate_id ('rp.session.'+name+'.%(item_counter)06d', mode=ru.ID_CUSTOM)
                else :
                    self._uid  = ru.generate_id ('rp.session', mode=ru.ID_PRIVATE)
                    self._name = self._uid


                self._dbs, self._created, self._connection_info = \
                        dbSession.new(sid     = self._uid,
                                      name    = self._name,
                                      db_url  = self._database_url,
                                      db_name = self._database_name)

                logger.info("New Session created %s." % str(self))

                _rec = os.environ.get('RADICAL_PILOT_RECORD_SESSION')
                if _rec:
                    self._rec = "%s/%s" % (_rec, self.uid)
                    os.system('mkdir -p %s' % self._rec)
                    ru.write_json({'dburl' : str(self._database_url)}, "%s/session.json" % self._rec)
                    logger.info("recording session in %s" % self._rec)
                else:
                    self._rec = None


            except Exception, ex:
                logger.exception ('session create failed')
                raise PilotException("Couldn't create new session (database URL '%s' incorrect?): %s" \
                                % (self._database_url, ex))  

        ######################################
        ## RECONNECT TO AN EXISTING SESSION ##
        ######################################
        else:
            try:
                self._uid = uid

                # otherwise, we reconnect to an existing session
                self._dbs, session_info, self._connection_info = \
                        dbSession.reconnect(sid     = self._uid, 
                                            db_url  = self._database_url,
                                            db_name = self._database_name)

                self._created   = session_info["created"]
                self._connected = session_info["connected"]

                logger.info("Reconnected to existing Session %s." % str(self))

            except Exception, ex:
                raise PilotException("Couldn't re-connect to session: %s" % ex)  

    #---------------------------------------------------------------------------
    #
    def __del__ (self) :
        self.close ()


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

        logger.debug("session %s closing" % (str(self._uid)))

        uid = self._uid

        if not self._uid:
            logger.error("Session object already closed.")
            return

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
                logger.warning("'delete' flag on session is deprecated. " \
                               "Please use 'cleanup' and 'terminate' instead!")

        if  cleanup :
            # cleanup implies terminate
            terminate = True

        for pmgr in self._pilot_manager_objects:
            logger.debug("session %s closes   pmgr   %s" % (str(self._uid), pmgr._uid))
            pmgr.close (terminate=terminate)
            logger.debug("session %s closed   pmgr   %s" % (str(self._uid), pmgr._uid))

        for umgr in self._unit_manager_objects:
            logger.debug("session %s closes   umgr   %s" % (str(self._uid), umgr._uid))
            umgr.close()
            logger.debug("session %s closed   umgr   %s" % (str(self._uid), umgr._uid))

        if  cleanup :
            self._destroy_db_entry()

        logger.debug("session %s closed" % (str(self._uid)))


    #---------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        object_dict = {
            "uid"           : self._uid,
            "created"       : self._created,
            "connected"     : self._connected ,
            "database_name" : self._connection_info.dbname,
            "database_auth" : self._connection_info.dbauth,
            "database_url"  : self._connection_info.dburl
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
    def name(self):
        return self._name

    #---------------------------------------------------------------------------
    #
    @property
    def created(self):
        """Returns the UTC date and time the session was created.
        """
        self._assert_obj_is_valid()
        return self._created

    #---------------------------------------------------------------------------
    #
    @property
    def connected (self):
        """Returns the most recent UTC date and time the session was
        reconnected to.
        """
        self._assert_obj_is_valid()
        return self._connected 


    #---------------------------------------------------------------------------
    #
    def _destroy_db_entry(self):
        """Terminates the session and removes it from the database.

        All subsequent attempts access objects attached to the session and 
        attempts to re-connect to the session via its uid will result in
        an error.

        **Raises:**
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        self._assert_obj_is_valid()

        self._dbs.delete()
        logger.info("Deleted session %s from database." % self._uid)
        self._uid = None

    #---------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """Lists the unique identifiers of all :class:`radical.pilot.PilotManager` 
        instances associated with this session.

        **Example**::

            s = radical.pilot.Session(database_url=DBURL)
            for pm_uid in s.list_pilot_managers():
                pm = radical.pilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`radical.pilot.PilotManager` uids (`list` oif strings`).

        **Raises:**
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        self._assert_obj_is_valid()
        return self._dbs.list_pilot_manager_uids()


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
        self._assert_obj_is_valid()

        return_scalar = False

        if pilot_manager_ids is None:
            pilot_manager_ids = self.list_pilot_managers()

        elif not isinstance(pilot_manager_ids, list):
            pilot_manager_ids = [pilot_manager_ids]
            return_scalar = True

        pilot_manager_objects = []

        for pilot_manager_id in pilot_manager_ids:
            pilot_manager = PilotManager._reconnect(session=self, pilot_manager_id=pilot_manager_id)
            pilot_manager_objects.append(pilot_manager)

            self._pilot_manager_objects.append(pilot_manager)

        if return_scalar is True:
            pilot_manager_objects = pilot_manager_objects[0]

        return pilot_manager_objects

    #---------------------------------------------------------------------------
    #
    def list_unit_managers(self):
        """Lists the unique identifiers of all :class:`radical.pilot.UnitManager` 
        instances associated with this session.

        **Example**::

            s = radical.pilot.Session(database_url=DBURL)
            for pm_uid in s.list_unit_managers():
                pm = radical.pilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`radical.pilot.UnitManager` uids (`list` of `strings`).

        **Raises:**
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        self._assert_obj_is_valid()
        return self._dbs.list_unit_manager_uids()

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
        self._assert_obj_is_valid()

        return_scalar = False
        if unit_manager_ids is None:
            unit_manager_ids = self.list_unit_managers()

        elif not isinstance(unit_manager_ids, list):
            unit_manager_ids = [unit_manager_ids]
            return_scalar = True

        unit_manager_objects = []

        for unit_manager_id in unit_manager_ids:
            unit_manager = UnitManager._reconnect(session=self, unit_manager_id=unit_manager_id)
            unit_manager_objects.append(unit_manager)

            self._unit_manager_objects.append(unit_manager)

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
                logger.info("Loaded resource configurations for %s" % rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        else :
            self._resource_configs[resource_config.label] = resource_config.as_dict()

    # -------------------------------------------------------------------------
    #
    def get_resource_config (self, resource_key, schema=None):
        """Returns a dictionary of the requested resource config
        """

        if  resource_key in self._resource_aliases :
            logger.warning ("using alias '%s' for deprecated resource key '%s'" \
                         % (self._resource_aliases[resource_key], resource_key))
            resource_key = self._resource_aliases[resource_key]

        if  resource_key not in self._resource_configs:
            error_msg = "Resource key '%s' is not known." % resource_key
            raise PilotException(error_msg)

        resource_cfg = copy.deepcopy (self._resource_configs[resource_key])

        if  not schema :
            if 'schemas' in resource_cfg :
                schema = resource_cfg['schemas'][0]

        if  schema:
            if  schema not in resource_cfg :
                raise RuntimeError ("schema %s unknown for resource %s" \
                                  % (schema, resource_key))

            for key in resource_cfg[schema] :
                # merge schema specific resource keys into the
                # resource config
                resource_cfg[key] = resource_cfg[schema][key]


        return resource_cfg


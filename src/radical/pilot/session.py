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
import threading
import radical.utils as ru

from .utils           import *
from .unit_manager    import UnitManager
from .pilot_manager   import PilotManager
from .resource_config import ResourceConfig
from .exceptions      import PilotException
from .db              import Session as dbSession


# ------------------------------------------------------------------------------
#
class Session (saga.Session):
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
    def __init__ (self, database_url=None, database_name=None, name=None):
        """Creates a new session.

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

        logger = ru.get_logger('radical.pilot')

        if database_name:
            logger.warning("The 'database_name' parameter is deprecated - please specify an URL path")
        else:
            database_name = 'radicalpilot'

        # init the base class inits
        saga.Session.__init__ (self)
        self._dh        = ru.DebugHelper()
        self._valid     = False
        self._terminate = threading.Event()
        self._terminate.clear()

        # before doing anything else, set up the debug helper for the lifetime
        # of the session.
        self._debug_helper = ru.DebugHelper ()

        # Dictionaries holding all manager objects created during the session.
        self._pilot_manager_objects = dict()
        self._unit_manager_objects  = dict()

        # The resource configuration dictionary associated with the session.
        self._resource_configs = {}

        if  not database_url:
            database_url = os.getenv ("RADICAL_PILOT_DBURL", None)

        if  not database_url:
            raise PilotException ("no database URL (set RADICAL_PILOT_DBURL)")  

        dburl = ru.Url(database_url)

        # if the database url contains a path element, we interpret that as
        # database name (without the leading slash)
        if  not dburl.path         or \
            dburl.path[0]   != '/' or \
            len(dburl.path) <=  1  :
            logger.warning("incomplete URLs are deprecated -- missing database name!")
            dburl.path = database_name # defaults to 'radicalpilot'

        logger.info("using database %s" % dburl)

        # ----------------------------------------------------------------------
        # create new session
        self._dbs       = None
        self._uid       = None
        self._connected = None
        self._dburl     = None

        try:
            if name :
                uid = name
                ru.reset_id_counters(prefix=['pmgr', 'umgr', 'pilot', 'unit', 'unit.%(counter)06d'])
            else :
                uid = ru.generate_id ('rp.session', mode=ru.ID_PRIVATE)
                ru.reset_id_counters(prefix=['pmgr', 'umgr', 'pilot', 'unit', 'unit.%(counter)06d'])

            # initialize profiling
            self.prof = Profiler('%s' % uid)
            self.prof.prof('start session', uid=uid)

            logger.report.info ('<<new session: ')
            logger.report.plain('[%s]' % uid)
            logger.report.info ('<<database   : ')
            logger.report.plain('[%s]' % dburl)

            self._dbs = dbSession(sid   = uid,
                                  name  = name,
                                  dburl = dburl)

            # only now the session should have an uid
            self._dburl = self._dbs._dburl
            self._name  = name
            self._uid   = uid

            # from here on we should be able to close the session again
            self._valid = True
            logger.info("New Session created: %s." % str(self))

        except Exception, ex:
            logger.report.error(">>err\n")
            logger.exception ('session create failed')
            raise PilotException("Couldn't create new session (database URL '%s' incorrect?): %s" \
                            % (self._dburl, ex))  

        # Loading all "default" resource configurations
        module_path  = os.path.dirname(os.path.abspath(__file__))
        default_cfgs = "%s/configs/resource_*.json" % module_path
        config_files = glob.glob(default_cfgs)

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

        user_cfgs     = "%s/.radical/pilot/configs/resource_*.json" % os.environ.get ('HOME')
        config_files  = glob.glob(user_cfgs)

        for config_file in config_files:

            try :
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e :
                logger.error ("skip config file %s: %s" % (config_file, e))
                continue

            for rc in rcs:
                logger.info("Load resource configurations for %s" % rc)

                if  rc in self._resource_configs :
                    # config exists -- merge user config into it
                    ru.dict_merge (self._resource_configs[rc],
                                   rcs[rc].as_dict(),
                                   policy='overwrite')
                else :
                    # new config -- add as is
                    self._resource_configs[rc] = rcs[rc].as_dict() 

        default_aliases = "%s/configs/resource_aliases.json" % module_path
        self._resource_aliases = ru.read_json_str (default_aliases)['aliases']

        self.prof.prof('configs parsed', uid=self._uid)

        _rec = os.environ.get('RADICAL_PILOT_RECORD_SESSION')
        if _rec:
            self._rec = "%s/%s" % (_rec, self._uid)
            os.system('mkdir -p %s' % self._rec)
            ru.write_json({'dburl' : str(self._dburl)}, "%s/session.json" % self._rec)
            logger.info("recording session in %s" % self._rec)
        else:
            self._rec = None

        logger.report.ok('>>ok\n')



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

        logger.report.info('closing session %s' % self._uid)
        logger.debug("session %s closing" % (str(self._uid)))
        self.prof.prof("close", uid=self._uid)

        uid = self._uid

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

        if terminate:
            self._terminate.set()

        for pmgr_uid, pmgr in self._pilot_manager_objects.iteritems():
            logger.debug("session %s closes   pmgr   %s" % (str(self._uid), pmgr_uid))
            pmgr.close (terminate=terminate)
            logger.debug("session %s closed   pmgr   %s" % (str(self._uid), pmgr_uid))

        for umgr_uid, umgr in self._unit_manager_objects.iteritems():
            logger.debug("session %s closes   umgr   %s" % (str(self._uid), umgr._uid))
            umgr.close()
            logger.debug("session %s closed   umgr   %s" % (str(self._uid), umgr._uid))

        if  cleanup :
            self.prof.prof("cleaning", uid=self._uid)
            self._dbs.delete()
            self.prof.prof("cleaned", uid=self._uid)
        else:
            self._dbs.close()

        logger.debug("session %s closed" % (str(self._uid)))
        self.prof.prof("closed", uid=self._uid)
        self.prof.close()

        self._valid = False

        logger.report.info('<<session lifetime: %.1fs' % (self.closed - self.created))
        logger.report.ok('>>ok\n')


    #---------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """

        self._is_valid()

        object_dict = {
            "uid"           : self._uid,
            "created"       : self._dbs.created,
            "connected"     : self._dbs.connected,
            "closed"        : self._dbs.closed,
            "database_url"  : str(self._dbs.dburl)
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
        return self._dbs.dburl

    #---------------------------------------------------------------------------
    #
    def get_db(self):

        self._is_valid()
        return self._dbs.get_db()

    #---------------------------------------------------------------------------
    #
    def get_dbs(self):
        return self._dbs

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
        return self._dbs.created


    #---------------------------------------------------------------------------
    #
    @property
    def connected(self):
        """Returns the most recent UTC date and time the session was
        reconnected to.
        """
        return self._dbs.connected 


    #---------------------------------------------------------------------------
    #
    @property
    def closed(self):
        """
        Returns the time of closing
        """
        return self._dbs.closed 


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

            s = radical.pilot.Session(database_url=DBURL)
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


    # -------------------------------------------------------------------------
    #
    def fetch_profiles (self, tgt=None):
        return fetch_profiles (self._uid, dburl=self._dburl, tgt=tgt, session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_json (self, tgt=None):
        return fetch_json (self._uid, dburl=self._dburl, tgt=tgt)



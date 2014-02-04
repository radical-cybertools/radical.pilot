"""
.. module:: sagapilot.session
   :platform: Unix
   :synopsis: Implementation of the Session class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from sagapilot.unit_manager  import UnitManager
from sagapilot.pilot_manager import PilotManager

from sagapilot.utils.logger      import logger

from sagapilot.db                import Session as dbSession
from sagapilot.db                import DBException

import sagapilot.exceptions
from bson.objectid import ObjectId

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
        return self._dict.keys

    def remove(self, key):
        """Remove a process from the registry.
        """
        if key in self._dict:
            del self._dict[key]

# ------------------------------------------------------------------------------
#
class Session(object):
    """A Session encapsulates a SAGA-Pilot instance and is the *root* object
    for all other SAGA-Pilot objects. 

    A Session holds :class:`sagapilot.PilotManager` and :class:`sagapilot.UnitManager`
    instances which in turn hold  :class:`sagapilot.Pilot` and
    :class:`sagapilot.ComputeUnit` instances.

    Each Session has a unique identifier :data:`sagapilot.Session.uid` that can be
    used to re-connect to a SAGA-Pilot instance in the database.

    **Example**::

        s1 = sagapilot.Session(database_url=DBURL)
        s2 = sagapilot.Session(database_url=DBURL, session_uid=s1.uid)

        # s1 and s2 are pointing to the same session
        assert s1.uid == s2.uid
    """


    #---------------------------------------------------------------------------
    #
    def __init__ (self, database_url, database_name="sagapilot", session_uid=None):
        """Creates a new or reconnects to an exising session.

        If called without a session_uid, a new Session instance is created and 
        stored in the database. If session_uid is set, an existing session is 
        retrieved from the database. 

        **Arguments:**
            * **database_url** (`string`): The MongoDB URL. 

            * **database_name** (`string`): An alternative database name 
              (default: 'sagapilot').

            * **session_uid** (`string`): If session_uid is set, we try 
              re-connect to an existing session instead of creating a new one.

        **Returns:**
            * A new Session instance.

        **Raises:**
            * :class:`sagapilot.DatabaseError`

        """

        # Create a new process registry. All objects belonging to this 
        # session will register their worker processes (if they have any)
        # in this registry. This makes it easier to shut down things in 
        # a more coordinate fashion. 
        self._process_registry = _ProcessRegistry()

        try:
            self._database_url  = database_url
            self._database_name = database_name 

            if session_uid is None:
                # if session_uid is 'None' we create a new session
                session_uid = str(ObjectId())
                self._dbs = dbSession.new(sid=session_uid, 
                                          db_url=database_url, 
                                          db_name=database_name)
                self._session_uid   = session_uid
                logger.info("Created new Session %s." % str(self))

            else:
                # otherwise, we reconnect to an exissting session
                self._dbs = dbSession.reconnect(sid=session_uid, 
                                                db_url=database_url, 
                                                db_name=database_name)

                self._session_uid   = session_uid
                logger.info("Reconnected to existing Session %s." % str(self))

        except DBException, ex:
            raise exceptions.SagapilotException("Database Error: %s" % ex)

        # list of security contexts
        self._credentials      = []


    #---------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a dictionary containing the session data.
        """
        return {"uid"           : self._session_uid,
                "database_url"  : self._database_url,
                "database_name" : self._database_name,
                "session_uid"   : self._session_uid,}

    #---------------------------------------------------------------------------
    #
    def __str__(self):
        return str(self.as_dict())

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the session's unique identifier.

       The uid identifies the session in the database and can be used to 
       re-connect to an existing session. 

        **Returns:**
            * A unique identifier (`string`).

        **Raises:**
            * :class:`sagapilot.IncorrectState` if the session is closed
              or doesn't exist. 

        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

        return self._session_uid


    #---------------------------------------------------------------------------
    #
    def add_credential(self, credential):
        """Adds a new security credential to the session.
        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

        self._credentials.append(credential)
        logger.info("Added credential %s to session %s." % (str(credential), self.uid))

    #---------------------------------------------------------------------------
    #
    def list_credentials(self):
        """Lists the security credentials of the session.
        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

        return self._credentials

    #---------------------------------------------------------------------------
    #
    def destroy(self):
        """Terminates the session and removes it from the database.

        All subsequent attempts access objects attached to the session and 
        attempts to re-connect to the session via its uid will result in
        an error.

        **Raises:**
            * :class:`sagapilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

        self._dbs.delete()
        logger.info("Deleted session %s from database." % self._session_uid)
        self._session_uid = None


    #---------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """Lists the unique identifiers of all :class:`sagapilot.PilotManager` 
        instances associated with this session.

        **Example**::

            s = sagapilot.Session(database_url=DBURL)
            for pm_uid in s.list_pilot_managers():
                pm = sagapilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`sagapilot.PilotManager` uids (`list` oif strings`).

        **Raises:**
            * :class:`sagapilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

        return self._dbs.list_pilot_manager_uids()


    # --------------------------------------------------------------------------
    #
    def get_pilot_managers(self, pilot_manager_ids=None) :
        """ Re-connects to and returns one or more existing PilotManager(s).

        **Arguments:**

            * **session** [:class:`sagapilot.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * One or more new [:class:`sagapilot.PilotManager`] objects.

        **Raises:**

            * :class:`sagapilot.SagapilotException` if a PilotManager with 
              `pilot_manager_uid` doesn't exist in the database.
        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

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


        if return_scalar is True:
            pilot_manager_objects = pilot_manager_objects[0]

        return pilot_manager_objects

    #---------------------------------------------------------------------------
    #
    def list_unit_managers(self):
        """Lists the unique identifiers of all :class:`sagapilot.UnitManager` 
        instances associated with this session.

        **Example**::

            s = sagapilot.Session(database_url=DBURL)
            for pm_uid in s.list_unit_managers():
                pm = sagapilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`sagapilot.UnitManager` uids (`list` of `strings`).

        **Raises:**
            * :class:`sagapilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

        return self._dbs.list_unit_manager_uids()

    # --------------------------------------------------------------------------
    #
    def get_unit_managers(self, unit_manager_ids=None) :
        """ Re-connects to and returns one or more existing UnitManager(s).

        **Arguments:**

            * **session** [:class:`sagapilot.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * One or more new [:class:`sagapilot.PilotManager`] objects.

        **Raises:**

            * :class:`sagapilot.SagapilotException` if a PilotManager with 
              `pilot_manager_uid` doesn't exist in the database.
        """
        if not self._session_uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise exceptions.IncorrectState(msg=msg)

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

        if return_scalar is True:
            unit_manager_objects = unit_manager_objects[0]

        return unit_manager_objects


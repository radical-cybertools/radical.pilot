"""
.. module:: sinon.session
   :platform: Unix
   :synopsis: Implementation of the Session class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, RADICAL Group at Rutgers University"
__license__   = "MIT"

import uuid
from sinon.db import Session as dbSession

# ------------------------------------------------------------------------------
#
class Session(object):
    """A Session encapsulates a SAGA-Pilot instance and is the *root* object
    for all other SAGA-Pilot objects. 

    A Session holds :class:`sinon.PilotManager` and :class:`sinon.UnitManager`
    instances which in turn hold  :class:`sinon.Pilot` and
    :class:`sinon.ComputeUnit` instances.

    Each Session has a unique identifier :data:`sinon.Session.uid` that can be
    used to re-connect to a SAGA-Pilot instance in the database.

    **Example**::

        s1 = sinon.Session(database_url=DBURL)
        s2 = sinon.Session(database_url=DBURL, session_uid=s1.uid)

        # s1 and s2 are pointing to the same session
        assert s1.uid == s2.uid
    """


    #---------------------------------------------------------------------------
    #
    def __init__ (self, database_url, session_uid=None, database_name="sinon"):
        """Creates a new or reconnects to an exising session.

        If called without a session_uid, a new Session instance is created and 
        stored in the database. If session_uid is set, an existing session is 
        retrieved from the database. 

        **Arguments:**
            * **database_url** (`string`): The URL of the MongoDB back-end database. 

            * **database_name** (`string`): An alternative root database name 
              (default: 'sinon').

            * **session_uid** (`string`): If session_uid is set, we try re-connect to an
              existing session instead of creating a new one.

        **Raises:**
            * :class:`sinon.SinonException`
        """
        if session_uid is None:
            # if session_uid is 'None' we create a new session
            session_uid = str(uuid.uuid4())
            self._dbs = dbSession.new(sid=session_uid, 
                                      db_url=database_url, 
                                      db_name=database_name)
        else:
            # otherwise, we reconnect to an exissting session
            self._dbs = dbSession.reconnect(sid=session_uid, 
                                            db_url=database_url, 
                                            db_name=database_name)

        self._database_url = database_url
        self._session_uid = session_uid

    #---------------------------------------------------------------------------
    #
    def __repr__(self):
        return {"database_url": self._database_url,
                "session_uid"  : self._session_uid}

    #---------------------------------------------------------------------------
    #
    def __str__(self):
        return str(self.__repr__())

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the session's unique identifier.

       The uid identifies the session in the database and can be used to 
       re-connect to an existing session. 

        **Returns:**
            * A unique identifier (`string`).

        """
        return self._session_uid

    #---------------------------------------------------------------------------
    #
    def destroy(self):
        """Terminates the session and removes it from the database.

        All subsequent attempts access objects attached to the session and 
        attempts to re-connect to the session via its uid will result in
        an error.
        """
        self._dbs.delete()

    #---------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """Lists the unique identifiers of all :class:`sinon.PilotManager` 
        instances associated with this session.

        **Example**::

            s = sinon.Session(database_url=DBURL)
            for pm_uid in s.list_pilot_managers():
                pm = sinon.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`sinon.PilotManager` uids (`list` oif strings`).

        **Raises:**
            * :class:`sinon.SinonException`
        """
        return self._dbs.list_pilot_manager_uids()

    #---------------------------------------------------------------------------
    #
    def list_unit_managers(self):
        """Lists the unique identifiers of all :class:`sinon.UnitManager` 
        instances associated with this session.

        **Example**::

            s = sinon.Session(database_url=DBURL)
            for pm_uid in s.list_unit_managers():
                pm = sinon.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`sinon.UnitManager` uids (`list` of `strings`).

        **Raises:**
            * :class:`sinon.SinonException`
        """
        return self._dbs.list_unit_manager_uids()


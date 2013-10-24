import uuid
import sinon._api as sa
from sinon.db import Session as dbSession

# ------------------------------------------------------------------------------
#
class Session(sa.Session):

    #---------------------------------------------------------------------------
    #
    def __init__ (self, database_url, session_id=None, database_name="sinon"):
        
        if session_id is None:
            # if session_id is 'None' we create a new session
            session_id = str(uuid.uuid4())
            self._dbs = dbSession.new(sid=session_id, db_url=database_url, db_name=database_name)
        else:
            # otherwise, we reconnect to an exissting session
            self._dbs = dbSession.reconnect(sid=session_id, db_url=database_url, db_name=database_name)

        self._database_url = database_url
        self._session_id = session_id

    #---------------------------------------------------------------------------
    #
    def __repr__(self):
        return {"database_url": self._database_url,
                "session_id"  : self._session_id}

    #---------------------------------------------------------------------------
    #
    def __str__(self):
        return str(self.__repr__())

    #---------------------------------------------------------------------------
    #
    def destroy(self):
        """ Terminates the session and removed it from the database.
        """
        self._dbs.delete()

    #---------------------------------------------------------------------------
    #
    @property
    def sid(self):
        """ Returns the session id.
        """
        return self._session_id

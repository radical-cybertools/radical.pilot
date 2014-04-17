"""
.. module:: radical.pilot.utils.db_connection_info
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

from pymongo import *

# ----------------------------------------------------------------------------
#
class DBConnectionInfo(object):
    """DBConnectionInfo encapsulates the information that is necessary to 
    connect to a MongoDB server. 
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, url, dbname, session_id):

        self._url = url
        self._dbname = dbname
        self._session_id = session_id

    # ------------------------------------------------------------------------
    #
    @property 
    def url(self):
        """Returns the database url.
        """
        return self._url

    # ------------------------------------------------------------------------
    #
    @property
    def dbname(self):
        """Returns the database name.
        """
        return self._dbname

    # ------------------------------------------------------------------------
    #
    @property
    def session_id(self):
        """Returns the session id.
        """
        return self._session_id

    # ------------------------------------------------------------------------
    #
    def get_db_handle(self):
        """get_db_handle() returns a MongoDB handle.
        """
        client = MongoClient(self._url)
        return client


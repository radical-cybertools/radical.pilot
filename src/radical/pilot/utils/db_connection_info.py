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
    def __init__(self, dburl, dbname, dbauth, session_id):

        self._dburl  = dburl
        self._dbauth = dbauth
        self._dbname = dbname
        self._session_id = session_id

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
    def dbauth(self):
        """Returns the database auth ('username:password').
        """
        return self._dbauth

    # ------------------------------------------------------------------------
    #
    @property
    def dburl(self):
        """Returns the database auth ('username:password').
        """
        return self._dburl

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
        client = MongoClient(self._dburl)
        return client

    # ------------------------------------------------------------------------
    #
    def __str__ (self):

        return "Connection Info: session %s : %s / %s" \
             % (self._session_id, self._dburl, self._dbname)


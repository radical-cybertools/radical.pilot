
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import copy


# ------------------------------------------------------------------------------
#
class Client(object):
    '''
    An `rp.Client` manages unit managers, pilot managers, and database
    connection for an RP session.
    '''

    def __init__(self, session):

        self._session = session

        self._uid     = '%s.client' % self._session.uid
        self._prof    = self._session.get_profiler(name=self._uid)
        self._rep     = self._session.get_reporter(name=self._uid)
        self._log     = self._session.get_logger  (name=self._uid)
        self._cfg     = copy.deepcopy(self._session._cfg)

        self._pmgrs   = dict()
        self._umgrs   = dict()


    # --------------------------------------------------------------------------
    #
    def close(self, cleanup, terminate):
        '''
        close all managers
        '''

        for umgr_uid,umgr in self._umgrs.iteritems():
            self._log.debug("session %s closes umgr   %s", self._uid, umgr_uid)
            umgr.close()
            self._log.debug("session %s closed umgr   %s", self._uid, umgr_uid)

        for pmgr_uid,pmgr in self._pmgrs.iteritems():
            self._log.debug("session %s closes pmgr   %s", self._uid, pmgr_uid)
            pmgr.close(terminate)
            self._log.debug("session %s closed pmgr   %s", self._uid, pmgr_uid)


    # --------------------------------------------------------------------------
    #
    def is_valid(self):

        ## FIXME
        return True


    # --------------------------------------------------------------------------
    #
    def register_pmgr(self, pmgr):

        self.is_valid()
        self._pmgrs[pmgr.uid] = pmgr


    # --------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """
        Lists the unique identifiers of all :class:`radical.pilot.PilotManager` 
        instances associated with this session.

        **Returns:**
            * A list of :class:`radical.pilot.PilotManager` uids
            (`list` of `strings`).
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
    def register_umgr(self, umgr):

        self.is_valid()
        self._umgrs[umgr.uid] = umgr


    # --------------------------------------------------------------------------
    #
    def list_unit_managers(self):
        """
        Lists the unique identifiers of all :class:`radical.pilot.UnitManager` 
        instances associated with this session.

        **Returns:**
            * A list of :class:`radical.pilot.UnitManager` uids 
              (`list` of `strings`).
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


# -----------------------------------------------------------------------------


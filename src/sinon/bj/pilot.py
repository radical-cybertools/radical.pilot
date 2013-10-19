

import saga
import radical.utils   as ru

import sinon.api       as sa
import sinon
from   attributes  import *
from   constants   import *

import bj_dummy        as bj


# ------------------------------------------------------------------------------
#
class Pilot (Attributes, sa.Pilot) :

    # dict to map pid's to bigjob pilot URLs, pilot descriptions and manager instances
    #
    _pilots = {}

    # we also keep a dict of bj backend entities around, for both pilots and
    # pilot services
    _bj_pilots         = {}
    _bj_pilot_services = {}


    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        # initialize session
        self._sid = sinon.initialize ()

        if  not pid :
            raise sinon.BadParameter ("pilot c'tor requires 'pid' parameter)")

        if  not pid in self._pilots :
            raise sinon.BadParameter ("no such pilot '%s'" % pid)

        pmid  = self._pilots[pid]['pmid']
        descr = self._pilots[pid]['descr']


        # initialize attributes
        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (PID,           pid,   STRING, SCALAR, READONLY)
        self._attributes_register  (DESCRIPTION,   descr, 'any',  SCALAR, READONLY)
        self._attributes_register  (STATE,         None,  STRING, SCALAR, READONLY)
        self._attributes_register  (STATE_DETAIL,  None,  STRING, SCALAR, READONLY)

        # deep inspection
        self._attributes_register  (UNITS,         None,  STRING, VECTOR, READONLY)
        self._attributes_register  (UNIT_MANAGERS, None,  STRING, VECTOR, READONLY)
        self._attributes_register  (PILOT_MANAGER, pmid,  STRING, SCALAR, READONLY)
        # ...

        self._attributes_set_getter (STATE,         self._get_state)
        self._attributes_set_getter (PILOT_MANAGER, self._get_pilot_manager)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _create (cls, description, manager) :
        """
        """

        pid = ru.generate_id ('p.')

        # create a BJ pilot service, then from it create the pilot.  We will
        # always keep the tuple around.
        bj_pilot_service = bj.PilotComputeService (coordination_url=manager.coord)
        bj_pilot         = bj_pilot_service.create_pilot (description)

        self._pilots[pid] = {}
        self._pilots[pid]['pmid']             = manager.id
        self._pilots[pid]['descr']            = description
        self._pilots[pid]['url']              = pilot.get_url ()
        self._pilots[pid]['bj_pilot']         = bj_pilot
        self._pilots[pid]['bj_pilot_service'] = bj_pilot_service

        return cls (pid)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _connect (cls, pid, manager) :
        """
        """

        pid = ru.generate_id ('p.')

        return cls (pid, _description=description, _manager=manager)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None) :

        state = self._get_state ()

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass

    

    # --------------------------------------------------------------------------
    #
    def cancel (self, drain=False) :

        # FIXME
        pass
    

    # --------------------------------------------------------------------------
    #
    def _get_state (self) :

        if  not self._pilot :
            return UNKNOWN

        state = self._pilot.get_state ()

        if state == bj.state.Running : return RUNNING
        if state == bj.state.New     : return NEW
        if state == bj.state.Staging : return STAGING
        if state == bj.state.Failed  : return FAILED
        if state == bj.state.Done    : return DONE
        if state == bj.state.Unknown : return UNKNOWN
        if state == None             : return UNKNOWN

        raise ValueError ('could not get pilot state from BigJob')


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


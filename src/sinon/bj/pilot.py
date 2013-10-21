

import saga
import radical.utils        as ru

import session              as s
import exceptions           as e
import attributes           as att
import sinon.api            as sa

import bj_dummy             as bj


# ------------------------------------------------------------------------------
#
class Pilot (att.Attributes, sa.Pilot) :

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
        self._sid = s.initialize ()

        if  not pid :
            raise e.BadParameter ("pilot c'tor requires 'pid' parameter)")

        if  not pid in self._pilots :
            raise e.BadParameter ("no such pilot '%s'" % pid)

        pmid  = self._pilots[pid]['pmid']
        descr = self._pilots[pid]['descr']


        # initialize attributes
        att.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register   (sa.PID,           pid,   att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register   (sa.DESCRIPTION,   descr,     dict(), att.SCALAR, att.READONLY)
        self._attributes_register   (sa.STATE,         None,  att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register   (sa.STATE_DETAIL,  None,  att.STRING, att.SCALAR, att.READONLY)

        # deep inspection
        self._attributes_register   (sa.UNITS,         None,  att.STRING, att.VECTOR, att.READONLY)
        self._attributes_register   (sa.UNIT_MANAGERS, None,  att.STRING, att.VECTOR, att.READONLY)
        self._attributes_register   (sa.PILOT_MANAGER, pmid,  att.STRING, att.SCALAR, att.READONLY)
        # ...

        self._attributes_set_getter (sa.STATE,         self._get_state)
        self._attributes_set_getter (sa.PILOT_MANAGER, self._get_pilot_manager)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _create (cls, description, manager) :
        """
        """

        pid = ru.generate_id ('p.')

        bjp = bj.get_bj_pilot_api ()

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
    def wait (self, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=None) :

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
            return sa.UNKNOWN

        state = self._pilot.get_state ()

        if state == bj.state.Running : return sa.RUNNING
        if state == bj.state.New     : return sa.NEW
        if state == bj.state.Staging : return sa.STAGING
        if state == bj.state.Failed  : return sa.FAILED
        if state == bj.state.Done    : return sa.DONE
        if state == bj.state.Unknown : return sa.UNKNOWN
        if state == None             : return sa.UNKNOWN

        raise ValueError ('could not get pilot state from BigJob')


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


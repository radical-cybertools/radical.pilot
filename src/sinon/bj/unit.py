

import time

import session
import exceptions     as e
import attributes     as att
import sinon._api     as sa

import bj_dummy       as bj

# ------------------------------------------------------------------------------
#
class Unit (att.Attributes, sa.Unit) :
    """ 
    Base class for DataUnit and ComputeUnit.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid, _description=None, _manager=None, _pid=None) : 


        print "uid          : %s" % uid
        print "_description : %s" % _description 
        print "_manager     : %s" % _manager
        print "_pid         : %s" % _pid

        self.uid = uid
        if  not self.uid :
            raise e.BadParameter ("unit c'tor requires 'uid' parameter)")

        # initialize session
        self._sid = session.initialize ()

        umid = None
        if  _manager :
            umid = _manager.umid

        descr = None
        if  _description :
            descr = _description

        # keep unit in the manager pool of unassigned units if we don't have
        # a specific pilot assigned, yet
        if  not _pid :
            _pid = 'unassigned'

        # FIXME: check if unit is valid
        print 'checking uid validity'

        # FIXME: reconnect to unit
        print 'reconnect to unit'


        # initialize attributes
        att.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # set basic state attributes
        self._attributes_register  (sa.UID,          self.uid,   att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.STATE,        sa.UNKNOWN, att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.STATE_DETAIL, None,       att.STRING, att.SCALAR, att.READONLY)

        # set inspection attributes
        self._attributes_register  (sa.UNIT_MANAGER, umid,       att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.DESCRIPTION,  descr,      att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.PILOT,        _pid,       att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.SUBMIT_TIME,  None,       att.TIME,   att.SCALAR, att.READONLY)
        self._attributes_register  (sa.START_TIME,   None,       att.TIME,   att.SCALAR, att.READONLY)
        self._attributes_register  (sa.END_TIME,     None,       att.TIME,   att.SCALAR, att.READONLY)

        self._attributes_set_getter (sa.STATE,       self._get_state)

        # private attributes
        self._attributes_register   ('_bj_unit',      None,      att.ANY, att.SCALAR, att.WRITEABLE)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=None) :

        if  not isinstance (state, list) :
            state = [state]

        start_wait = time.time ()
        while self.state not in state :
            print "%s waiting for %s (%s)" % (self.uid, state, self.state)
            time.sleep (1)

            if  (None != timeout) and (timeout <= (time.time () - start_wait)) :
                print "wait timeout"
                break

        # done waiting
        return

    # --------------------------------------------------------------------------
    #
    def cancel (self) :
        """
        :param state:  the state to wait for
        :type  state:  enum `state` (PENDING, ACTIVE, DONE, FAILED, CANCELED, UNKNOWN)
        :returns   :  Nothing
        :rtype     :  None
        :raises    :  BadParameter (on invalid initialization)

        Move the unit into Canceled state -- unless it it was in a final state,
        then state is not changed.
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def _get_state (self) :

      # FIXME
      # with self._rlock :
        if True :

            if  not self._bj_unit :
                return sa.UNKNOWN

            state = self._bj_unit.get_state ()

            if state == bj.state.Running : return sa.RUNNING
            if state == bj.state.New     : return sa.PENDING
            if state == bj.state.Staging : return sa.STAGING
            if state == bj.state.Failed  : return sa.FAILED
            if state == bj.state.Done    : return sa.DONE
            if state == bj.state.Unknown : return sa.UNKNOWN
            if state == None             : return sa.UNKNOWN

            raise ValueError ('could not get pilot state from BigJob')



# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


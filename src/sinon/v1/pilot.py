

import saga
import radical.utils   as ru

import sinon._api      as sa
import exceptions      as e
import attributes      as att


# ------------------------------------------------------------------------------
#
class Pilot (att.Attributes, sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid, _description=None, _manager=None) : 


        self.pid = pid
        if  not self.pid :
            raise e.BadParameter ("pilot c'tor requires 'pid' parameter)")

        # initialize session
        self._sid = sinon.initialize ()

        pmid = None
        if  _manager :
            pmid = _manager.pmid

        descr = None
        if  _description :
            descr = _description


        # initialize attributes
        att.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (sa.PID,           pid,   att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.DESCRIPTION,   descr, 'any',      att.SCALAR, att.READONLY)
        self._attributes_register  ('manager',        pmid,  att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.STATE,         None,  att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.STATE_DETAIL,  None,  att.STRING, att.SCALAR, att.READONLY)

        # deep inspection
        self._attributes_register  (sa.UNITS,         None,  att.STRING, att.VECTOR, att.READONLY)
        self._attributes_register  (sa.UNIT_MANAGERS, None,  att.STRING, att.VECTOR, att.READONLY)
        self._attributes_register  (sa.PILOT_MANAGER, None,  att.STRING, att.SCALAR, att.READONLY)
        # ...


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _create (cls, description, manager) :
        """
        """

        pid = ru.generate_id ('p.')

        return cls (pid, _description=description, _manager=manager)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel (self, drain=False) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


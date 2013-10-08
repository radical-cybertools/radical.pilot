

import saga

import sinon.api       as sa
import sinon.utils     as su
import sinon
from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class Pilot (Attributes, sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid, _description=None, _manager=None) : 


        self.pid = pid
        if  not self.pid :
            raise sinon.BadParameter ("pilot c'tor requires 'pid' parameter)")

        # initialize session
        self._sid, self._root = sinon.initialize ()

        pmid = None
        if  _manager :
            pmid = _manager.pmid

        descr = None
        if  _description :
            descr = _description


        # initialize attributes
        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (PID,           pid,   STRING, SCALAR, READONLY)
        self._attributes_register  (DESCRIPTION,   descr, 'any',  SCALAR, READONLY)
        self._attributes_register  ('manager',     pmid,  STRING,  SCALAR, READONLY)
        self._attributes_register  (STATE,         None,  STRING, SCALAR, READONLY)
        self._attributes_register  (STATE_DETAIL,  None,  STRING, SCALAR, READONLY)

        # deep inspection
        self._attributes_register  (UNITS,         None,  STRING, VECTOR, READONLY)
        self._attributes_register  (UNIT_MANAGERS, None,  STRING, VECTOR, READONLY)
        self._attributes_register  (PILOT_MANAGER, None,  STRING, SCALAR, READONLY)
        # ...

        # register state
        pdir = "%s/%s" % (self.manager, self.pid)
        self._base = self._root.open_dir (pdir, flags=saga.advert.CREATE_PARENTS)
        self._base.set_attribute ('manager', self.manager)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _create (cls, description, manager) :
        """
        """

        pid = su.generate_pilot_id ()

        return cls (pid, _description=description, _manager=manager)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None, async=False) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel (self, drain=False, async=False) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


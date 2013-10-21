

import threading
import saga
import radical.utils   as ru

import session         as s
import exceptions      as e
import attributes      as att
import pilot_manager   as pm
import sinon._api      as sa


# ------------------------------------------------------------------------------
#
class Pilot (att.Attributes, sa.Pilot) :

    # dict to map pid's to bigjob pilot URLs, pilot descriptions and manager instances
    #
    _pilots = {}


    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        # initialize
        self._rlock = threading.RLock ()
        self._sid   = s.initialize ()

        if  not pid :
            raise e.BadParameter ("pilot c'tor requires 'pid' parameter)")

        if  not pid in self._pilots :
            raise e.BadParameter ("no such pilot '%s'" % pid)

        pmid     = self._pilots[pid]['pmid']
        descr    = self._pilots[pid]['descr']


        # initialize attributes
        att.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (sa.PID,           pid,   att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register   (sa.DESCRIPTION,   descr,  att.ANY,    att.SCALAR, att.READONLY)
        self._attributes_register  (sa.STATE,         None,  att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.STATE_DETAIL,  None,  att.STRING, att.SCALAR, att.READONLY)

        # deep inspection
        self._attributes_register  (sa.UNITS,         None,  att.STRING, att.VECTOR, att.READONLY)
        self._attributes_register  (sa.UNIT_MANAGERS, None,  att.STRING, att.VECTOR, att.READONLY)
        self._attributes_register   (sa.PILOT_MANAGER, pmid,   att.STRING, att.SCALAR, att.READONLY)
        # ...

        self._attributes_set_getter (sa.STATE,         self._get_state)
        self._attributes_set_getter (sa.PILOT_MANAGER, self._get_pilot_manager)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _create (cls, description, manager) :
        """
        """

      # FIXME
      # with self._rlock :
        if True :

            pid = ru.generate_id ('p.')

            print description


            cls._pilots[pid] = {}
            cls._pilots[pid]['pmid']             = manager.pmid
            cls._pilots[pid]['descr']            = description

            return cls (pid)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _connect (cls, pid, manager) :
        """
        """

        with self._rlock :

            pid = ru.generate_id ('p.')

            return cls (pid, _description=description, _manager=manager)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=None) :

        with self._rlock :

            if  not isinstance (state, list) :
                state = [state]

                start_wait = time.time ()
                while self.state not in state :
                    print "%s waiting for %s (%s)" % (self.pid, state, self.state)
                    time.sleep (1)

                    if  (None != timeout) and (timeout <= (time.time () - start_wait)) :
                        print "wait timeout"
                        break

                # done waiting
                return


    # --------------------------------------------------------------------------
    #
    def cancel (self, drain=False) :

        with self._rlock :

            # FIXME drain

            if  self.state in [sa.DONE, sa.FAILED, sa.CANCELED] :
                # nothing to do
                return

            if  self.state in [sa.UNKNOWN] :
                raise e.IncorrectState ("Pilot state is UNKNOWN, cannot cancel")

            # FIXME
    

# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


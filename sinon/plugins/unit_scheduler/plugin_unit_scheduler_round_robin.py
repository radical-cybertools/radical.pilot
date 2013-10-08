
import sinon

# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'name'        : 'round_robin', 
    'version'     : '0.1',
    'type'        : 'unit_scheduler', 
    'description' : 'simple scheduler, assigns CUs to pilots in round-robin fashion.'
  }

# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS (object) :
    """
    This class implements the (trivial) round-robin scheduler for Sinon.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        print "loading the round-robin unit plugin"
        self._manager    = None
        self._last_pilot = -1


    # --------------------------------------------------------------------------
    #
    def init (self, manager) :

        self._manager = manager


    # --------------------------------------------------------------------------
    #
    def schedule (self, unit_descr) :

        print "round-robin scheduling of %s" % unit_descr

        if  not self._manager :
            print "WARNING: unit scheduler is not initialized"
            return None

        pilots = self._manager.pilots

        print "pilots: %s" % pilots
        print "len   : %s" % len(pilots)
        print "last  : %s" % self._last_pilot

        # check if a pilot beyond the last pilot exists -- if so use it, if not,
        # start afresh
        if  len (pilots) > (self._last_pilot + 1) :
            self._last_pilot += 1

        elif len (pilots) :
            self._last_pilot = 0

        else :
            # no pilots?  Duh!
            print "WARNING: unit scheduler cannot operate on empty pilot set"
            return None
        
        print "round-robin scheduling of %s" % pilots[self._last_pilot]
        return pilots[self._last_pilot]


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


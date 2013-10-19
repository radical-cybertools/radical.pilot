
import sinon

# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'name'        : 'direct-submission', 
    'version'     : '0.1',
    'type'        : 'unit_scheduler', 
    'description' : 'makes sure that units end up on a unique pilot.'
  }

# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS (object) :
    """
    This class implements the (trivial) direct-submission scheduler for Sinon.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        print "loading the direct_submission unit plugin"
        self._manager    = None
        self._last_pilot = -1


    # --------------------------------------------------------------------------
    #
    def init (self, manager) :

        self._manager = manager


    # --------------------------------------------------------------------------
    #
    def schedule (self, unit_descr) :

        print "direct submission of %s" % unit_descr

        if  not self._manager :
            print "WARNING: unit scheduler is not initialized"
            return None

        pilots = self._manager.pilots

        if len (pilots) > 1:
            raise sinon.IncorrectState ('Direct Submission only works for a single pilot!')

        
        print "direct submission to %s" % pilots[0]
        return pilots[0]


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4


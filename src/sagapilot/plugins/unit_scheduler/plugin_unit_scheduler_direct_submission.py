
import sinon

# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'name'        : 'direct_submission', 
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

      # print "loading the direct_submission unit plugin"
        self._manager    = None
        self._last_pilot = -1


    # --------------------------------------------------------------------------
    #
    def init (self, manager) :

        self._manager = manager


    # --------------------------------------------------------------------------
    #
    def schedule (self, unit_descriptions) :

        if  not self._manager:
            raise RuntimeError ('Unit scheduler is not initialized')

        pilots = self._manager.list_pilots ()

        if not len (pilots):
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')

        if len (pilots) > 1:
            raise RuntimeError ('Direct Submission only works for a single pilot!')

        
        ret            = dict()
        ret[pilots[0]] = list ()
        for ud in unit_descriptions :
            ret[pilots[0]].append (ud)

      # print "direct submission to %s" % pilots[0]

        return ret


# ------------------------------------------------------------------------------
#



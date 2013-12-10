
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

        #print "loading the round-robin unit plugin"
        self._manager = None
        self._idx     = 0


    # --------------------------------------------------------------------------
    #
    def init (self, manager) :

        self._manager = manager


    # --------------------------------------------------------------------------
    #
    def schedule (self, unit_descriptions) :

        # the scheduler will return a dictionary of the form:
        #   { 
        #     pilot_id_1  : [ud_1, ud_2, ...], 
        #     pilot_id_2  : [ud_3, ud_4, ...], 
        #     ...
        #   }
        # The scheduler may not be able to schedule some units -- those will
        # simply not be listed for any pilot.  The UM needs to make sure
        # that no UD from the original list is left untreated, eventually.

        #print "round-robin scheduling of %s units" % len(unit_descriptions)

        if  not self._manager :
            print "WARNING: unit scheduler is not initialized"
            return None

        pilots = self._manager.list_pilots ()
        ret    = dict()

        if not len (pilots) :
            print "WARNING: unit scheduler cannot operate on empty pilot set"
            return ret


        for ud in unit_descriptions :
            
            if  self._idx >= len(pilots) : 
                self._idx = 0
            
            pilot = pilots[self._idx]

            if  pilot not in ret :
                ret[pilot] = []

            ret[pilot].append (ud)

            self._idx += 1


        return ret


    # --------------------------------------------------------------------------
    #
    def schedule_bulk (self, unit_descriptions) :
        pass


# ------------------------------------------------------------------------------
#



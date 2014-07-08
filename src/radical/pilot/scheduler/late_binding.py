#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler.LateBindingScheduler
   :platform: Unix
   :synopsis: A multi-pilot, late-binding scheduler.

.. moduleauthor:: Mark Santcroos <mark.santcroos@rutgers.edu>
"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 

from radical.pilot.utils.logger        import logger
from radical.pilot.scheduler.interface import Scheduler
from radical.pilot.states              import ACTIVE

# -----------------------------------------------------------------------------
# 
class LateBindingScheduler(Scheduler):
    """LateBindingScheduler implements a multi-pilot, late-binding
    scheduling algorithm. Only schedules CUs to Pilots that are active and have a free-slot.
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self):
        """
        """
        Scheduler.__init__(self)
        logger.info("Loaded scheduler: %s." % self.name)


    # -------------------------------------------------------------------------
    #
    def _name(self):
        return "LateBindingScheduler"

    
    # -------------------------------------------------------------------------
    #
    def unit_callback (self, cu, state) :
        
        logger.debug ("unit %s changed to %s" % (cu.uid, state))

        self.re_schedule (cu.cores
    
    # -------------------------------------------------------------------------
    #
    def schedule(self, manager, unit_descriptions):
        # the scheduler will return a dictionary of the form:
        #   { 
        #     pilot_id_1  : [ud_1, ud_2, ...], 
        #     pilot_id_2  : [ud_3, ud_4, ...], 
        #     ...
        #   }
        # The scheduler may not be able to schedule some units -- those will
        # simply not be listed for any pilot.  The UM needs to make sure
        # that no UD from the original list is left untreated, eventually.

        print "Late-binding scheduling of %s units" % len(unit_descriptions)

        if not manager:
            raise RuntimeError ('Unit scheduler is not initialized')

        pilots = manager.list_pilots()
        print 'Pilots: %s' % pilots

        if not len(pilots) :
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')

        ret = {}

        for ud in unit_descriptions:

            found_slot = False

            if ud.cores > 1:
                raise Exception("Late-binding scheduler only supports single core tasks for now!")

            pilots_json = manager._worker._db.get_pilots(pilot_ids=pilots)

            for pilot in pilots_json:
                pilot_uid = str(pilot['_id'])

                if pilot['state'] == ACTIVE:
                    print 'Found an active Pilot: %s' % pilot_uid
                else:
                    print 'Found an inactive Pilot %s, skipping ...' %  pilot_uid
                    continue

                for node in pilot['slots']:

                    if 'Free' in node['cores']:
                        print 'Found a free slot at node %s to schedule on Pilot %s!' % (node['node'], pilot_uid)

                        if pilot_uid not in ret :
                            ret[pilot_uid] = []
                        ret[pilot_uid].append(ud)

                        found_slot = True
                        break

                if found_slot:
                    break
                else:
                    print 'Did not find a free slot to schedule on Pilot %s!' % pilot_uid

        return ret

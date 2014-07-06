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
from radical.pilot.states              import *

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
    def __del__(self):
        """
        """

    # -------------------------------------------------------------------------
    #
    def _name(self):
        return "LateBindingScheduler"

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

            if ud.cores > 1:
                raise Exception("Late-binding scheduler only supports single core tasks for now!")

            pilots_json = manager._worker._db.get_pilots(pilot_ids=pilots)

            for pilot in pilots_json:
                if pilot['state'] == 'Active':
                    print 'Found an active Pilot'

                if 'Free' in pilot['slots'][0]['cores']:
                    pilot_uid = str(pilot['_id'])

                    print 'Found a free slot to schedule on Pilot %s!' % pilot_uid

                    if pilot_uid not in ret :
                        ret[pilot_uid] = []

                    ret[pilot_uid].append(ud)

                else:
                    print 'Did not found a free slot to schedule!'

        return ret

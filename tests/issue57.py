
import os
import sys
import radical.pilot as rp

# ##############################################################################
# #57: Bulk CU submission hangs forewer on HOTEL
# ##############################################################################

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#-------------------------------------------------------------------------------

if __name__ == "__main__":

    print "Test: Adding CUs in bulks"

    for i in [8, 16]:

        session = rp.Session()

        c = rp.Context('ssh')
        c.user_id = 'merzky'
        session.add_context(c)

        pm = rp.PilotManager (session=session)
        um = rp.UnitManager  (session=session, scheduler=rp.SCHED_ROUND_ROBIN) 

        pilot = []

        pd = rp.ComputePilotDescription()
        pd.resource = "hotel.futuregrid.org"
        pd.cores = i
        pd.runtime = 10
        #pd.cleanup = True
        pilot.append(pd)

        pilots = pm.submit_pilots(pilot)

        um.add_pilots(pilots)

        unit_descrs = []
        print "submitting %d CUs to pilot" % ( i*2 )
        for k in range(0, i*2):
            cu = rp.ComputeUnitDescription()
            cu.cores = 1
            cu.executable = "/bin/date"
            unit_descrs.append(cu)


        print "submitting CUS %s" % unit_descrs
        um.submit_units(unit_descrs)

        print "* Waiting for all compute units to finish..."
        um.wait_units()

        print "  FINISHED"
        pm.cancel_pilots(pilots.uid)       

#-------------------------------------------------------------------------------


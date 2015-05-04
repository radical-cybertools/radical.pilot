#!/usr/bin/env python


import sys
import radical.pilot as rp

# ##############################################################################
# #124: CUs are failing on Trestles
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


#------------------------------------------------------------------------------
#
session = rp.Session()

try:

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    c.user_id = 'amerzky'
    session.add_context(c)

    pmgr = rp.PilotManager(session=session)
    pmgr.register_callback(pilot_state_cb)

    pd = rp.ComputePilotDescription()
    pd.resource = "xsede.trestles"
    pd.cores    = 1
    pd.runtime  = 10
    pd.cleanup  = True

    pilot_object = pmgr.submit_pilots(pd)
    
    umgr = rp.UnitManager(session=session, scheduler=rp.SCHED_ROUND_ROBIN)

    umgr.add_pilots(pilot_object)

    compute_units = []
    for k in range(0, 32):
        cu = rp.ComputeUnitDescription()
        cu.cores = 1
        cu.executable = "/bin/date"
        compute_units.append(cu)

    units = umgr.submit_units(compute_units)

    print "Waiting for all compute units to finish..."
    umgr.wait_units()

    for unit in units :
        assert (unit.state == rp.DONE)

    print "  FINISHED"
    pmgr.cancel_pilots()
    pmgr.wait_pilots()

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()


# ------------------------------------------------------------------------------



import sys
import radical.pilot as rp
import os

# #124: CUs are failing on Trestles

PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[AppCallback]: ComputePilot '{0}' state changed to {1}.".format(
        pilot.uid, state)

    if state == rp.states.FAILED:
        sys.exit(1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    print "[AppCallback]: ComputeUnit '{0}' state changed to {1}.".format(
        unit.uid, state)
    if state == rp.states.FAILED:
        print "            Log: %s" % unit.log[-1]


def run():
    try:
        session = rp.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        c = rp.Context('ssh')
        c.user_id = 'amerzky'
        session.add_context(c)

        pm = rp.PilotManager(session=session)
        pm.register_callback(pilot_state_cb)

        pd = rp.ComputePilotDescription()
        pd.resource = "trestles.sdsc.xsede.org"
        pd.cores    = 1
        pd.runtime  = 10
        pd.cleanup  = True

        pilot_object = pm.submit_pilots(pd)
        
        um = rp.UnitManager(session=session, scheduler=rp.SCHED_ROUND_ROBIN)

        um.add_pilots(pilot_object)

        compute_units = []
        for k in range(0, 32):
            cu = rp.ComputeUnitDescription()
            cu.cores = 1
            cu.executable = "/bin/date"
            compute_units.append(cu)

        submitted_units = um.submit_units(compute_units)

        print "Waiting for all compute units to finish..."
        um.wait_units()

        print "  FINISHED"
        pm.cancel_pilots()
        return 0

    except rp.PilotException, ex:
        print "Error: %s" % ex
        return -1


#-------------------------------------------------------------------------------

if __name__ == "__main__":

    sys.exit (run())

                                                                                                                                         

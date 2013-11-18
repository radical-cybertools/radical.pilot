__copyright__ = "Copyright 2013, The RADICAL Group at Rutgers University"
__license__   = "MIT"

import sys
import sinon
import time

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json'

#-------------------------------------------------------------------------------
#
def demo_milestone_02():
    """Demo for Milestone 2: Submit a single pilot to FutureGrid an execute 
    O(10) work units. Document performance.
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Submit a 16-core pilot to sierra.futuregrid.org
        pd = sinon.ComputePilotDescription()
        pd.resource = "futuregrid.INDIA"
        pd.cores = 16
        pd.cleanup = True
        sierra_pilot = pm.submit_pilots(pd)

        # Create a workload of 64 '/bin/sleep' compute units
        compute_units = []
        for unit_count in range(0, 64):
            cu = sinon.ComputeUnitDescription()
            cu.executable = "/bin/sleep"
            cu.arguments = ['60']
            compute_units.append(cu)

        # Combine the pilot, the workload and a scheduler via 
        # a UnitManager.
        um = sinon.UnitManager(session=session, scheduler="ROUNDROBIN")
        um.add_pilot(sierra_pilot)
        um.submit_units(compute_units)

        sierra_pilot.wait()

        # Wait for all compute units to finish.
        um.wait_units()

        # Cancel all pilots.
        pm.cancel_pilots()

        return 0

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        return -1

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(demo_milestone_02())

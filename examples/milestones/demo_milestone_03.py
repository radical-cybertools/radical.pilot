"""Demo for Milestone 3: 
    * submit 2 Pilot to resource_B and 2 to sierra
    * run 10 bulks of 10 CUs (CUs vary in runtime)
    * after 5 bulks: disconnect / reconnect
    * state changes for pilots and CUs are delivered via notifications
    * performance for above is measured and reported routinely
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import sinon
import time
from random import randint

PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------
# Change these according to your needs 
CFG_USERNAME      = "oweidner"

CFG_RESOURCE_A    = "hotel.futuregrid.org"    
CFG_WORKING_DIR_A = "/gpfs/scratch/oweidner/sinon"
CFG_NUMCORES_A    = 32

CFG_RESOURCE_B    = "india.futuregrid.org"    
CFG_WORKING_DIR_B = "/N/u/oweidner/sinon"
CFG_NUMCORES_B    = 32

#-------------------------------------------------------------------------------
#
def demo_milestone_03_part_1():
    """PART 1: Create two 32-core pilots on resource_A and resource_B, submit 16 bulks 
    of 32 compute unites and disconnect. 
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        cred = sinon.SSHCredential()
        cred.user_id = CFG_USERNAME

        session.add_credential(cred)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pmgr = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Define a 16-core pilot to resource_A.futuregrid.org
        pd_resource_A = sinon.ComputePilotDescription()
        pd_resource_A.resource          = CFG_RESOURCE_A
        pd_resource_A.sandbox           = CFG_WORKING_DIR_A
        pd_resource_A.cores             = CFG_NUMCORES_A
        pd_resource_A.runtime           = 10 # minutes

        # Define a 16-core pilot to resource_B.futuregrid.org
        pd_resource_B = sinon.ComputePilotDescription()
        pd_resource_B.resource          = CFG_RESOURCE_B
        pd_resource_B.sandbox            = CFG_WORKING_DIR_B
        pd_resource_B.cores             = CFG_NUMCORES_B
        pd_resource_B.runtime           = 10 # minutes

        # Submit both pilots
        pilots = pmgr.submit_pilots([pd_resource_A, pd_resource_B])

        # Set to true if one or more pilots fail
        failed = False

        # Error checking.
        for pilot in pilots:
            state = pilot.wait(state=[sinon.states.RUNNING, sinon.states.FAILED])
            if state == sinon.states.FAILED:
                print "  [ERROR] Pilot %s failed: %s." % (pilot, pilot.state_details[-1])
                failed = True
            else:
                print "  [OK]    Pilot %s submitted successfully: %s." % (pilot, pilot.state_details[-1])

        # If one or more pilots fail, we call cancel() on all pilots
        # and abort the example! 
        if failed == True:
            for pilot in pilots:
                pilot.cancel()
            sys.exit(1)

        print "  \n  <Submitted pilots to '%s'>" % [pd_resource_A.resource, pd_resource_B.resource]


        # Create a new unit manager, attach both pilots and select
        # 'round_robin' as the scheduling method.
        umgr = sinon.UnitManager(session=session, scheduler=sinon.SCHED_ROUND_ROBIN)
        umgr.add_pilots(pilots)

        # Submit 16 bulks of 64 tasks with varying runtime runtime varies 
        # between 1 and 10 seconds
        for bulk in xrange(1, 3):
            compute_units = []
            for _ in xrange(0, 32):
                cunit = sinon.ComputeUnitDescription()
                cunit.cores = 1
                cunit.executable = "/bin/sleep"
                cunit.arguments  = ["%s" % str(randint(1, 2))]
                compute_units.append(cunit)
            umgr.submit_units(compute_units)
            print "  <Submmitted %s bulk(s) of 64 compute units>" % bulk

        # Done for now. We return the IDs of the manager objects so that we 
        # can reconnect later. 
        return (session.uid, pmgr.uid, umgr.uid)

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        sys.exit(255)

#-------------------------------------------------------------------------------
#
def demo_milestone_03_part_2(session_id, pmgr_id, umgr_id):
    """PART 2: Re-connect, print some information about the re-connected
    instances and submit another 16 bulks of 32 compute units. Then we 
    wait until everything has finished and cancel the compute pilots.
    """
    try:
        # Re-connect to the previously created session via its ID.
        session = sinon.Session(session_uid=session_id, database_url=DBURL)
        print "  Session: %s" % str(session)

        # Re-connect to the pilot manager and print some information about it
        pmgr = session.get_pilot_managers(pilot_manager_ids=pmgr_id)
        print "  |\n  |- Pilot Manager: %s " % str(pmgr) 

        # Get the pilots from the pilot manager and print some information about them
        pilots = pmgr.get_pilots()
        for pilot in pilots:
            print "  |  |- Pilot: %s " % str(pilot)

        # Re-connect to the unit manager and print some information about it
        umgr = session.get_unit_managers(unit_manager_ids=umgr_id)
        print "  |\n  |- Unit Manager: %s " % str(umgr)
        print "  |  |- Units: %s" % (len(umgr.list_units()))

        raw_input("\nPress Enter to add more units ...\n")

        # Submit 16 bulks of 64 tasks with varying runtime runtime varies 
        # between 1 and 20 seconds
        for bulk in xrange(1, 3):
            compute_units = []
            for _ in xrange(0, 32):
                cunit = sinon.ComputeUnitDescription()
                cunit.cores = 1
                cunit.executable = "/bin/sleep"
                cunit.arguments  = ["%s" % str(randint(1, 2))]
                compute_units.append(cunit)
            umgr.submit_units(compute_units)
            print "  <Submmitted %s bulk(s) of 64 compute units>" % bulk

        # Show information about the unit manager again.
        print " \n  |- Unit Manager: %s " % str(umgr)
        print "  |  |- Units: %s" % (len(umgr.list_units()))

        # Wait for all compute units to finish.
        print "\n  <Waiting for all compute units to finish...>"
        umgr.wait_units()
        print "  <FINISHED>"

        raw_input("\nPress Enter to inspect units ...\n")

        for unit in umgr.get_units():
            print unit

    except sinon.SinonException, ex:
        print "Error: %s" % ex

    finally:
        # cancel the pilots
        pmgr.cancel_pilots()

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    session_id, pmgr_id, umgr_id = demo_milestone_03_part_1()

    raw_input("\nPress Enter to reconnect ...\n")

    demo_milestone_03_part_2(session_id, pmgr_id, umgr_id)
    
    sys.exit(0)




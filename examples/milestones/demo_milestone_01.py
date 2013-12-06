__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import sinon

PWD    = os.path.dirname(os.path.abspath(__file__))

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD
    


#-------------------------------------------------------------------------------
#
def demo_milestone_01_1():
    """Demo part 01: Create a new Session with a PilotManager and UnitManager.
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)
        um = sinon.UnitManager(session=session, scheduler="round_robin")

        # Add a Pilot Manager and a Pilot to the session.
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        pd = sinon.ComputePilotDescription()
        pd.resource          = "futuregrid.SIERRA"
        pd.working_directory = "/N/u/oweidner/sinon"
        pd.cores             = 8
        pd.run_time          = 10 # minutes

        p1 = pm.submit_pilots(pd)

        # Error checking
        if p1.state in [sinon.states.FAILED]:
            print "* [ERROR] Pilot %s failed: %s." % (p1, p1.state_details[-1])
            sys.exit(-1)

        # Add a Unit Manager to the session and add the newly created 
        # pilot to it.
        um = sinon.UnitManager(session=session, scheduler="round_robin")
        um.add_pilots(p1)

        # Now we create a few ComputeUnits ...
        compute_units = []
        for unit_count in range(0, 16):
            cu = sinon.ComputeUnitDescription()
            cu.executable = "/bin/hostname"
            compute_units.append(cu)

        # ... and add them to the manager. Note that this happens in bulk!
        um.submit_units(compute_units)

        # Print and return the session id so we can re-connect to it later.
        print "* Session created with session ID %s" % session.uid
        return session.uid

    except sinon.SinonException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------
#
def demo_milestone_01_2(session_uid):
    """Demo part 02: Re-connect to the previously created session.
    """
    try:
        # Re-connect to the previously created session via its ID.
        session = sinon.Session(session_uid=session_uid, database_url=DBURL)
        print "* Reconnected to session with session ID %s" % session.uid

        for pm_uid in session.list_pilot_managers():
            pm = sinon.PilotManager.get(session=session, pilot_manager_id=pm_uid)
            print "   * Found Pilot Manager with ID %s" % pm.uid
            for pilot_ids in pm.list_pilots():
                print "      * Owns Pilot [%s]" % pilot_ids

        for um_uid in session.list_unit_managers():
            um = sinon.UnitManager.get(session=session, unit_manager_id=um_uid)
            print "   * Found Unit Manager with ID %s" % um.uid
            for pilot_ids in um.list_pilots():
                print "      * Associated with Pilot [%s]" % pilot_ids
                print "      * Work units: %s" % um.list_units()

    except sinon.SinonException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------
#
def demo_milestone_01_3(session_uid):
    """Demo part 03: Delete and remove session from the database.
    """
    try:
        # Re-connect to the previously created session via its ID.
        session = sinon.Session(session_uid=session_uid, database_url=DBURL)
        session.destroy()

    except sinon.SinonException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session
    print "\n%s" % demo_milestone_01_1.__doc__.rstrip()
    session_uid = demo_milestone_01_1()

    raw_input("\nPress Enter to reconnect to session...")

    # Reconnect to that session
    print "\n%s" % demo_milestone_01_2.__doc__.rstrip()
    demo_milestone_01_2(session_uid=session_uid)

    raw_input("\nPress Enter to delete session...")

    # Finally, we delete the session
    print "\n%s\n" % demo_milestone_01_3.__doc__.rstrip()
    demo_milestone_01_3(session_uid=session_uid)


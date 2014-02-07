__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import sinon

PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------
# Change these according to your needs 
CFG_USERNAME    = "oweidner"
CFG_RESOURCE    = "localhost"    

#-------------------------------------------------------------------------------
#
def demo_milestone_01_1():
    """Demo part 01: Create a new Session with a PilotManager and UnitManager.
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
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Submit a 16-core pilot to india.futuregrid.org
        pd = sinon.ComputePilotDescription()
        pd.resource  = CFG_RESOURCE
        pd.cores     = 8
        pd.runtime   = 10 # minutes

        print "* Submitting pilot to '%s'..." % (pd.resource)
        p1 = pm.submit_pilots(pd)

        state = p1.wait(state=[sinon.states.RUNNING, sinon.states.FAILED])

        # If the pilot is in FAILED state it probably didn't start up properly. 
        if state == sinon.states.FAILED:
            print "  [ERROR] Pilot %s failed: %s." % (p1, p1.log[-1])
            sys.exit(-1)
        else:
            print "  [OK]    Pilot %s submitted successfully: %s." % (p1, p1.log[-1])

        # Create a workload of 64 '/bin/hostname' compute units
        compute_units = []
        for unit_count in range(0, 64):
            cu = sinon.ComputeUnitDescription()
            cu.executable = "/bin/hostname"
            compute_units.append(cu)

        # Combine the pilot, the workload and a scheduler via 
        # a UnitManager.
        um = sinon.UnitManager(session=session, scheduler=sinon.SCHED_ROUND_ROBIN)
        um.add_pilots(p1)
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

        for pm in session.get_pilot_managers():
            print "   * Found Pilot Manager with ID %s" % pm.uid
            for pilot_ids in pm.list_pilots():
                print "      * Owns Pilot [%s]" % pilot_ids

        for um in session.get_unit_managers():
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


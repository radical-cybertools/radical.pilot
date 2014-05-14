import os
import sys
import radical.pilot

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

RCONF  = ["https://raw.github.com/radical-cybertools/radical.pilot/devel/configs/xsede.json",
          "https://raw.github.com/radical-cybertools/radical.pilot/devel/configs/futuregrid.json"]


#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
        pilot.uid, state)

    if state == radical.pilot.states.FAILED:
        sys.exit(1)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # SAGA-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session, resource_configurations=RCONF)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 2-core local pilot that runs for 10 minutes.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "localhost"
        pdesc.runtime = 10
        pdesc.cores = 2
        
        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        pmgr.wait_pilots()

        print "\n== PILOT STATE HISTORY==\n"

        for state in pilot.state_history:
            print " * %s: %s\n" % (state.timestamp, state.state)

        # Remove session from database
        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

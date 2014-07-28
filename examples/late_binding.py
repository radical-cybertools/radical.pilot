import os
import sys
import radical.pilot as rp

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!
#
# RADICAL-Pilot uses ssh to communicate with the remote resource. The 
# easiest way to make this work seamlessly is to set up ssh key-based
# authentication and add the key to your keychain so you won't be 
# prompted for a password. The following article explains how to set 
# this up on Linux:
#   http://www.cyberciti.biz/faq/ssh-password-less-login-with-dsa-publickey-authentication/


# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

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

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = rp.Session(database_url=DBURL)

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
  # c.user_id = "tg803521"
    session.add_context(c)
    print "========================="
    print session.uid
    print "========================="

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    pdesc = rp.ComputePilotDescription()
    pdesc.resource  = "localhost"
    pdesc.runtime   = 20 # minutes
    pdesc.cores     = 1
    pdesc.cleanup   = True
  # pdesc.queue     = "normal"
  # pdesc.project   = "TG-MCB140109"

    pilot_1 = pmgr.submit_pilots(pdesc)

    pdesc = rp.ComputePilotDescription()
    pdesc.resource  = "india.futuregrid.org"
    pdesc.runtime   = 20 # minutes
    pdesc.cores     = 16
    pdesc.cleanup   = True
  # pdesc.queue     = "normal"
  # pdesc.project   = "TG-MCB140109"

    # Launch the pilot.
    pilot_2 = pmgr.submit_pilots(pdesc)

    pdesc = rp.ComputePilotDescription()
    pdesc.resource  = "india.futuregrid.org"
    pdesc.runtime   = 20 # minutes
    pdesc.cores     = 8
    pdesc.cleanup   = True
  # pdesc.queue     = "normal"
  # pdesc.project   = "TG-MCB140109"

    # Launch the pilot.
    pilot_3 = pmgr.submit_pilots(pdesc)

    # Create a workload of 8 ComputeUnits.  Each compute unit
    # uses /bin/cat to concatenate two input files, file1.dat and
    # file2.dat. The output is written to STDOUT. cu.environment is
    # used to demonstrate how to set environment variables withih a
    # ComputeUnit - it's not strictly necessary for this example. As
    # a shell script, the ComputeUnits would look something like this:
    #
    #    export INPUT1=file1.dat
    #    export INPUT2=file2.dat
    #    /bin/cat $INPUT1 $INPUT2
    #
    cus = list()

  # for unit_count in range(0, 30):
  #     cu = rp.ComputeUnitDescription()
  #     cu.executable  = "/bin/sleep"
  #     cu.arguments   = ["5"]
  #     cu.cores       = 1
  #   # cu.input_data  = ["/tmp/test.in.dat"]
  #     cus.append(cu)

    for unit_count in range(0, 30):
        cu = rp.ComputeUnitDescription()
        cu.kernel      = 'TEST'
        cu.arguments   = ["5"]
        cus.append(cu)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager (session=session, scheduler=rp.SCHED_LATE_BINDING)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_change_cb)

    # Add the previsouly created ComputePilot to the UnitManager.
    umgr.add_pilots([pilot_1, pilot_2, pilot_3])

  # # wait until first pilots become active
    pilot_1.wait (state=rp.ACTIVE)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(cus)

    # Wait for all compute units to reach a terminal state (DONE or FAILED).
    umgr.wait_units()

    pmgr.cancel_pilots ()

    for unit in units:
        print "* Unit %s state: %s, exit code: %s, started: %s, finished: %s" \
            % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time)

    # Close automatically cancels the pilot(s).
    session.close(delete=False)


#!/usr/bin/env python

import sys
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

  # print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state in [rp.FAILED]:
        sys.exit (1)


# ------------------------------------------------------------------------------
#
def unit_state_change_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if state == rp.FAILED:
      # print " '%s' stderr: %s." % (unit.uid, unit.stderr)
      # print " '%s' stdout: %s." % (unit.uid, unit.stdout)
        sys.exit (1)


# ------------------------------------------------------------------------------
#
# Create a new session. A session is the 'root' object for all other
# RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
# well as security contexts.

def test_globus_online():

    # FIXME: test disabled
    return

    session = rp.Session()

    try:

        # Add an ssh identity to the session.
        c = rp.Context('ssh')
      # c.user_id = 'merzky'
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a X-core that runs for N minutes.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "nersc.edison"
      # pdesc.access   = "go"
        pdesc.runtime  = 10  # N minutes
        pdesc.cores    = 24  # X cores
        pdesc.queue    = "debug"

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        cud_list = list()

        pilot_globe = {
            'source': 'go://marksant#netbook/Users/mark/proj/radical.pilot/' \
                      'examples/helloworld_mpi.py',
            'target': 'go://nersc#edison/scratch2/scratchdirs/marksant/go/',
          # 'target': 'staging:///go/',
            'action':  rp.TRANSFER
        }
        pilot.stage_in(pilot_globe)

        unit_globe = {
            'source': '/scratch2/scratchdirs/marksant/go/helloworld_mpi.py',
          # 'source': 'staging:///go/helloworld_mpi.py',
            'action':  rp.LINK,
        }

        for unit_count in range(0, 1):

            cud = rp.ComputeUnitDescription()

            cud.pre_exec      = ["module load python",
                                  "module load mpi4py"]
            cud.input_staging = [unit_globe]
            cud.executable    = "python-mpi"
            cud.arguments     = ["helloworld_mpi.py"]
            cud.mpi           = True
            cud.cores         = 24

            cud_list.append(cud)


        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(session, scheduler=rp.SCHEDULER_DIRECT)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_change_cb)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cud_list)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        for unit in units:
          # print "* Task %s - state: %s, exit code: %s, started: %s, " \
          #       "finished: %s, stdout: %s" \
          #     % (unit.uid, unit.state, unit.exit_code, unit.start_time,
          #        unit.stop_time, unit.stdout)
            assert (unit.state == rp.DONE)

    finally:
        # Remove session from database
        session.close()


# ------------------------------------------------------------------------------


import os
import sys
import radical.pilot as rp

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = rp.Session()

        # Add an ssh identity to the session.
        c = rp.Context('ssh')
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Get all configs,
        res = pmgr.list_resource_configs()
        # ... and the entry specific for stampede
        s = res['stampede.tacc.utexas.edu']
        print 'Default queue of stampede is: "%s".' % s['default_queue']

        # Build a new one based on Stampede's
        rc = rp.ResourceConfig(s)
        #rc.name = 'testing'

        # And set the queue to development to get a faster turnaround
        rc.default_queue = 'development'

        # Now add the entry back to the PM
        pmgr.add_resource_config(rc)

        # Get all configs,
        res = pmgr.list_resource_configs()
        # ... and the entry specific for stampede
        s = res['stampede.tacc.utexas.edu']
        #s = res['testing']
        print 'Default queue of stampede after change is: "%s".' % s['default_queue']

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 32-core on stamped that runs for 15 mintutes and 
        # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
        pdesc = rp.ComputePilotDescription()
        pdesc.resource  = "stampede.tacc.utexas.edu"
        #pdesc.resource  = "testing"
        pdesc.runtime   = 15 # minutes
        pdesc.cores     = 16
        pdesc.cleanup   = True

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        compute_units = []

        for unit_count in range(0, 1):
            cu = rp.ComputeUnitDescription()
            cu.executable  = "/bin/date"
            cu.cores       = 1

            compute_units.append(cu)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_change_cb)

        # Add the previsouly created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_units)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        if not isinstance(units, list):
            units = [units]
        for unit in units:
            print "* Task %s (executed @ %s) state: %s, exit code: %s, started: %s, finished: %s, output: %s" \
                % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time,
                   unit.stdout)

        # Close automatically cancels the pilot(s).
        session.close(cleanup=False)
        sys.exit(0)

    except rp.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)


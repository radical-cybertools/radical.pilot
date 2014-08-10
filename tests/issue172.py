import os
import sys
import radical.pilot

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security contexts.
        session = radical.pilot.Session()

        # Add an ssh identity to the session.
        c = radical.pilot.Context('ssh')
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a X-core on stamped that runs for N minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
        pdesc = radical.pilot.ComputePilotDescription()
        #pdesc.resource         = "india.futuregrid.org"
        pdesc.resource         = "stampede.tacc.utexas.edu"
        pdesc.runtime          = 15 # N minutes
        pdesc.cores            = 16 # X cores
        pdesc.cleanup          = False

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        bar_variants = ['UNDEFINED', # Special case: env will not be set
                        None, # None
                        {}, # empty dict
                        {'foo': 'bar'}, # single entry dict
                        {'foo': 'bar', 'sports': 'bar', 'banana': 'bar'} # multi entry dict
                       ]
        cud_list = []

        for bar in bar_variants:
            # Serial
            cud = radical.pilot.ComputeUnitDescription()

            cud.executable  = "/bin/echo"
            if bar != 'UNDEFINED':
                cud.environment = bar
            cud.arguments   = ['Taverns:', '$foo', '$sports', '$banana', 'dollars\$\$', '"$dollar"', 'sitting \'all\' by myself', 'drinking "cheap" beer']
            cud.cores       = 1

            cud_list.append(cud)

            # MPI
            cud = radical.pilot.ComputeUnitDescription()

            # india
            # cud.pre_exec = ["module load openmpi/1.4.3-intel python",
            #                 "(test -d $HOME/mpive || virtualenv $HOME/mpive)",
            #                 "source $HOME/mpive/bin/activate",
            #                 "(pip freeze | grep -q mpi4py || pip install mpi4py)"
            # ]

            # stampede
            cud.pre_exec    = ["module load python intel mvapich2 mpi4py"]

            cud.executable  = "python"
            cud.input_data  = ["./mpi4py_env.py"]
            if bar != 'UNDEFINED':
                cud.environment = bar
            cud.arguments   = 'mpi4py_env.py'
            #cud.cores       = 8
            cud.cores       = 8
            cud.mpi = True

            cud_list.append(cud)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

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

        if not isinstance(units, list):
            units = [units]
        for unit in units:
            print "* Task %s - env: %s state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
                % (unit.uid, unit.description.environment, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)

        session.close(delete=False)
        sys.exit(0)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)


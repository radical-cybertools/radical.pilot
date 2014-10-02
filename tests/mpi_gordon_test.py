
import sys
import radical.pilot as rp

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state in [rp.FAILED, rp.DONE] :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        print "                         '%s' stderr: %s." % (unit.uid, unit.stderr)
        print "                         '%s' stdout: %s." % (unit.uid, unit.stdout)
        sys.exit (1)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
    # well as security contexts.
    session = rp.Session()

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    c.user_id = 'amerzky'
    session.add_context(c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a X-core that runs for N minutes on Gordon.
    # Gordon has 16 cores per node.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "xsede.gordon"
    pdesc.runtime  = 5 # N minutes
    pdesc.cores    = 32 # X cores

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    cud_list = []
    for unit_count in range(0, 4):

        mpi_test_task = rp.ComputeUnitDescription()

        mpi_test_task.pre_exec      = ["module load mvapich2_ib python",
                                       "source ~marksant/cuve/bin/activate"
                                      ]
        mpi_test_task.input_staging = ["helloworld_mpi.py"]
        mpi_test_task.executable    = "~marksant/cuve/bin/python"
        mpi_test_task.arguments     = ["helloworld_mpi.py"]
        mpi_test_task.mpi           = True
        mpi_test_task.cores         = 8

        cud_list.append(mpi_test_task)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_BACKFILLING)

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
        print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
            % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)
        
        assert (unit.state == rp.DONE)
        assert ('mpi rank 0/8' in unit.stdout)
        assert ('mpi rank 1/8' in unit.stdout)
        assert ('mpi rank 2/8' in unit.stdout)
        assert ('mpi rank 3/8' in unit.stdout)
        assert ('mpi rank 4/8' in unit.stdout)
        assert ('mpi rank 5/8' in unit.stdout)
        assert ('mpi rank 6/8' in unit.stdout)
        assert ('mpi rank 7/8' in unit.stdout)

    session.close()

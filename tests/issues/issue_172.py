#!/usr/bin/env python


import os
import sys
import radical.pilot as rp


# ##############################################################################
# #172: multinode mpirun environment passing
# ##############################################################################

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
# Create a new session. A session is the 'root' object for all other
# RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
# well as security contexts.
session = rp.Session()

try:

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    c.user_id = 'merzky'
    session.add_context(c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a X-core on stamped that runs for N minutes and
    pdesc = rp.ComputePilotDescription()
    pdesc.resource  = "futuregrid.india"
    pdesc.runtime   = 15 # N minutes
    pdesc.cores     = 4  # X cores
    pdesc.cleanup   = False

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_change_cb)

    # Add the previously created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)

    # generate some units which use env vars in different ways, w/ and w/o MPI
    env_variants = ['UNDEFINED', # Special case: env will not be set
                    None, # None
                    {}, # empty dict
                    {'foo': 'bar'}, # single entry dict
                    {'foo': 'bar', 'sports': 'bar', 'banana': 'bar'} # multi entry dict
                   ]
    cud_list = []

    for env in env_variants:

        # Serial
        cud = rp.ComputeUnitDescription()
        cud.executable  = "/bin/echo"
        cud.arguments   = ['Taverns:', '$foo', '$sports', 
                           '$banana', 'dollars\$\$', '"$dollar"', 
                           'sitting \'all\' by myself', 
                           'drinking "cheap" beer']
        if env != 'UNDEFINED':
            cud.environment = env

        cud_list.append(cud)


        # MPI
        cud = rp.ComputeUnitDescription()
        cud.pre_exec        = ["module load openmpi/1.4.3-intel python",
                               "virtualenv ./mpive",
                               ". ./mpive/bin/activate",
                               "pip install mpi4py"]
        cud.executable      = "python"
        cud.input_staging   = ["mpi4py_env.py"]
        cud.arguments       = './mpi4py_env.py'
        cud.cores           = 2
        cud.mpi             = True
        if  env != 'UNDEFINED' :
            cud.environment = env

        cud_list.append(cud)


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
            % (unit.uid, unit.description.environment, unit.state, \
               unit.exit_code, unit.start_time, unit.stop_time, repr(unit.stdout))

        assert (unit.state == rp.DONE)
        # FIXME: add some more asserts

    pmgr.cancel_pilots()
    pmgr.wait_pilots()

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()


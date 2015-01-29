import os
import sys
import radical.pilot

INPUT_FILE = 'input_file.txt'
INTERMEDIATE_FILE = 'intermediate_file.txt'
OUTPUT_FILE = 'output_file.txt'

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    if not pilot :
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == radical.pilot.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: unit %s on %s : %s." % (unit.uid, unit.pilot_id, state)

    if not unit :
        return

    if state in [radical.pilot.FAILED, radical.pilot.DONE, radical.pilot.CANCELED] :

        print "* unit %s (%s) state %s (%s) %s - %s, out/err: %s / %s" \
                 % (unit.uid, 
                    unit.execution_locations, 
                    unit.state, 
                    unit.exit_code, 
                    unit.start_time, 
                    unit.stop_time, 
                    unit.stdout,
                    unit.stderr)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create input file
        radical_cockpit_occupants = ['Carol', 'Eve', 'Alice', 'Bob']
        for occ in radical_cockpit_occupants:
            os.system('/bin/echo "%s" >> %s' % (occ, INPUT_FILE))

        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security credentials.
        session = radical.pilot.Session()

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session)
        pmgr.register_callback(pilot_state_cb)

        # Define a C-core on stamped that runs for M minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime = 15 # M minutes
        pdesc.cores = 2 # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)
        umgr.register_callback(unit_state_cb, radical.pilot.UNIT_STATE)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Configure the staging directive for intermediate data
        sd_inter_out = {
            'source': INTERMEDIATE_FILE,
            # Note the triple slash, because of URL peculiarities
            'target': 'staging:///%s' % INTERMEDIATE_FILE,
            'action': radical.pilot.COPY
        }

        # Task 1: Sort the input file and output to intermediate file
        cud1 = radical.pilot.ComputeUnitDescription()
        cud1.executable = 'sort'
        cud1.arguments = ['-o', INTERMEDIATE_FILE, INPUT_FILE]
        cud1.input_staging = INPUT_FILE
        cud1.output_staging = sd_inter_out

        # Submit the first task for execution.
        umgr.submit_units(cud1)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        # Configure the staging directive for input intermediate data
        sd_inter_in = {
            # Note the triple slash, because of URL peculiarities
            'source': 'staging:///%s' % INTERMEDIATE_FILE,
            'target': INTERMEDIATE_FILE,
            'action': radical.pilot.LINK
        }

        # Task 2: Take the first line of the sort intermediate file and write to output
        cud2 = radical.pilot.ComputeUnitDescription()
        cud2.executable = '/bin/bash'
        cud2.arguments = ['-c', 'head -n1 %s > %s' %
                          (INTERMEDIATE_FILE, OUTPUT_FILE)]
        cud2.input_staging = sd_inter_in
        cud2.output_staging = OUTPUT_FILE

        # Submit the second CU for execution.
        umgr.submit_units(cud2)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

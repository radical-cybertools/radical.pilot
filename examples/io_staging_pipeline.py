import os
import sys
import radical.pilot
import saga

#INDIA_STAGING = '/N/u/marksant/staging_area'
#RESOURCE = 'india.futuregrid.org'
RESOURCE = 'localhost'

INPUT_FILE = 'input_file.txt'
INTERMEDIATE_FILE = 'intermediate_file.txt'
OUTPUT_FILE = 'output_file.txt'

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security credentials.
        session = radical.pilot.Session()

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session)

        # Define a C-core on stamped that runs for M minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = RESOURCE
        pdesc.runtime = 15 # M minutes
        pdesc.cores = 2 # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define and create staging directory for the intermediate data on the remote machine
        remote_dir_url = saga.Url(os.path.join(pilot.sandbox, 'staging_area'))
        remote_dir = saga.filesystem.Directory(remote_dir_url,
                                               flags=saga.filesystem.CREATE_PARENTS)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Configure the staging directive for input file from local directory to working directory
        sd_input = INPUT_FILE

        # Configure the staging directive for intermediate data
        sd_inter_out = {
            'source': INTERMEDIATE_FILE,
            'target': 'staging:///%s' % INTERMEDIATE_FILE,
            'action': radical.pilot.COPY
        }

        # Task 1: Sort the input file and output to intermediate file
        cud1 = radical.pilot.ComputeUnitDescription()
        cud1.executable = 'sort'
        cud1.arguments = ['-o', INTERMEDIATE_FILE, INPUT_FILE]
        cud1.input_staging = sd_input
        cud1.output_staging = sd_inter_out

        # Submit the first task for execution.
        umgr.submit_units(cud1)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        # Configure the staging directive for input intermediate data
        sd_inter_in = {
            'source': 'staging:///%s' % INTERMEDIATE_FILE,
            'target': INTERMEDIATE_FILE,
            'action': radical.pilot.LINK
        }

        # Configure the staging directive for output data
        sd_output = OUTPUT_FILE

        # Task 2: Take the first line of the sort intermediate file and write to output
        cud2 = radical.pilot.ComputeUnitDescription()
        cud2.executable = '/bin/bash'
        cud2.arguments = ['-l', '-c', 'head -n1 %s > %s' %
                          (INTERMEDIATE_FILE, OUTPUT_FILE)]
        cud2.input_staging = sd_inter_in
        cud2.output_staging = sd_output

        # Submit the second CU for execution.
        umgr.submit_units(cud2)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

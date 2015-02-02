import os
import radical.pilot
import copy

SHARED_INPUT_FILE = 'shared_input_file.txt'
MY_STAGING_AREA = 'staging:///'

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:

        # Create shared input file
        os.system('/bin/echo -n "Hello world, " > %s' % SHARED_INPUT_FILE)
        radical_cockpit_occupants = ['Alice', 'Bob', 'Carol', 'Eve']

        # Create per unit input files
        for idx, occ in enumerate(radical_cockpit_occupants):
            input_file = 'input_file-%d.txt' % (idx+1)
            os.system('/bin/echo "%s" > %s' % (occ, input_file))

        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security credentials.
        session = radical.pilot.Session()

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Define a C-core on $RESOURCE that runs for M minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime  = 5 # M minutes
        pdesc.cores    = 2 # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE)

        staged_file = "%s%s" % (MY_STAGING_AREA, SHARED_INPUT_FILE)
        print "##########################"
        print staged_file
        print "##########################"

        # Configure the staging directive for to insert the shared file into
        # the pilot staging directory.
        sd_pilot = {'source': shared_input_file_url,
                    'target': staged_file,
                    'action': radical.pilot.TRANSFER
        }
        # Synchronously stage the data to the pilot
        pilot.stage_in(sd_pilot)

        # Configure the staging directive for shared input file.
        sd_shared = {'source': staged_file, 
                     'target': SHARED_INPUT_FILE,
                     'action': radical.pilot.LINK
        }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(session, radical.pilot.SCHED_BACKFILLING)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        compute_unit_descs = []

        for unit_idx in range(len(radical_cockpit_occupants)):

            # Configure the per unit input file.
            input_file = 'input_file-%d.txt' % (unit_idx+1)

            # Configure the for per unit output file.
            output_file = 'output_file-%d.txt' % (unit_idx+1)

            # Actual task description.
            # Concatenate the shared input and the task specific input.
            cud = radical.pilot.ComputeUnitDescription()
            cud.executable = '/bin/bash'
            cud.arguments = ['-c', 'sleep 10 && cat %s %s > %s' %
                             (SHARED_INPUT_FILE, input_file, output_file)]
            cud.cores = 1
            cud.input_staging = [sd_shared, input_file]
            cud.output_staging = output_file

            compute_unit_descs.append(cud)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_unit_descs)

        # Wait for all compute units to finish.
        umgr.wait_units()

        for unit in umgr.get_units():

            # Get the stdout and stderr streams of the ComputeUnit.
            print " STDOUT: %s" % unit.stdout
            print " STDERR: %s" % unit.stderr

        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

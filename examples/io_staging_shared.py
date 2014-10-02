import os
import radical.pilot

SHARED_INPUT_FILE = 'shared_input_file.txt'
MY_STAGING_AREA = '/tmp/my_staging_area'

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
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
        pdesc.cores    = 8 # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE)

        # Configure the staging directive for to insert the shared file into
        # the pilot staging directory.
        sd_pilot = {'source': shared_input_file_url,
                    'target': os.path.join(MY_STAGING_AREA, SHARED_INPUT_FILE),
                    'action': radical.pilot.TRANSFER
        }
        # Synchronously stage the data to the pilot
        pilot.stage_in(sd_pilot)

        # Configure the staging directive for shared input file.
        sd_shared = {'source': os.path.join(MY_STAGING_AREA, SHARED_INPUT_FILE),
                     'target': SHARED_INPUT_FILE,
                     'action': radical.pilot.LINK
        }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(session, radical.pilot.SCHED_BACKFILLING)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        compute_unit_descs = []

        for unit_count in range(4):

            # Configure the staging directive for per unit input file.
            sd_input = 'input_file-%d.txt' % unit_count

            # Configure the staging directive for per unit output file.
            sd_output = 'output_file-%d.txt' % unit_count

            # Actual task description.
            # Concatenate the shared input and the task specific input.
            cud = radical.pilot.ComputeUnitDescription()
            cud.executable = '/bin/bash'
            cud.arguments = ['-l', '-c',
                             'cat %s input_file-%d.txt > output_file-%d.txt' %
                             (SHARED_INPUT_FILE, unit_count, unit_count)]
            cud.cores = 1
            cud.input_staging = [sd_shared, sd_input]
            cud.output_staging = sd_output

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

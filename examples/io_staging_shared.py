import os
import sys
import radical.pilot
import saga

#RESOURCE = 'sierra.futuregrid.org'
RESOURCE = 'localhost'

SHARED_INPUT_FILE = 'shared_input_file.txt'

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
        pdesc.resource = RESOURCE
        pdesc.runtime = 5 # M minutes
        pdesc.cores = 8 # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define the url of the local file in the local directory
        shared_input_file_url = saga.Url('file://%s/%s' %
                                         (os.getcwd(), SHARED_INPUT_FILE))
        # Define and open staging directory on the remote machine
        remote_dir_url = saga.Url(os.path.join(pilot.sandbox, 'staging_area'))
        remote_dir = saga.filesystem.Directory(remote_dir_url,
                                               flags=saga.filesystem.CREATE_PARENTS)
        # Copy the local file to the remote staging area
        remote_dir.copy(shared_input_file_url, '.')

        # TODO: Change to above block to pilot.stage_in(shared_input_file_url)
        #       once that is available.

        # Configure the staging directive for shared input file.
        sd_shared = {'source': 'staging:///%s' % SHARED_INPUT_FILE,
                     # Note the triple slash, because of SAGA URL peculiarities
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
            cud.arguments = ['-l', '-c', 'cat shared_input_file.txt '
                             'input_file-%d.txt > output_file-%d.txt' %
                             (unit_count, unit_count)]
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

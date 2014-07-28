import os
import sys
import radical.pilot
import saga

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

REMOTE_STAGING = '///N/u/marksant/staging_area/'
REMOTE_HOST = 'india.futuregrid.org'

# REMOTE_STAGING = '///tmp/marksant/staging_area/'
# REMOTE_HOST = 'localhost'

SHARED_INPUT_FILE = 'shared_input_file.txt'

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Define a 32-core on stamped that runs for 15 mintutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directoy.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = REMOTE_HOST
        pdesc.runtime = 15 # minutes
        pdesc.cores = 8

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define and open staging directory on the remote machine
        remote_dir_url = saga.Url()
        remote_dir_url.scheme = 'sftp'
        remote_dir_url.host = REMOTE_HOST
        remote_dir_url.path = REMOTE_STAGING
        remote_dir = saga.filesystem.Directory(remote_dir_url)

        # Define the url of the local file in the local directory
        shared_input_file_url = saga.Url('file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE))
        # Upload the local file to the remote staging area
        remote_dir.copy(shared_input_file_url, '.')

        # Configure the staging directive for shared input file.
        sd_shared = radical.pilot.StagingDirectives()
        sd_shared.source = os.path.join(REMOTE_STAGING, SHARED_INPUT_FILE)
        sd_shared.target = SHARED_INPUT_FILE
        sd_shared.action = radical.pilot.LINK

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        compute_units = []

        for unit_count in range(4):

            # Configure the staging directive for input file.
            sd_input = radical.pilot.StagingDirectives()
            sd_input.source = 'input_file-%d.txt' % unit_count
            sd_input.target = 'input_file-%d.txt' % unit_count
            sd_input.action = radical.pilot.TRANSFER

            # Configure the staging directive for output file.
            sd_output = radical.pilot.StagingDirectives()
            sd_output.source = 'output_file-%d.txt' % unit_count
            sd_output.target = 'output_file-%d.txt' % unit_count
            sd_output.action = radical.pilot.TRANSFER

            # Actual task description.
            # Concatenate the shared input and the task specific input.
            cu = radical.pilot.ComputeUnitDescription()
            cu.executable = '/bin/bash'
            cu.arguments = ['-l', '-c', 'cat shared_input_file.txt input_file-%d.txt > output_file-%d.txt' % (unit_count, unit_count)]
            cu.cores = 1
            cu.input_staging = [sd_shared, sd_input]
            cu.output_staging = sd_output

            compute_units.append(cu)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_units)

        # Wait for all compute units to finish.
        umgr.wait_units()

        for unit in umgr.get_units():
            # Print some information about the unit.
            #print "\n{0}".format(str(unit))

            # Get the stdout and stderr streams of the ComputeUnit.
            print " STDOUT: {0}".format(unit.stdout)
            print " STDERR: {0}".format(unit.stderr)

        session.close(delete=False)

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

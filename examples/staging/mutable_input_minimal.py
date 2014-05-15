import os
import sys
import radical.pilot
import saga

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICALPILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICALPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

# RCONF points to the resource configuration files. Read more about resource
# configuration files at http://saga-pilot.readthedocs.org/en/latest/machconf.html
RCONF = ["https://raw.github.com/radical-cybertools/radical.pilot/master/configs/xsede.json",
         "https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json"]
#RCONF="file://localhost/Users/mark/proj/radical.pilot/configs/futuregrid.json"

REMOTE_STAGING = '///N/u/marksant/staging_area/'
REMOTE_HOST = 'india.futuregrid.org'

# REMOTE_STAGING = '///tmp/marksant/staging_area/'
# REMOTE_HOST = 'localhost'

MUTABLE_INPUT_FILE = 'mutable_input_file.txt'

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session, resource_configurations=RCONF)

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
        mutable_input_file_url = saga.Url('file://%s/%s' % (os.getcwd(), MUTABLE_INPUT_FILE))
        # Upload the local file to the remote staging area
        remote_dir.copy(mutable_input_file_url, '.')

        # Configure the move staging directive for mutable input file.
        sd_input = radical.pilot.StagingDirectives()
        sd_input.source = os.path.join(REMOTE_STAGING, MUTABLE_INPUT_FILE)
        sd_input.target = MUTABLE_INPUT_FILE
        sd_input.action = radical.pilot.MOVE

        # Configure the move staging directive for muted output file.
        sd_output = radical.pilot.StagingDirectives()
        sd_output.source = MUTABLE_INPUT_FILE
        sd_output.target = os.path.join(REMOTE_STAGING, MUTABLE_INPUT_FILE)
        sd_output.action = radical.pilot.MOVE

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Actual task description.
        # Concatenate the shared input and the task specific input.
        cud = radical.pilot.ComputeUnitDescription()
        cud.executable = '/bin/sort'
        cud.arguments = ('%s -o %s' % (MUTABLE_INPUT_FILE, MUTABLE_INPUT_FILE)).split()
        cud.cores = 1
        cud.input_staging = sd_input
        cud.output_staging = sd_output

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cud)

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

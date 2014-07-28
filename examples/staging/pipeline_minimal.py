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

INDIA_STAGING = '/N/u/marksant/staging_area'
INDIA_HOST = 'india.futuregrid.org'

INPUT_FILE = 'input_file.txt'
INTERMEDIATE_FILE = 'intermediate_file.txt'
OUTPUT_FILE = 'output_file.txt'

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
        pdesc.resource = "india.futuregrid.org"
        pdesc.runtime = 15 # minutes
        pdesc.cores = 8

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define and create staging directory for the intermediate data on the remote machine
        remote_dir_url = saga.Url()
        remote_dir_url.scheme = 'sftp'
        remote_dir_url.host = INDIA_HOST
        remote_dir_url.path = INDIA_STAGING
        remote_dir = saga.filesystem.Directory(remote_dir_url, saga.filesystem.CREATE)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previsouly created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Configure the staging directive for input file from local directory to working directory
        sd_input = radical.pilot.StagingDirectives()
        sd_input.source = INPUT_FILE
        sd_input.target = INPUT_FILE # Could be left empty if filename is same
        sd_input.action = radical.pilot.TRANSFER

        # Configure the staging directive for intermediate data
        sd_inter_out = radical.pilot.StagingDirectives()
        sd_inter_out.source = INTERMEDIATE_FILE
        sd_inter_out.target = '%s/%s' % (INDIA_STAGING, INTERMEDIATE_FILE)
        sd_inter_out.action = radical.pilot.COPY

        # Task 1: Sort the input file and output to intermediate file
        cud1 = radical.pilot.ComputeUnitDescription()
        cud1.executable = '/bin/sort'
        cud1.arguments = ('%s > %s' % (INPUT_FILE, INTERMEDIATE_FILE)).split()
        cud1.input_staging = sd_input
        cud1.output_staging = sd_inter_out
        cud1.cores = 1

        # Submit the first task for execution.
        umgr.submit_units(cud1)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        # Configure the staging directive for input intermediate data
        sd_inter_in = radical.pilot.StagingDirectives()
        sd_inter_in.source = '%s/%s' % (INDIA_STAGING, INTERMEDIATE_FILE)
        sd_inter_in.target = INTERMEDIATE_FILE
        sd_inter_in.action = radical.pilot.LINK

        # Configure the staging directive for output data
        sd_output = radical.pilot.StagingDirectives()
        sd_output.source = OUTPUT_FILE
        sd_output.target = OUTPUT_FILE # Could be left out if same as source
        sd_output.action = radical.pilot.TRANSFER

        # Task 2: Take the first line of the sort intermediate file and write to output
        cud2 = radical.pilot.ComputeUnitDescription()
        cud2.executable = '/usr/bin/head'
        cud2.arguments = ('-n1 %s > %s' % (INTERMEDIATE_FILE, OUTPUT_FILE)).split()
        cud2.input_staging = sd_inter_in
        cud2.output_staging = sd_output
        cud2.cores = 1

        # Submit the second CU for execution.
        umgr.submit_units(cud2)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        session.close(delete=False)

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

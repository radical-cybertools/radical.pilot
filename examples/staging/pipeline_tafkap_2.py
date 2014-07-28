import os
import sys
import radical.pilot
import saga
from tafkap_2 import TAFKAP_FileContainer, TAFKAP_StorageResource


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

REMOTE_STAGING = '/N/u/marksant/staging_area'
REMOTE_HOST = 'india.futuregrid.org'

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

        # Define and open staging directory on the remote machine
        staging_area_url = saga.Url('sftp://%s%s' % (REMOTE_HOST, REMOTE_STAGING))
        staging_area_tsr = TAFKAP_StorageResource(staging_area_url)

        # Define the url of the local file in the local directory
        input_file_url = saga.Url('file://localhost%s' % os.path.join(os.getcwd(), INPUT_FILE))
        # Add the local file to the remote staging area
        input_file_tfc = staging_area_tsr.create_tfc(input_file_url)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previsouly created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Configure the TFC for intermediate data
        intermediate_file_url = saga.Url(INTERMEDIATE_FILE)
        # Add the local file to the remote staging area
        intermediate_file_tfc = TAFKAP_FileContainer(intermediate_file_url)

        # Task 1: Sort the input file and output to intermediate file
        cud1 = radical.pilot.ComputeUnitDescription()
        cud1.executable = '/bin/sort'
        cud1.arguments = ('%s > %s' % (INPUT_FILE, INTERMEDIATE_FILE)).split()
        cud1.input_tfc = input_file_tfc
        cud1.output_tfc = intermediate_file_tfc
        cud1.cores = 1

        # Submit the first task for execution.
        umgr.submit_units(cud1)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        # Configure the TFC for output data
        output_file_url = saga.Url(OUTPUT_FILE)
        # Add the local file to the remote staging area
        output_file_tfc = TAFKAP_FileContainer(output_file_url)

        # Task 2: Take the first line of the sort intermediate file and write to output
        cud2 = radical.pilot.ComputeUnitDescription()
        cud2.executable = '/usr/bin/head'
        cud2.arguments = ('-n1 %s > %s' % (INTERMEDIATE_FILE, OUTPUT_FILE)).split()
        cud2.input_tfc = intermediate_file_tfc
        cud2.output_tfc = output_file_tfc
        cud2.cores = 1

        # Submit the second CU for execution.
        umgr.submit_units(cud2)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        session.close(delete=False)

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

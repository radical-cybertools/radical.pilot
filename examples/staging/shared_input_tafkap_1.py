import os
import sys
import radical.pilot
import saga

from tafkap_1 import StorageResource, StorageResourceDescription, DataUnit, DataUnitDescription

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

REMOTE_STAGING = '/N/u/marksant/staging_area/'
REMOTE_HOST = 'india.futuregrid.org'

# REMOTE_STAGING = '///tmp/marksant/staging_area/'
# REMOTE_HOST = 'localhost'

BAG_SIZE = 4

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security credentials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session, resource_configurations=RCONF)

        # Define a N-core pilot on $REMOTE_HOST that runs for M minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = REMOTE_HOST
        pdesc.cores = 8 # N
        pdesc.runtime = 15 # M

        # Submit the pilot to the remote resource.
        pilot = pmgr.submit_pilots(pdesc)

        # Define and open staging directory on the remote machine
        staging_area_sr_desc = StorageResourceDescription(url='sftp://%s%s' % (REMOTE_HOST, REMOTE_STAGING))
        staging_area_sr = StorageResource(staging_area_sr_desc)

        # Define the url of the local file in the local directory
        shared_input_file_name = 'shared_input_file.txt'
        shared_input_file_desc = DataUnitDescription(url='file://localhost%s' % os.path.join(os.getcwd(), shared_input_file_name))
        # Create DU by inserting local file to the remote staging area.
        shared_input_file_du = staging_area_sr.insert(shared_input_file_desc)
        # Wait until the transfer is completed
        shared_input_file_du.wait()

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(session=session, scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)
        # Add the previously created Storage Resource to the UnitManager
        umgr.add_storage(staging_area_sr)

        compute_unit_descs = []
        output_dus = []

        for task_no in range(BAG_SIZE):

            # Per task file names for output and non-shared input
            input_file_name = 'input_file-%d.txt' % task_no
            output_file_name = 'output_file-%d.txt' % task_no

            # Define the url of the local non-shared input file in the local directory
            input_file_url = 'file://%s' % (os.path.join(os.getcwd(), input_file_name))
            # Add the local non-shared input file to the remote staging area
            input_file_du = staging_area_sr.insert(input_file_url)

            # Create a DU based on the output file name for this task.
            output_file_du = DataUnit(DataUnitDescription(url=output_file_name))
            # Allocate the DU on the Staging Area
            staging_area_sr.allocate(output_file_du)

            # Actual task description:
            # Concatenate the shared input and the task specific input and write it to an output file.
            cud = radical.pilot.ComputeUnitDescription()
            cud.executable = '/bin/sh'
            cud.arguments = ['-c', 'cat %s %s > %s' % (shared_input_file_name, input_file_name, output_file_name)]
            cud.input_data = [input_file_du, shared_input_file_du]
            cud.output_data = output_file_du

            compute_unit_descs.append(cud)
            output_dus.append(output_file_du)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_unit_descs)

        # Wait for all compute units to finish.
        umgr.wait_units()

        # Export (download) the content of the DUs to the local directory.
        for du in output_dus:
            du.export_all('file://%s' % os.getcwd)

        session.close(delete=False)

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

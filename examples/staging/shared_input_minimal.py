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

RESOURCE = 'india.futuregrid.org'

SHARED_INPUT_FILE = 'shared_input_file.txt'

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security credentials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Define a N-core on $RESOURCE that runs for M minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = RESOURCE
        pdesc.runtime = 5 # M minutes
        pdesc.cores = 8 # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define the url of the local file in the local directory
        shared_input_file_url = saga.Url('file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE))
        # Define and open staging directory on the remote machine
        remote_dir_url = saga.Url(os.path.join(pilot.sandbox, 'staging_area'))
        remote_dir = saga.filesystem.Directory(remote_dir_url, flags=saga.filesystem.CREATE_PARENTS)
        # Copy the local file to the remote staging area
        remote_dir.copy(shared_input_file_url, '.') # Change to pilot.stage_in(shared_input_file_url)

        # Configure the staging directive for shared input file.
        sd_shared = {'source': 'staging:///%s' % SHARED_INPUT_FILE, # Note the triple slash
                     'target': SHARED_INPUT_FILE,
                     'action': radical.pilot.staging_directives.LINK
        }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session, scheduler=radical.pilot.SCHED_LATE_BINDING)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        compute_unit_descs = []

        for unit_count in range(4):

            # Configure the staging directive for input file.
            sd_input = {'source': 'input_file-%d.txt' % unit_count,
                        'target': 'input_file-%d.txt' % unit_count,
                        'action':  radical.pilot.staging_directives.TRANSFER
            }
            # Above sd_input is semantically identical to sd_input below
            sd_input = 'input_file-%d.txt' % unit_count

            # Configure the staging directive for output file.
            sd_output = {'source': 'output_file-%d.txt' % unit_count,
                         'target': 'output_file-%d.txt' % unit_count,
                         'action': radical.pilot.staging_directives.TRANSFER
            }

            # Actual task description.
            # Concatenate the shared input and the task specific input.
            cud = radical.pilot.ComputeUnitDescription()
            cud.executable = '/bin/bash'
            cud.arguments = ['-l', '-c', 'cat shared_input_file.txt input_file-%d.txt > output_file-%d.txt' % (unit_count, unit_count)]
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
            # Print some information about the unit.
            #print "\n{0}".format(str(unit))

            # Get the stdout and stderr streams of the ComputeUnit.
            print " STDOUT: {0}".format(unit.stdout)
            print " STDERR: {0}".format(unit.stderr)

        session.close(delete=False)

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

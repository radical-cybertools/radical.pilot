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

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session, scheduler=radical.pilot.SCHED_BACKFILLING)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        compute_unit_descs = []

        for unit_count in range(2):

            # Configure the staging directive for input file.
            sd_inputs = {'source': ['input_file-0.txt',
                                    'input_file-1.txt',
                                    'input_file-2.txt',
                                    'input_file-3.txt'],
                        'target': ['input_file-0.txt',
                                   'input_file-1.txt',
                                   'input_file-2.txt',
                                   'input_file-3.txt'],
                        'action':  radical.pilot.TRANSFER
            }

            # Configure the staging directive for output file.
            sd_output = {'source': 'output_file.txt',
                         'target': 'output_file-%d.txt' % unit_count,
                         'action': radical.pilot.TRANSFER
            }

            # Actual task description.
            # Concatenate the shared input and the task specific input.
            cud = radical.pilot.ComputeUnitDescription()
            cud.executable = '/bin/bash'
            cud.arguments = ['-l', '-c', 'cat input_file*.txt > output_file.txt']
            cud.cores = 1
            cud.input_staging = sd_inputs
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

        session.close(cleanup=False)

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

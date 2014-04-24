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
        pdesc.resource = 'ce1.opensciencegrid.org'
        pdesc.runtime = 15 # minutes
        pdesc.cores = 8

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define and open staging directory on the remote machine
        remote_dir_url = saga.Url()
        remote_dir_url.scheme = 'srm'
        remote_dir_url.host = 'srm.se.opensciencegrid.org'
        remote_dir_url.path = '/srm/vo/osg/marksant/staging_area'
        remote_dir = saga.filesystem.Directory(remote_dir_url)

        # Define the url of the local file in the local directory
        shared_input_file_url = saga.Url('file://%s/shared_input_file.txt' % os.getcwd() )
        # Upload the local shared input file to the remote staging area
        remote_dir.copy(shared_input_file_url, '.')

        # Configure the staging directive for shared input file.
        sd_shared = radical.pilot.StagingDirectives()
        sd_shared.source = 'srm://srm.se.opensciencegrid.org/srm/vo/osg/marksant/staging_area/shared_input_file.txt'
        sd_shared.target = 'shared_input_file.txt' # Could be left empty
        sd_shared.action = radical.pilot.StagingDirectives.TRANSFER

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previsouly created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        compute_units = []

        for unit_count in range(4):

            # Define the url of the local file in the local directory
            input_file_url = saga.Url('file://%s/input_file-%d.txt' % (os.getcwd(), unit_count))
            # Upload the local file to the remote staging area
            remote_dir.copy(input_file_url, '.')

            # Configure the staging directive for input file.
            sd_input = radical.pilot.StagingDirectives()
            sd_input.source = 'srm://srm.se.opensciencegrid.org/srm/vo/osg/marksant/staging_area/input_file-%d.txt' % unit_count
            sd_input.target = 'input_file-%d.txt' % unit_count # Could be left empty
            sd_input.action = radical.pilot.StagingDirectives.TRANSFER

            # Configure the staging directive for output file.
            sd_output = radical.pilot.StagingDirectives()
            sd_output.source = 'output_file-%d.txt' % unit_count
            sd_output.target = 'srm://srm.se.opensciencegrid.org/srm/vo/osg/marksant/staging_area/output_file-%d.txt' % unit_count
            sd_output.action = radical.pilot.StagingDirectives.TRANSFER

            cu = radical.pilot.ComputeUnitDescription()
            cu.executable = '/bin/cat'
            cu.arguments = ('shared_input_file.txt input_file-%d.txt > output_file-%d.txt' % (unit_count, unit_count)).split()
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

        # Now that all tasks have completed, we can retrieve the output files from storage
        local_dir_url = saga.Url('file://%s' % os.getcwd())
        for unit_count in range(4):
            remote_dir.copy('output_file-%d.txt' % unit_count, local_dir_url)

        for unit in umgr.get_units():
            # Print some information about the unit.
            #print "\n{0}".format(str(unit))

            # Get the stdout and stderr streams of the ComputeUnit.
            print " STDOUT: {0}".format(unit.stdout)
            print " STDERR: {0}".format(unit.stderr)

        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

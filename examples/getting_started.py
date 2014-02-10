import sagapilot

# DBURL points to a MongoDB server. For installation of a MongoDB server, please
# refer to the MongoDB website: http://docs.mongodb.org/manual/installation/
DBURL  = "mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017"

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sagapilot.Session(database_url=DBURL)
        print "Session UID      : {0} ".format(session.uid)

        # Add a Pilot Manager 
        pmgr = sagapilot.PilotManager(session=session)
        print "PilotManager UID : {0} ".format( pmgr.uid )

        # Define a 2-core local pilot in /tmp/sagapilot.sandbox that runs 
        # for 10 minutes.
        pdesc = sagapilot.ComputePilotDescription()
        pdesc.resource  = "localhost"
        pdesc.runtime   = 15 # minutes 
        pdesc.cores     = 4 

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)
        print "Pilot UID        : {0} ".format( pilot.uid )

        # Create a workload of 8 ComputeUnits (tasks). Each compute unit
        # uses /bin/cat to concatenate two input files, file1.dat and 
        # file2.dat. The output is written to STDOUT. cu.environment is 
        # used to demonstrate how to set environment variables withih a 
        # ComputeUnit - it's not strictly necessary for this example. As 
        # a shell script, the ComputeUnits would look something like this:
        #
        #    export INPUT1=file1.dat
        #    export INPUT2=file2.dat
        #    /bin/cat $INPUT1 $INPUT2
        #
        compute_units = []

        for unit_count in range(0, 8):
            cu = sagapilot.ComputeUnitDescription()
            cu.environment = {"INPUT1" : "file1.dat", "INPUT2" : "file2.dat"}
            cu.executable  = "/bin/cat"
            cu.arguments   = ["$INPUT1", "$INPUT2"]
            cu.cores       = 1
            cu.input_data  = [ "./file1.dat   > file1.dat",
                               "./file2.dat   > file2.dat" ]    
            cu.output_data = [ "result-%s.dat < STDOUT" % unit_count]
             
            compute_units.append(cu)

        # # Combine the pilot, the workload and a scheduler via 
        # # a UnitManager.
        umgr = sagapilot.UnitManager(session=session, scheduler=sagapilot.SCHED_DIRECT_SUBMISSION)
        print "UnitManager UID  : {0} ".format( umgr.uid )

        # Add the previsouly created ComputePilot to the UnitManager. 
        umgr.add_pilots(pilot)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start 
        # assigning ComputeUnits to the ComputePilots. 
        umgr.submit_units(compute_units)

        # unit_list = um.list_units()
        # print "* Submitted %s compute units: %s" % (len(unit_list), unit_list)

        # # Wait for all compute units to finish.
        # print "* Waiting for all compute units to finish..."
        umgr.wait_units()

        for unit in umgr.get_units():
            print "* UID: {0}, STATE: {1}, START_TIME: {2}, STOP_TIME: {3}, EXEC_LOC: {4}".format(
                unit.uid, unit.state, unit.start_time, unit.stop_time, unit.execution_details)
        
            # Get the stdout and stderr streams of the ComputeUnit.
            print "  STDOUT: {0}".format(unit.stdout)
            print "  STDERR: {0}".format(unit.stderr)

        # Cancel all pilots.
        pmgr.cancel_pilots()

        # Remove session from database
        session.destroy()

    except sagapilot.SagapilotException, ex:
        print "Error: %s" % ex
        


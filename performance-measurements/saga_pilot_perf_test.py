import sys
import sinon
import time
import numpy
import os
import datetime
import matplotlib.pyplot as plt


PWD    = os.path.dirname(os.path.abspath(__file__))

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'

FGCONF = 'file://localhost/%s/../configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------

def pilot_bulk_submit_test_real():

    print "Test: Adding Pilots in bulks"
    try:
        session = sinon.Session(database_url=DBURL)
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)
        um = sinon.UnitManager(session=session, scheduler=sinon.SCHED_ROUND_ROBIN)  

        avg_pm_submit_pilots_times = []
        avg_um_add_pilots_times = []
        avg_um_submit_units_times = []

        err_pm_submit_pilots_times = []
        err_um_add_pilots_times = []
        err_um_submit_units_times = []

        avg_time_to_submission = []
        avg_time_from_sub_to_start = []
        avg_runtime = []

        err_time_to_submission = []
        err_time_from_sub_to_start = []
        err_runtime = []

        avg_pmsp_create_pilot_descr = []
        avg_pmsp_perform_checks = []
        avg_pmsp_saga_bootstraping = []
        avg_pmsp_create_pilot_object = []
        avg_pmsp_create_database_entries = []

        for i in [1,2]:
            
            pm_submit_pilots_times = []
            um_add_pilots_times = []
            um_submit_units_times = []

            time_to_submission = []
            time_from_sub_to_start = []
            runtime = []

            pmsp_create_pilot_descr = []
            pmsp_perform_checks = []
            pmsp_saga_bootstraping = []
            pmsp_create_pilot_object = []
            pmsp_create_database_entries = []

            for j in range(0, 2):
                
                pilots = []
                for k in range(0, i):
                    pd = sinon.ComputePilotDescription()
                    pd.resource = "futuregrid.SIERRA"
                    pd.working_directory = "/N/u/antonstre/sinon-performance"
                    pd.cores = 8
                    pd.run_time = 10
                    pd.cleanup = True
                    pilots.append(pd)

                init_time = datetime.datetime.utcnow()
                init_time_posix = time.mktime(init_time.timetuple())
                

                start_time = time.time()
                pilot_objects = pm.submit_pilots(pilots)
                stop_time = time.time() - start_time
                pm_submit_pilots_times.append(stop_time)

                if i > 1:
                    for pilot in pilot_objects:
                        if pilot.state in [sinon.states.FAILED]:
                            print " * [ERROR] Pilot %s failed: %s." % (pilot, pilot.state_details[-1])
                            # Add some smart fault tolerance mechanism here!
                        #else:
                            #print " * [OK] Pilot %s submitted successfully: %s" % (pilot, pilot.state_details[-1])
                else:
                    if pilot_objects.state in [sinon.states.FAILED]:
                        print " * [ERROR] Pilot %s failed: %s." % (pilot_objects, pilot_objects.state_details[-1])
                        # Add some smart fault tolerance mechanism here!
                    #else:

                # Create a workload of i '/bin/date' compute units
                compute_units = []
                for unit_count in range(0, i):
                    cu = sinon.ComputeUnitDescription()
                    cu.cores = 1
                    cu.executable = "/bin/date"
                    compute_units.append(cu)

                start_time = time.time()
                um.add_pilots(pilot_objects)
                end_time = time.time() - start_time
                um_add_pilots_times.append(end_time)

                start_time = time.time()
                um.submit_units(compute_units)
                end_time = time.time() - start_time
                um_submit_units_times.append(end_time)

                print "* Waiting for all compute units to finish..."
                um.wait_units()

                print "  FINISHED"
                pm.cancel_pilots()
                time.sleep(5)

                if i > 1:
                    # 
                    p_sub_time_posix = 0
                    p_start_time_posix = 0
                    p_stop_time_posix = 0
                    for pilot_object in pilot_objects:
                        print "Time init time is: %s" % str( init_time_posix )

                        p_sub_time = pilot_object._get_submission_time_priv()
                        temp_time = time.mktime(p_sub_time.timetuple())
                        if temp_time > p_sub_time_posix:
                            p_sub_time_posix = temp_time
                        print "Pilot submission_time is: %s" % str( temp_time )

                        p_start_time = pilot_object._get_start_time_priv()
                        temp_time = time.mktime(p_start_time.timetuple())
                        if p_start_time_posix == 0:
                            p_start_time_posix = temp_time
                        else:    
                            if temp_time < p_start_time_posix:
                                p_start_time_posix = temp_time
                        print "Pilot start_time is: %s" % str( temp_time )

                        p_stop_time = pilot_object._get_stop_time_priv()
                        temp_time = time.mktime(p_stop_time.timetuple())
                        if p_stop_time_posix == 0:
                            p_stop_time_posix = temp_time
                        else:
                            if temp_time > p_stop_time_posix:
                                p_stop_time_posix = temp_time
                        print "Pilot stop_time is: %s" % str( temp_time )

                else:
                    print "Time init time is: %s" % str( init_time_posix )

                    p_sub_time = pilot_objects._get_submission_time_priv()
                    p_sub_time_posix = time.mktime(p_sub_time.timetuple())
                    print "Pilot submission_time is: %s" % str( p_sub_time_posix)

                    p_start_time = pilot_objects._get_start_time_priv()
                    p_start_time_posix = time.mktime(p_start_time.timetuple())
                    print "Pilot start_time is: %s" % str( p_start_time_posix )

                    p_stop_time = pilot_objects._get_stop_time_priv()
                    p_stop_time_posix = time.mktime(p_stop_time.timetuple())
                    print "Pilot stop_time is: %s" % str( p_stop_time_posix )

                time_i = p_sub_time_posix - init_time_posix
                time_to_submission.append(time_i)
                time_s = p_start_time_posix - p_sub_time_posix
                time_from_sub_to_start.append(time_s)
                time_r = p_stop_time_posix - p_start_time_posix
                runtime.append(time_r)

                # collect data for pm.submit_pilot() decomposition
                filename = "pm_submit_pilots_times.dat"
                f = open(filename,'r')
                line = f.readline()
                words = line.split()
                f.close()

                for index in range(len(words)):
                    if words[index] == "create.pilot.descriptions":
                        pmsp_create_pilot_descr.append( float(words[index+1]) )
                    elif words[index] == "perform.checks":
                        pmsp_perform_checks.append( float(words[index+1]) )
                    elif words[index] == "saga.bootstraping":
                        pmsp_saga_bootstraping.append( float(words[index+1]) )
                    elif words[index] == "create.pilot.object":
                        pmsp_create_pilot_object.append( float(words[index+1]) )
                    elif words[index] == "bulk.create.database.entries":
                        pmsp_create_database_entries.append( float(words[index+1]) )


            avg_pmsp_create_pilot_descr.append( numpy.mean( pmsp_create_pilot_descr ) )
            avg_pmsp_perform_checks.append( numpy.mean( pmsp_perform_checks ) )
            avg_pmsp_saga_bootstraping.append( numpy.mean( pmsp_saga_bootstraping ) )
            avg_pmsp_create_pilot_object.append( numpy.mean( pmsp_create_pilot_object ) )
            avg_pmsp_create_database_entries.append( numpy.mean( pmsp_create_database_entries ) )

            avg_pm_submit_pilots_times.append( numpy.mean(pm_submit_pilots_times) )
            avg_um_add_pilots_times.append( numpy.mean(um_add_pilots_times) )
            avg_um_submit_units_times.append( numpy.mean(um_submit_units_times) )

            err_pm_submit_pilots_times.append( numpy.ptp(pm_submit_pilots_times) )
            err_um_add_pilots_times.append( numpy.ptp(um_add_pilots_times) )
            err_um_submit_units_times.append( numpy.ptp(um_submit_units_times) )

            avg_time_to_submission.append( numpy.mean(time_to_submission) )
            avg_time_from_sub_to_start.append( numpy.mean(time_from_sub_to_start) )
            avg_runtime.append( numpy.mean(runtime) )

            err_time_to_submission.append( numpy.ptp(time_to_submission) )
            err_time_from_sub_to_start.append( numpy.ptp(time_from_sub_to_start) )
            err_runtime.append( numpy.ptp(runtime) )


            print "Average time to submit %i pilots is: %f sec." % ( i, numpy.mean(time_to_submission) )
            print "Average time from submission to start for %i pilots is: %f sec." % ( i, numpy.mean(time_from_sub_to_start) )
            print "Average runtime to run %i pilots is: %f sec." % ( i, numpy.mean(runtime) )

            print "Average time to call pm.submit_pilots() for bulks of %i pilots is %f sec." % (i, numpy.mean(pm_submit_pilots_times))
            print "Average time to call um.add_pilots() for bulks of %i pilots is %f sec." % (i, numpy.mean(um_add_pilots_times))
            print "Average time to call um.submit_units() for bulks of %i pilots is %f sec." % (i, numpy.mean(um_submit_units_times))
            
    except sinon.SinonException, ex:
        print "Error: %s" % ex

    try:        
        session.destroy()
    except sinon.SinonException, ex:
        print "Error: %s" % ex


    #--------------------------------------------------
    # plot: Distribution of times for Pilot stages
    #--------------------------------------------------

    N = 2

    ind = numpy.arange(N)    # the x locations for the groups
    width = 0.1       # the width of the bars: can also be len(x) sequence 

    sum_for_avg_runtime = []
    for i in range(len(avg_time_to_submission)):
        sum_for_avg_runtime.append(avg_time_to_submission[i] + avg_time_from_sub_to_start[i]) 

    p1 = plt.bar(ind, avg_time_to_submission,   width, color='lightskyblue', yerr=err_time_to_submission, edgecolor = "none")
    p2 = plt.bar(ind, avg_time_from_sub_to_start,   width, color='yellowgreen', bottom=avg_time_to_submission, yerr=err_time_from_sub_to_start, edgecolor = "none" )
    p3 = plt.bar(ind, avg_runtime, width, color='lightcoral', bottom=sum_for_avg_runtime, yerr=err_runtime, edgecolor = "none" )

    plt.ylabel('Time in seconds')
    plt.title('Pilot bulk submission: Distribution of times for Pilot stages')
    plt.xticks(ind+width/2., ('1', '2') )
    plt.xlabel('Number of Pilots')
    plt.yticks(numpy.arange(0,641,20))
    plt.legend( (p1[0], p2[0], p3[0]), ('avg time to submission', 'avg time from sub to start', 'avg runtime') )

    plt.show()
    #plt.savefig('pilot_bulk_sub_distribution_of_times_for_pilot_stages.png')

    # save data
    filename = "distribution-of-times-for-pilot-stages.dat"
    f = open(filename,'w')
    f.write("Number of pilots" + " 1 2 " + "\n")

    f.write("Average time to submission ")
    for entry in avg_time_to_submission:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average time from submission to start ")
    for entry in avg_time_from_sub_to_start:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average runtime ")
    for entry in avg_runtime:
        f.write(str(entry) + " ")
    f.write("\n")
    f.close()


    #--------------------------------------------------
    # plot: Distribution of times between functions
    #--------------------------------------------------

    N = 2

    ind = numpy.arange(N)    # the x locations for the groups
    width = 0.1       # the width of the bars: can also be len(x) sequence 

   
    # dividing avg_pm_submit_pilots_times by 100
    for n in range(len(avg_pm_submit_pilots_times)):
        avg_pm_submit_pilots_times[n] = avg_pm_submit_pilots_times[n] / 100.0

    for n in range(len(err_pm_submit_pilots_times)):
        err_pm_submit_pilots_times[n] = err_pm_submit_pilots_times[n] / 100.0


    sum_for_avg_um_submit_units_times = []
    for i in range(len(avg_um_add_pilots_times)):
        sum_for_avg_um_submit_units_times.append(avg_pm_submit_pilots_times[i] + avg_um_add_pilots_times[i]) 

    p1 = plt.bar(ind, avg_pm_submit_pilots_times,   width, color='lightskyblue', edgecolor = "none", yerr=err_pm_submit_pilots_times)
    p2 = plt.bar(ind, avg_um_add_pilots_times,   width, color='red', bottom=avg_pm_submit_pilots_times, edgecolor = "none", yerr=err_um_add_pilots_times)
    p3 = plt.bar(ind, avg_um_submit_units_times, width, color='yellowgreen', bottom=sum_for_avg_um_submit_units_times, edgecolor = "none", yerr=err_um_submit_units_times)

    plt.ylabel('Time in seconds')
    plt.title('Pilot bulk submission: Distribution of times between functions')
    plt.xticks(ind+width/2., ('1', '2') )
    plt.xlabel('Number of Pilots')
    plt.yticks(numpy.arange(0,4,0.2))
    plt.legend( (p1[0], p2[0], p3[0]), ('pm.submit_pilots() / 100', 'um.add_pilots()', 'um.submit_units()') )

    plt.show()
    #plt.savefig('pilot_bulk_sub_distribution_of_times_between_functions.png')

    # save data
    filename = "distribution-of-times-between-functions.dat"
    f = open(filename,'w')
    f.write("Number of pilots" + " 1 2 " + "\n")

    f.write("Average times for pm.submit_pilots() ")
    for entry in avg_pm_submit_pilots_times:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average times for um.add_pilots() ")
    for entry in avg_um_add_pilots_times:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average times for um.submit_units() ")
    for entry in avg_um_submit_units_times:
        f.write(str(entry) + " ")
    f.write("\n")
    f.close()

    #--------------------------------------------------
    # plot: Distribution of times for pm.submit_pilots()
    #--------------------------------------------------

    N = 2

    ind = numpy.arange(N)    # the x locations for the groups
    width = 0.1       # the width of the bars: can also be len(x) sequence 

    # dividing avg_pmsp_saga_bootstraping by 100
    for n in range(len(avg_pmsp_saga_bootstraping)):
        avg_pmsp_saga_bootstraping[n] = avg_pmsp_saga_bootstraping[n] / 100.0
    
    bottom_avg_pmsp_saga_bootstraping = []
    for i in range(len(avg_um_add_pilots_times)):
        bottom_avg_pmsp_saga_bootstraping.append(avg_pmsp_create_pilot_descr[i] + avg_pmsp_perform_checks[i]) 

    bottom_avg_pmsp_create_pilot_object = []
    for i in range(len(avg_um_add_pilots_times)):
        bottom_avg_pmsp_create_pilot_object.append(bottom_avg_pmsp_saga_bootstraping[i] + avg_pmsp_saga_bootstraping[i])

    bottom_avg_pmsp_create_database_entries = []
    for i in range(len(avg_um_add_pilots_times)):
        bottom_avg_pmsp_create_database_entries.append(bottom_avg_pmsp_create_pilot_object[i] + avg_pmsp_create_pilot_object[i]) 
 

    p1 = plt.bar(ind, avg_pmsp_create_pilot_descr,   width, color='lightskyblue', edgecolor = "none")
    p2 = plt.bar(ind, avg_pmsp_perform_checks,   width, color='yellowgreen', edgecolor = "none", bottom=avg_pmsp_create_pilot_descr )
    p3 = plt.bar(ind, avg_pmsp_saga_bootstraping, width, color='0.75', edgecolor = "none", bottom=bottom_avg_pmsp_saga_bootstraping )
    p4 = plt.bar(ind, avg_pmsp_create_pilot_object,   width, color='gold', edgecolor = "none", bottom=bottom_avg_pmsp_create_pilot_object )
    p5 = plt.bar(ind, avg_pmsp_create_database_entries,   width, color='lightcoral', edgecolor = "none", bottom=bottom_avg_pmsp_create_database_entries )

    plt.ylabel('Time in seconds')
    plt.title('Pilot bulk submission: Distribution of times for pm.submit_pilots()')
    plt.xticks(ind+width/2., ('1', '2') )
    plt.xlabel('Number of Pilots')
    plt.yticks(numpy.arange(0,4,0.2))
    plt.legend( (p1[0], p2[0], p3[0], p4[0], p5[0]), ('create pilot descriptions', 'perform checks', 'saga bootstraping / 100', 'create pilot object (ComputePilot)', 'bulk create database entries') )

    plt.show()
    #plt.savefig('pilot_bulk_sub_distribution_of_times_for_pm_submit_pilots.png')

    # save data
    filename = "distribution-of-times-for-pm_submit_pilots.dat"
    f = open(filename,'w')
    f.write("Number of pilots" + " 1 2 " + "\n")

    f.write("Average times to create pilot description ")
    for entry in avg_pmsp_create_pilot_descr:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average times perform checks ")
    for entry in avg_pmsp_perform_checks:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average times for saga_bootstraping ")
    for entry in avg_pmsp_saga_bootstraping:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average times to create pilot object (ComputePilot) ")
    for entry in avg_pmsp_create_pilot_object:
        f.write(str(entry) + " ")
    f.write("\n")

    f.write("Average times to bulk create database entries ")
    for entry in avg_pmsp_create_database_entries:
        f.write(str(entry) + " ")
    f.write("\n")

    f.close()

#-------------------------------------------------------------------------------


def pilot_bulk_submit_test_db_calls():

    print "Test: Adding Pilots in bulks: saga bootstrapping must be disabled"
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add a Pilot Manager and a Pilot to the session.
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        for i in [1,2,4,8,16,32,64,128,256]:
            
            pm_submit_pilots_times = []
            um_add_pilots_times = []
            um_submit_units_times = []

            for j in range(0, 5):
                
                pilots = []
                for k in range(0, i):
                    pd = sinon.ComputePilotDescription()
                    pd.resource = "futuregrid.SIERRA"
                    pd.working_directory = "/N/u/antonstre/sinon-performance"
                    pd.cores = 8
                    pd.run_time = 10
                    pd.cleanup = True
                    pilots.append(pd)
                
                start_time = time.time()
                pilot_objects = pm.submit_pilots(pilots)
                end_time = time.time() - start_time
                pm_submit_pilots_times.append(end_time)

                if i > 1:
                    for pilot in pilot_objects:
                        if pilot.state in [sinon.states.FAILED]:
                            print " * [ERROR] Pilot %s failed: %s." % (pilot, pilot.state_details[-1])
                            # Add some smart fault tolerance mechanism here!
                        #else:
                            #print " * [OK] Pilot %s submitted successfully: %s" % (pilot, pilot.state_details[-1])
                else:
                    if pilot_objects.state in [sinon.states.FAILED]:
                        print " * [ERROR] Pilot %s failed: %s." % (pilot_objects, pilot_objects.state_details[-1])
                        # Add some smart fault tolerance mechanism here!
                    #else:

                # Create a workload of i '/bin/date' compute units
                compute_units = []
                for unit_count in range(0, i):
                    cu = sinon.ComputeUnitDescription()
                    cu.cores = 1
                    cu.executable = "/bin/date"
                    compute_units.append(cu)

                # Add a Unit Manager to the session and add the newly created pilot to it 
                um = sinon.UnitManager(session=session, scheduler=sinon.SCHED_ROUND_ROBIN)  

                start_time = time.time()
                um.add_pilots(pilot_objects)
                end_time = time.time() - start_time
                um_add_pilots_times.append(end_time)

                start_time = time.time()
                um.submit_units(compute_units)
                end_time = time.time() - start_time
                um_submit_units_times.append(end_time)

                print "* Waiting for all compute units to finish..."
                um.wait_units()

                print "  FINISHED"
                pm.cancel_pilots()

            print "Average time to call pm.submit_pilots() for bulks of %i pilots is %f sec." % (i, numpy.mean(pm_submit_pilots_times))
            print "Average time to call um.add_pilots() for bulks of %i pilots is %f sec." % (i, numpy.mean(um_add_pilots_times))
            print "Average time to call um.submit_units() for bulks of %i pilots is %f sec." % (i, numpy.mean(um_submit_units_times))
            
    except sinon.SinonException, ex:
        print "Error: %s" % ex

    try:        
        session.destroy()
    except sinon.SinonException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------

def pilot_serialised_submit_test():
    
    print "Test: Adding Pilots in series"
    try:
        
        session = sinon.Session(database_url=DBURL)
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)
        um = sinon.UnitManager(session=session)

        for i in [1,2,4,8,16,32,64,128]:
            
            pilot_insert_times = []
            for j in range(0, 5):                
                t_in_1 = time.time()
                for k in range(0, i):
                    pd = sinon.ComputePilotDescription()
                    #pd.resource = "futuregrid.INDIA"
                    pd.resource = "localhost"
                    pd.cores = 32
                    pd.run_time = 10
                    #pd.cleanup = True
                    pilot_objects = pm.submit_pilots(pd)
                t_in_2 = time.time() - t_in_1
                pilot_insert_times.append(t_in_2)
       
                if i > 1:
                    for pilot in pilot_objects:
                        if pilot.state in [sinon.states.FAILED]:
                            print " * [ERROR] Pilot %s failed: %s." % (pilot, pilot.state_details[-1])
                            # Add some smart fault tolerance mechanism here!
                        #else:
                            #print " * [OK] Pilot %s submitted successfully: %s" % (pilot, pilot.state_details[-1])
                else:
                    if pilot_objects.state in [sinon.states.FAILED]:
                        print " * [ERROR] Pilot %s failed: %s." % (pilot_objects, pilot_objects.state_details[-1])
                        # Add some smart fault tolerance mechanism here!
                    #else:
                        #print " * [OK] Pilot %s submitted successfully: %s" % (pilot_objects, pilot_objects.state_details[-1])

                pm.cancel_pilots()

            print "Average time to add a series of %i Pilots: %f sec." % (i, numpy.mean(pilot_insert_times))

    except sinon.SinonException, ex:
        print "Error: %s" % ex

    try:        
        session.destroy()
    except sinon.SinonException, ex:
        print "Error: %s" % ex

#----------------------------------------------------------------------------------

def cu_bulk_submit_test():

    print "Test: Adding CUs in bulks"
    try:
        session = sinon.Session(database_url=DBURL)
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)
        um = sinon.UnitManager(session=session, scheduler=sinon.SCHED_ROUND_ROBIN) 

        #for i in [1,2,4,8,16,32,64,128,256,512,1024]:
        for i in [1,2]:
            
            pm_submit_pilots_times = []
            um_add_pilots_times = []
            um_submit_units_times = []

            time_to_submission = []
            time_from_sub_to_start = []
            runtime = []

            pilot = []
            for j in range(0, 1):
                pd = sinon.ComputePilotDescription()
                pd.resource = "futuregrid.SIERRA"
                pd.working_directory = "/N/u/antonstre/sinon-performance"
                pd.cores = 8
                pd.run_time = 10
                pd.cleanup = True
                pilot.append(pd)

                init_time = datetime.datetime.utcnow()
                init_time_posix = time.mktime(init_time.timetuple())


                start_time = time.time()
                pilot_object = pm.submit_pilots(pilot)
                stop_time = time.time() - start_time
                pm_submit_pilots_times.append(stop_time)


                start_time = time.time()
                um.add_pilots(pilot_object)
                stop_time = time.time() - start_time
                um_add_pilots_times.append(stop_time)

                
                compute_units = []
                for k in range(0, i):
                    cu = sinon.ComputeUnitDescription()
                    cu.cores = 1
                    cu.executable = "/bin/date"
                    compute_units.append(cu)

                start_time = time.time()
                um.submit_units(compute_units)
                stop_time = time.time() - start_time
                um_submit_units_times.append(stop_time)

                print "* Waiting for all compute units to finish..."
                um.wait_units()

                print "  FINISHED"
                pm.cancel_pilots()
                time.sleep(5)

                print "pilot obj: "
                print pilot_object

                print "Time init time is: %f" % init_time_posix

                p_sub_time = pilot_object._get_submission_time_priv()
                p_sub_time_posix = time.mktime(p_sub_time.timetuple())
                print "Pilot submission_time is: %f" % p_sub_time_posix

                p_start_time = pilot_object._get_start_time_priv()
                p_start_time_posix = time.mktime(p_start_time.timetuple())
                print "Pilot start_time is: %f" % p_start_time_posix 

                p_stop_time = pilot_object._get_stop_time_priv()
                p_stop_time_posix = time.mktime(p_stop_time.timetuple())
                print "Pilot stop_time is: %f" % p_stop_time_posix 

                time_i = p_sub_time_posix - init_time_posix
                time_to_submission.append(time_i)
                time_s = p_start_time_posix - p_sub_time_posix
                time_from_sub_to_start.append(time_s)
                time_r = p_stop_time_posix - p_start_time_posix
                runtime.append(time_r)

            print "Average time to submit pilot is: %f sec." % ( numpy.mean(time_to_submission) )
            print "Average time from submission to start for pilot is: %f sec." % ( numpy.mean(time_from_sub_to_start) )
            print "Average runtime to run pilot is: %f sec." % ( numpy.mean(runtime) )

            print "Average time to call pm.submit_pilots() is %f sec." % (numpy.mean(pm_submit_pilots_times))
            print "Average time to call um.add_pilots() is %f sec." % (numpy.mean(um_add_pilots_times))
            print "Average time to call um.submit_units() is %f sec." % (numpy.mean(um_submit_units_times))
            
    except sinon.SinonException, ex:
        print "Error: %s" % ex

    try:        
        session.destroy()
    except sinon.SinonException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------

def cu_serialised_submit_test():

    print "Test: Adding CUs in series"
    try:
        session = sinon.Session(database_url=DBURL)
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        pd = sinon.ComputePilotDescription()
        #pd.resource = "futuregrid.INDIA"
        pd.resource = "localhost"
        pd.cores = 120
        pd.run_time = 10
        p1 = pm.submit_pilots(pd)
       
        um = sinon.UnitManager(session=session)
        um.add_pilots(p1)

        for i in [1,2,4,8,16,32,64,128]:
            #
            compute_unit_insert_times = []
            for j in range(0, 5):
                #
                t_in_1 = time.time()
                for k in range(0, i):
                    cu = sinon.ComputeUnitDescription()
                    cu.executable = "/bin/hostname"
                    um.submit_units(cu)
                t_in_2 = time.time() - t_in_1

                compute_unit_insert_times.append(t_in_2)

            print "Average time to add a series of %i compute units: %f sec." % (i, numpy.mean(compute_unit_insert_times))
            
    except sinon.SinonException, ex:
        print "Error: %s" % ex

    try:        
        session.destroy()
    except sinon.SinonException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------

if __name__ == "__main__":

    
    session_uid = pilot_bulk_submit_test_real()

    #session_uid = cu_bulk_submit_test()

    print "performance test completed..."

  

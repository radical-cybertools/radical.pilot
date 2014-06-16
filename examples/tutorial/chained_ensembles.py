import os
import sys
import pilot
import traceback

""" DESCRIPTION: Tutorial 2: Chaining Tasks
Note: User must edit PILOT SETUP and TASK DESCRIPTION 1-2 sections
This example will not run if these values are not set.
"""

# ---------------- BEGIN REQUIRED PILOT SETUP -----------------

# Distributed Coordination Service - Redis server and password
REDIS_PWD   = # Fill in the password to your redis server
REDIS_URL   = "redis://%s@localhost:6379" % REDIS_PWD

# Resource Information
HOSTNAME     = # Remote Resource URL
USER_NAME    = # Username on the remote resource
SAGA_ADAPTOR = # Name of the SAGA adaptor, e.g. fork, sge, pbs, slurm, etc.
# NOTE: See complete list of BigJob supported SAGA adaptors at:
# http://saga-project.github.io/BigJob/sphinxdoc/tutorial/table.html

# Fill in queue and allocation for the given resource 
# Note: Set fields to "None" if not applicable
QUEUE        = # Add queue you want to use
PROJECT      = # Add project / allocation / account to charge

WALLTIME     = # Maximum Runtime (minutes) for the Pilot Job

WORKDIR      = "" # Path of Resource Working Directory
# This is the directory where BigJob will store its output and error files

SPMD_VARIATION = # Specify the WAYNESS of SGE clusters ONLY, valid input '12way' for example. 

PROCESSES_PER_NODE = # Valid on PBS clusters ONLY - this is the number of processors per node. One processor core is treated as one processor on PBS; e.g. a node with 8 cores has a maximum ppn=8

PILOT_SIZE = # Number of cores required for the Pilot-Job

# Job Information
NUMBER_JOBS  = # The TOTAL number of tasks to run

# Continue to USER DEFINED TASK DESCRIPTION to add 
# the required information about the individual tasks.

# ---------------- END REQUIRED PILOT SETUP -----------------
#

def main():
    try:
        # this describes the parameters and requirements for our pilot job
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "%s://%s@%s" %  (SAGA_ADAPTOR,USER_NAME,HOSTNAME)
        pilot_description.queue = QUEUE
        pilot_description.project = PROJECT
        pilot_description.number_of_processes = PILOT_SIZE
        pilot_description.working_directory = WORKDIR
        pilot_description.walltime = WALLTIME
	pilot_description.processes_per_node = PROCESSES_PER_NODE
	pilot_description.spmd_variation = SPMD_VARIATION

        # create a new pilot job
        pilot_compute_service = pilot.PilotComputeService(REDIS_URL)
        pilotjob = pilot_compute_service.create_pilot(pilot_description)

        # submit 'A' tasks to pilot job
        task_set_A = list()
        for i in range(NUMBER_JOBS):

	# -------- BEGIN USER DEFINED TASK 1 DESCRIPTION --------- #
            task_desc = pilot.ComputeUnitDescription()
            task_desc.executable = '/bin/echo'
            task_desc.arguments = ['I am an $TASK_SET task with id $TASK_NO', ]
            task_desc.environment = {'TASK_SET': 'A', 'TASK_NO': i}
	    task_desc.spmd_variation = 'single'
            task_desc.number_of_processes = 1
            task_desc.output = 'A-stdout.txt'
            task_desc.error  = 'A-stderr.txt'
	# -------- END USER DEFINED TASK 1 DESCRIPTION --------- #

	    # Submit task to PilotJob
            task = pilotjob.submit_compute_unit(task_desc)
            print "* Submitted 'A' task '%s' with id '%s'" % (i, task.get_id())
            task_set_A.append(task)

        # Chaining tasks i.e submit a compute unit, when compute unit from A is successfully executed.
        # A 'B' task reads the content of the output file of an 'A' task and writes it into its own
        # output file.
        task_set_B = list()
        while len(task_set_A) > 0:
            for a_task in task_set_A:
                if a_task.get_state() == "Done":
                    print "One 'A' task %s finished. Launching a 'B' task." % (a_task.get_id())

	# -------- BEGIN USER DEFINED TASK 2 DESCRIPTION --------- #
                    task_desc = pilot.ComputeUnitDescription()
                    task_desc.executable = '/bin/echo'
                    task_desc.arguments = ['I am a $TASK_SET task with id $TASK_NO', ]
                    task_desc.environment = {'TASK_SET': 'B', 'TASK_NO': a_task}
	    	    task_desc.spmd_variation = 'single'
                    task_desc.number_of_processes = 1
                    task_desc.output = 'B-stdout.txt'
                    task_desc.error  = 'B-stderr.txt'
	# -------- END USER DEFINED TASK 2 DESCRIPTION --------- #

		    # Submit task to Pilot Job
                    task = pilotjob.submit_compute_unit(task_desc)
                    print "* Submitted 'B' task '%s' with id '%s'" % (i, task.get_id())
                    task_set_B.append(task)
                    task_set_A.remove(a_task)

        return(0)

    except Exception, ex:
            print "AN ERROR OCCURRED: %s" % ((str(ex)))
            # print a stack trace in case of an exception -
            # this can be helpful for debugging the problem
            traceback.print_exc()
            return(-1)

    finally:
        # alway try to shut down pilots, otherwise jobs might end up
        # lingering in the queue
        print ("Terminating BigJob...")
        pilotjob.cancel()
        pilot_compute_service.cancel()


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.pilot as rp

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

""" DESCRIPTION: Tutorial 3: Coupled Tasks
For every task A1 and B1 a C1 is started.
"""

# READ: The RADICAL-Pilot documentation:
#   https://radicalpilot.readthedocs.io/en/stable/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scenes!


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    RESOURCE_LABEL = None
    PILOT_CORES    = None
    NUMBER_COUPLES = None
    TASK_A_CORES   = None
    TASK_B_CORES   = None
    TASK_C_CORES   = None
    QUEUE          = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:


        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        print("Initializing Pilot Manager ...")
        pmgr = rp.PilotManager(session=session)


        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        #
        # If you want to run this example on your local machine, you don't have
        # to change anything here.
        #
        # Change the resource below if you want to run on a remote resource.
        # You also might have to set the 'project' to your allocation ID if
        # your remote resource does compute time accounting.
        #
        # A list of preconfigured resources can be found at:
        # https://radicalpilot.readthedocs.io/en/stable/machconf.html#preconfigured-resources
        #
        pdesc = rp.PilotDescription ()
        pdesc.resource = RESOURCE_LABEL
        pdesc.runtime  = 30
        pdesc.cores    = PILOT_CORES
        pdesc.cleanup  = True
        pdesc.queue    = QUEUE

        # submit the pilot.
        print("Submitting  Pilot to Pilot Manager ...")
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object.
        print("Initializing Task Manager ...")
        tmgr = rp.TaskManager (session=session)

        # Add the created Pilot to the TaskManager.
        print("Registering  Pilot with Task Manager ...")
        tmgr.add_pilots(pilot)

        # submit A tasks to pilot job
        taskdesc_list_A = []
        for idx in range(NUMBER_COUPLES):

            # -------- BEGIN USER DEFINED Task 1 DESCRIPTION --------- #
            taskdesc = rp.TaskDescription()
            taskdesc.environment = {"TASK_LIST": "A", "TASK_NO": "%02d" % idx}
            taskdesc.executable  = "/bin/echo"
            taskdesc.arguments   = ['"$TASK_LIST Task with id $TASK_NO"']
            taskdesc.cores       = TASK_A_CORES
            # -------- END USER DEFINED Task 1 DESCRIPTION --------- #

            taskdesc_list_A.append(taskdesc)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        print("Submit Tasks 'A' to Task Manager ...")
        task_set_A = tmgr.submit_tasks(taskdesc_list_A)

        # submit B tasks to pilot job
        taskdesc_list_B = []
        for idx in range(NUMBER_COUPLES):

            # -------- BEGIN USER DEFINED Task 2 DESCRIPTION --------- #
            taskdesc = rp.TaskDescription()
            taskdesc.environment = {"TASK_LIST": "B", "TASK_NO": "%02d" % idx}
            taskdesc.executable  = "/bin/echo"
            taskdesc.arguments   = ['"$TASK_LIST Task with id $TASK_NO"']
            taskdesc.cores       = TASK_B_CORES
            # -------- END USER DEFINED Task 2 DESCRIPTION --------- #

            taskdesc_list_B.append(taskdesc)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        print("Submit Tasks 'B' to Task Manager ...")
        task_set_B = tmgr.submit_tasks(taskdesc_list_B)


        # ---------------------------------------------------------------------
        print("Waiting for 'A' and 'B' tasks to complete...")
        tmgr.wait_tasks()
        print("Executing 'C' tasks now...")
        # ---------------------------------------------------------------------

        # submit 'C' tasks to pilot job. each 'C' task takes the output of
        # an 'A' and a 'B' task and puts them together.
        taskdesc_list_C = []
        for idx in range(NUMBER_COUPLES):

            # -------- BEGIN USER DEFINED Task 3 DESCRIPTION --------- #
            taskdesc = rp.TaskDescription()
            taskdesc.environment = {"TASK_SET": "C", "TASK_NO": "%02d" % idx}
            taskdesc.executable  = "/bin/echo"
            taskdesc.arguments   = ['"$TASK_SET Task with id $TASK_NO"']
            taskdesc.cores       = TASK_C_CORES
            # -------- END USER DEFINED Task 3 DESCRIPTION --------- #

            taskdesc_list_C.append(taskdesc)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        print("Submit Tasks 'C' to Task Manager ...")
        task_set_C = tmgr.submit_tasks(taskdesc_list_C)

        # ---------------------------------------------------------------------
        print("Waiting for 'C' tasks to complete...")
        tmgr.wait_tasks()
        print("All tasks completed successfully!")


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print("caught Exception: %s" % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print("need to exit now: %s" % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print("closing session")
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


# ------------------------------------------------------------------------------


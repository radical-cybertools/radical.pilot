#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.pilot as rp


verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

""" DESCRIPTION: Tutorial 2: Chaining Tasks.
For every task A_n a task B_n is started consecutively.
"""

# READ: The RADICAL-Pilot documentation:
#   https://radicalpilot.readthedocs.io/en/stable/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scenes!


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    RESOURCE_LABEL    = None
    PILOT_CORES       = None
    NUMBER_CHAINS     = None
    TASK_A_EXECUTABLE = None
    TASK_B_EXECUTABLE = None
    QUEUE             = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        #
        # Change the user name below if you are using a remote resource
        # and your username on that resource is different from the username
        # on your local machine.
        #


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
        # https://radicalpilot.readthedocs.io/en/stable/ \
        #        machconf.html#preconfigured-resources
        #
        pdesc = rp.PilotDescription ()
        pdesc.resource = RESOURCE_LABEL
        pdesc.runtime  = 30
        pdesc.cores    = PILOT_CORES
        pdesc.cleanup  = True

        # submit the pilot.
        print("Submitting Pilot to Pilot Manager ...")
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
        for i in range(NUMBER_CHAINS):

            # -------- BEGIN USER DEFINED Task A_n DESCRIPTION --------- #
            taskdesc = rp.TaskDescription()
            taskdesc.environment = {"TASK_LIST": "A", "TASK_NO": "%02d" % i}
            taskdesc.executable  = TASK_A_EXECUTABLE
            taskdesc.arguments   = ['"$TASK_LIST Task with id $TASK_NO"']
            taskdesc.cores       = 1
            # -------- END USER DEFINED Task A_n DESCRIPTION --------- #

            taskdesc_list_A.append(taskdesc)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        print("Submit 'A' Tasks to Task Manager ...")
        task_list_A = tmgr.submit_tasks(taskdesc_list_A)

        # Chaining tasks i.e submit a task, when task from A is
        # successfully executed.  A B Task reads the content of the output file of
        # an A Task and writes it into its own output file.
        task_list_B = []

        # We create a copy of task_list_A so that we can remove elements from it,
        # and still reference to the original index.
        task_list_A_copy = task_list_A[:]
        while task_list_A:
            for task_a in task_list_A:
                idx = task_list_A_copy.index(task_a)

                task_a.wait ()
                print("'A' Task '%s' done. Submitting 'B' Task ..." % idx)

                # -------- BEGIN USER DEFINED Task B_n DESCRIPTION --------- #
                taskdesc = rp.TaskDescription()
                taskdesc.environment = {'TASK_LIST': 'B', 'TASK_NO': "%02d" % idx}
                taskdesc.executable  = TASK_B_EXECUTABLE
                taskdesc.arguments   = ['"$TASK_LIST Task with id $TASK_NO"']
                taskdesc.cores       = 1
                # -------- END USER DEFINED Task B_n DESCRIPTION --------- #

                # Submit Task to Pilot Job
                task_b = tmgr.submit_tasks(taskdesc)
                task_list_B.append(task_b)
                task_list_A.remove(task_a)

        print("Waiting for 'B' Tasks to complete ...")
        for task_b in task_list_B :
            task_b.wait ()
            print("'B' Task '%s' finished with output:" % (task_b.uid))
            print(task_b.stdout)

        print("All Tasks completed successfully!")

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


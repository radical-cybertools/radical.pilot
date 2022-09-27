#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp

SHARED_INPUT_FILE = 'shared_input_file.txt'
MY_STAGING_AREA = 'pilot:///'

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        session_name = sys.argv[1]
    else:
        session_name = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(uid=session_name)
    print("session id: %s" % session.uid)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # Create shared input file
        os.system('/bin/echo -n "Hello world, " > %s' % SHARED_INPUT_FILE)
        radical_cockpit_occupants = ['Alice', 'Bob', 'Carol', 'Eve']

        # Create per task input files
        for idx, occ in enumerate(radical_cockpit_occupants):
            input_file = 'input_file-%d.txt' % (idx + 1)
            os.system('/bin/echo "%s" > %s' % (occ, input_file))

        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        pmgr = rp.PilotManager(session=session)

        # Define a C-core on $RESOURCE that runs for M minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = rp.PilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime  = 5  # M minutes
        pdesc.cores    = 2  # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE)

        staged_file = "%s%s" % (MY_STAGING_AREA, SHARED_INPUT_FILE)
        print("##########################")
        print(staged_file)
        print("##########################")

        # Configure the staging directive for to insert the shared file into
        # the pilot staging directory.
        sd_pilot = {'source': shared_input_file_url,
                    'target': staged_file,
                    'action': rp.TRANSFER
        }
        # Synchronously stage the data to the pilot
        pilot.stage_in(sd_pilot)

        # Configure the staging directive for shared input file.
        sd_shared = {'source': staged_file,
                     'target': SHARED_INPUT_FILE,
                     'action': rp.LINK
        }

        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object.
        tmgr = rp.TaskManager(session=session)

        # Add the previously created Pilot to the TaskManager.
        tmgr.add_pilots(pilot)

        task_descs = []

        for task_idx in range(len(radical_cockpit_occupants)):

            # Configure the per task input file.
            input_file = 'input_file-%d.txt' % (task_idx + 1)

            # Configure the for per task output file.
            output_file = 'output_file-%d.txt' % (task_idx + 1)

            # Actual task description.
            # Concatenate the shared input and the task specific input.
            td = rp.TaskDescription()
            td.executable = '/bin/bash'
            td.arguments = ['-c', 'cat %s %s > %s' %
                             (SHARED_INPUT_FILE, input_file, output_file)]
            td.input_staging = [sd_shared, input_file]
            td.output_staging = output_file

            task_descs.append(td)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        tasks = tmgr.submit_tasks(task_descs)

        # Wait for all tasks to finish.
        tmgr.wait_tasks()

        for task in tmgr.get_tasks():

            # Get the stdout and stderr streams of the Task.
            print(" STDOUT: %s" % task.stdout)
            print(" STDERR: %s" % task.stderr)

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


# -------------------------------------------------------------------------------


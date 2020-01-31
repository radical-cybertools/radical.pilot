__author__    = "George Chantzialexiou"
__copyright__ = "Copyright  2014, The RADICAL Group"
__license__   = "MIT"

import sys
import radical.pilot as rp
import math
import time


"""
This is a simple implementation of k-means algorithm using the RADICAl-Pilot API
"""


# ------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print("[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state))

    if state == rp.FAILED:
        sys.exit (1)


# ------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    print("[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state))

    if state == rp.FAILED:
        print("stderr: %s" % unit.stderr)
        # do not exit


# ------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print("[Callback]: wait_queue_size: %s." % wait_queue_size)


# ------------------------------------------------------------------------------
#
def quickselect(x, k):
    # k should start from 0
    # returns the element itself

    pivot = x[len(x) // 2]
    left = [e for e in x if e < pivot]
    right = [e for e in x if e > pivot]

    delta = len(x) - len(right)
    if k < len(left):
        return quickselect(left, k)
    elif k >= delta:
        return quickselect(right, k - delta)
    else:
        return pivot


# -------------------------------------------------------------------------
#
def get_distance(dataPointX, dataPointY, centroidX, centroidY):

    # Calculate Euclidean distance.
    return math.sqrt(math.pow((centroidY - dataPointY), 2) +
                     math.pow((centroidX - dataPointX), 2))


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print("session id: %s" % session.uid)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        start_time = time.time()

        # FUNCTIONS OF RADICAL PILOT
        # don't forget to change the localhost label
        c = rp.Context('ssh')
        #c.user_id = 'userid'
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print("Initiliazing Pilot Manager...")
        pmgr = rp.PilotManager(session=session)

        # Register our callback with our Pilot Manager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state
        pmgr.register_callback(pilot_state_cb)

        # This describes the requirements and the parameters
        pdesc          = rp.ComputePilotDescription()
        pdesc.resource = "localhost"
      # pdesc.resource = "xsede.stampede"
      # pdesc.project  = "TG-MCB090174"
        pdesc.runtime  = 10
        pdesc.cores    = 4

        print("Submitting Compute Pilot to PilotManager")
        pilot = pmgr.submit_pilots(pdesc)

        # Combine all the units
        print("Initiliazing Unit Manager")

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        print('Registering the callbacks so we can keep an eye on the CUs')
        umgr.register_callback(unit_state_cb)

        print("Registering Compute Pilot with Unit Manager")
        umgr.add_pilots(pilot)
        # need to read the number of the k division the user want to do: that's also the only argument
        args = sys.argv[1:]
        if len(args) < 1:
            print("Usage: python %s needs the k-variable. Please run again" % __file__)
            print("python k-means k")
            sys.exit(-1)
        k = int(sys.argv[1])  # k is the number of clusters i want to create
        # read the dataset from dataset.data file and pass the elements to x list
        try:
            data = open("dataset4.in",'r')
        except IOError:
            print("Missing dataset. file! Run:")
            sys.exit(-1)

        dataset_as_string_array = data.readline().split(',')
        x = map(float, dataset_as_string_array)
        data.close()
        # initialize the centroids: choose the centroids and delete them
        # from the element list (x)
        # FUNCTIONS OF RADICAL PILOT


        #  CHOOSING THE CENTROIDS
        centroid = []
        size = len(x)
        for i in range(0,k):
            a1 = quickselect(x, (size * (i + 1)) // ((k + 1)))
            centroid.append(a1)

        for i in range(0,k):
            x.remove(centroid[i])
        # END OF CHOOSING CENTROIDS

        # PUT THE CENTROIDS IN A FILE
        centroid_to_string = ','.join(map(str,centroid))
        centroid_file = open('centroids.txt', 'w')
        centroid_file.write(centroid_to_string)
        centroid_file.close()
        # END OF PUTTING CENTROIDS IN A FILE CENTROIDS.DATA


        # VARIABLE DEFINITIONS
        p           = pdesc.cores  # NUMBER OF CORES OF THE SYSTEM I USE
        convergence = False        # We have no convergence yet
        m           = 0            # number of iterations
        maxIt       = 20           # the maximum number of iteration
        part_length = len(x) / p   # this is the length of the part that each unit is going to control
        # END OF VARIABLE DEFINITIONS


        while ((m < maxIt) and (convergence is False)):

            # PUT THE CENTROIDS INTO DIFFERENT FILES
            for i in range(1, p + 1):

                input_file = open("cu_%d.data" % i, "w")
                p1 = part_length * (i - 1)
                p2 = part_length * (i)

                if (i == p):
                    p2 = len(x)

                input_string = ','.join(map(str,x[p1:p2]))
                input_file.write(input_string)
                input_file.close()
            # END OF PUTTING CENTROIDS INTO DIFFERENT FILES


            # GIVE THE FILES INTO CUS TO START CALCULATING THE CLUSTERS - PHASE A
            mylist = []
            for i in range(1, p + 1):

                cudesc                = rp.ComputeUnitDescription()
                cudesc.executable     = "python"
                cudesc.arguments      = ['clustering_the_elements.py', i, k]
                cudesc.input_staging  = ['clustering_the_elements.py',
                                         'cu_%d.data' % i,'centroids.txt']
                cudesc.output_staging = 'centroid_cu_%d.data' % i
                mylist.append(cudesc)

            print('Submitting the CU to the Unit Manager...')
            mylist_units = umgr.submit_units(mylist)

            # wait for all units to finish
            umgr.wait_units()
            print("All Compute Units completed PhaseA successfully! Now.." )
          # print " We compose the files into k files "

            # END OF PHASE A - NOW WE HAVE THE CLUSTERS CALCULATED IN THE CU FILES


            # FINDING THE AVG_ELEMENTS WHICH ARE CANDIDATE CENTROIDS

            sum_of_all_centroids = []
            for i in range(0, 2 * k):
                sum_of_all_centroids.append(0)

            for i in range(1, p + 1):
                read_file = open("centroid_cu_%d.data" % i, "r")
                read_as_string_array = read_file.readline().split(',')
                read_as_float_array  = map(float, read_as_string_array)

                for j in range(0, k):
                    x = 2 * j
                    y = x + 1
                    sum_of_all_centroids[x] += read_as_float_array[x]
                    sum_of_all_centroids[y] += read_as_float_array[y]
                read_file.close()

            for i in range(0,k):
                x = 2 * j
                y = x + 1
                if (sum_of_all_centroids[y] != 0):
                    sum_of_all_centroids[i] = sum_of_all_centroids[x] \
                                            / sum_of_all_centroids[y]
                else:
                    # there are no centroids in this cluster
                    sum_of_all_centroids[i] = -1

            # writing the elements to a file
            input_file   = open('centroids.txt','w')
            input_string = ','.join(map(str,sum_of_all_centroids[0:p]))
            input_file.write(input_string)
            input_file.close()


            # END THE AVG_ELEMENTS WHICH ARE CANDIATE CENTROIDS
            mylist = []

            for i in range(1, p + 1):

                cudesc                = rp.ComputeUnitDescription()
                cudesc.executable     = "python"
                cudesc.arguments      = ['finding_the_new_centroids.py', i, k]
                cudesc.input_staging  = ['finding_the_new_centroids.py',
                                         'cu_%d.data' % i,'centroids.txt']
                cudesc.output_staging = 'centroid_cu_%d.data' % i
                mylist.append(cudesc)

            print('Submitting the CU to the Unit Manager...')
            mylist_units = umgr.submit_units(mylist)

            # wait for all units to finish
            umgr.wait_units()
            print("All Compute Units completed PhaseB successfully! Now.." )
          # print " We compose the files into k files "


            # FINDING THE NEW CENTROIDS- THESE ARE THE ELEMENTS WHO ARE CLOSER
            # TO THE AVG_ELEMENTS
            # sum_of_all_centroids have the avg_elemnt
            new_centroids = []
            a = sys.maxint
            for i in range(0,k):
                new_centroids.append(a)

            for i in range(1, p + 1):
                read_file = open("centroid_cu_%d.data" % i, "r")
                read_as_string_array = read_file.readline().split(',')
                read_as_float_array  = map(float,read_as_string_array)

                for j in range(0,k):
                    d0 = get_distance(new_centroids[j],        0,
                                      sum_of_all_centroids[j], 0)
                    d1 = get_distance(read_as_float_array[j],  0,
                                      sum_of_all_centroids[j], 0)
                    if d0 > d1:
                        new_centroids[j] = read_as_float_array[j]

                read_file.close()


            # END OF FINDING THE NEW CENTROIDS- THESE ARE THE ELEMENTS WHO ARE
            # CLOSER TO THE AVG_ELEMENT


            # CHECKING FOR CONVERGENCE

            # now we check for convergence - the prev centroids are in
            # !centroid! and the new are in !new_centroids!
            print("Now we check the converge")
            print('new centroids:')
            print(new_centroids)
            print('Old centroids:')
            print(centroid)
           # print 'element list'
           # print x

            convergence = True
            for i in range(0, len(new_centroids)):
                if (abs(new_centroids[i] - centroid[i]) > 1000):
                    # have elements from 1 to 10.000 so I give 1000 convergence
                    convergence = False

            if convergence is False:
                m += 1
                for l in range(0,k):
                    # put the old centroids to the element file... remove the
                    # new cetroids from element list
                    if (centroid[l] != new_centroids[l]):
                        x.remove(new_centroids[l])
                        x.append(centroid[l])

                centroid     = new_centroids
                input_string = ','.join(map(str,new_centroids))
                input_file   = open('centroids.txt', 'w')

                input_file.write(input_string)
                input_file.close()

            # END OF CHECKING FOR CONVERGENCE

        print('K-means algorithm ended successfully after %d iterations' % m)
        # the only thing i have to do here is to check for convergence & add the
        # file- arguments into the enviroments
      # print 'Thre centroids are in the cetroidss.txt file, and the elements
      #        of each centroid at centroid_x.data file'
        print('The centroids of these elements are: \n')
        print(new_centroids)

        # END OF K-MEANS ALGORITHM
        finish_time = time.time()
        total_time = finish_time - start_time  # total execution time
        print('The total execution time is: %f seconds' % total_time)
        total_time /= 60

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


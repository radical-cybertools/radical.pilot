__author__    = "George Chantzialexiou"
__copyright__ = "Copyright 2012-2013, The RADICAL Group"
__license__   = "MIT"

""" A Mandelbrot Fractal Generator Using RADICAL-Pilot

    This is an example of mandelbrot Fractal Generator
    using the capabilities of Pilot Job API.

    It requires the Python Image Library (PIL) which can be easily
    installed with 'pip install Pillow'. Also, it requires the
    mandel_lines.py program to generate the parts of the fractal.

    The parameters are the following:

        imgX, imgY: the dimensions of the mandelbrot image, e.g. 1024, 1024
        xBeg, xEnd: the x-axis portion of the (sub-)image to calculate
        yBeg, yEnd: the y-axis portion of the (sub-)image to calculate


    This module takes the parameters of the Mandelbrot fractal and decompose
    the image into n different parts, where n is the number of the cores of
    the system. Then it runs for every part the mandelbrot Generator Code 
    which is the mandel_lines.py. The mandel_lines.py creates n Images and
    then we compose the n images into one. The whole fractal Image.
    For every part of the image we create one Compute Unit.

    You can run this code via command line:

    python mandelbrot_pilot.py imgX imgY xBeg xEnd yBeg yEnd

"""

import os, sys,  radical.pilot
from PIL import Image
import multiprocessing # this library is used to find the number of the cores.

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """

    if state == radical.pilot.states.FAILED:
        print "Compute Pilot '%s' failed, exiting ..." % pilot.uid
        sys.exit(1)

    elif state == radical.pilot.states.ACTIVE:
        print "Compute Pilot '%s' became active!" % (pilot.uid)

#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    if state == radical.pilot.states.FAILED:
        print "Compute Unit '%s' failed ..." % unit.uid
        sys.exit(1)

    elif state == radical.pilot.states.DONE:
        print "Compute Unit '%s' finished with output:" % (unit.uid)
        print unit.stdout

#------------------------------------------------------------------------------
#


def main():
    try:
        # reading the input from user:
        args = sys.argv[1:]

        if len(args) < 6:
            print "Usage: python %s imgX imgY xBeg xEnd yBeg yEnd filename" % __file__
            sys.exit(-1)

        imgX = int(sys.argv[1])
        imgY = int(sys.argv[2])
        xBeg = int(sys.argv[3])
        xEnd = int(sys.argv[4])
        yBeg = int(sys.argv[5])
        yEnd = int(sys.argv[6])
        # end of reading  input from the user

        session = radical.pilot.Session()

        # Add the following three lines if you want to run remote
        c = radical.pilot.Context('ssh')
        #c.user_id = 'user_id'
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initiliazing Pilot Manager..."
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with our Pilot Manager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state
        pmgr.register_callback(pilot_state_cb)

        # This describes the requirements and the parameters
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "localhost" #  we are running on localhost
        #pdesc.resource = "xsede.stampede" #  we are running on stampede
        pdesc.runtime = 10 # minutes
        pdesc.cores = multiprocessing.cpu_count() # we use all the cores we have
        pdesc.cleanup = True  # delete all the files that are created automatically and we don't need anymore  when the job is done
        pdesc.project  = 'TG-MCB090174'

        print "Submitting Compute Pilot to PilotManager"
        pilot = pmgr.submit_pilots(pdesc)

        umgr = radical.pilot.UnitManager(session=session, scheduler = radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Combine all the units
        print "Initializing Unit Manager"

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        print 'Registering the callbacks so we can keep an eye on the CUs'
        umgr.register_callback(unit_state_change_cb)

        print "Registering Compute Pilot with Unit Manager"
        umgr.add_pilots(pilot)

        output_data_list = []
        mylist = []

        for i in range(1,pdesc.cores+1):
            output_data_list.append('mandel_%d.gif' % i)
            # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
            cudesc = radical.pilot.ComputeUnitDescription()
            cudesc.executable  = "python"
            # Pre-execs to configure environment for CU on localhost
            cudesc.pre_exec = ["virtualenv ./ve", "source ./ve/bin/activate", "pip install Pillow"]
            # Pre-execs to configure environment for CU on stampede
            #cudesc.pre_exec = ["module load python", "python ../virtualenv-1.9/virtualenv.py ./ve", "source ./ve/bin/activate", "pip install Pillow"]
            cudesc.arguments = ['mandel_lines.py', imgX, imgY, xBeg, xEnd, yBeg, yEnd, pdesc.cores, i]
            cudesc.input_staging = ['mandel_lines.py']
            cudesc.output_staging = output_data_list[i-1]
            mylist.append(cudesc)
            # -------- END USER DEFINED CU DESCRIPTION --------- #

        print 'Submitting the CU to the Unit Manager...'
        mylist_units = umgr.submit_units(mylist)

        # wait for all units to finish
        umgr.wait_units()

        print "All Compute Units completed successfully! Now.."

        # stitch together the final image
        fullimage = Image.new("RGB", (xEnd-xBeg, yEnd-yBeg))

        print "Stitching together the whole fractal to: mandelbrot_full.gif"

        for i in range(1,pdesc.cores+1):
            partimage = Image.open('mandel_%d.gif' % i)
            box_top = (xBeg, int((yEnd*(i-1))/pdesc.cores), xEnd ,int((yEnd*(i+1))/pdesc.cores))
            mandel_part = partimage.crop(box_top)
            fullimage.paste(mandel_part, box_top)

        fullimage.save("mandelbrot_full.gif", "GIF")

        print 'Images is now saved at the working directory..'
        session.close()

        print "Session closed, exiting now ..."
        sys.exit(0)

    except Exception as e:
            print "AN ERROR OCCURRED: %s" % ((str(e)))
            return(-1)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(main())

#
#------------------------------------------------------------------------------

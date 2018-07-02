#!/usr/bin/env python

__author__    = "George Chantzialexiou"
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
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

    You can run this code via command line (and example):

      mandelbrot_pilot_cores.py imgX imgY xBeg xEnd yBeg yEnd
      mandelbrot_pilot_cores.py 1024 1024 -2.0 +1.0 -1.5 +1.5

"""

import os
import sys
import radical.pilot as rp
import multiprocessing # this library is used to find the number of the cores.

from PIL import Image


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        sys.exit (2)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # reading the input from user:
        args = sys.argv[1:]

        if len(args) < 6:
            print "Usage: python %s imgX imgY xBeg xEnd yBeg yEnd" % __file__
            sys.exit(-1)

        imgX =   int(sys.argv[1])
        imgY =   int(sys.argv[2])
        xBeg = float(sys.argv[3])
        xEnd = float(sys.argv[4])
        yBeg = float(sys.argv[5])
        yEnd = float(sys.argv[6])

        # Add the following three lines if you want to run remote
      # c = rp.Context('ssh')
      # c.user_id = 'user_id'
      # session.add_context(c)
  
        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initiliazing Pilot Manager..."
        pmgr = rp.PilotManager(session=session)
  
        # Register our callback with our Pilot Manager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state
        pmgr.register_callback(pilot_state_cb)
  
        # This describes the requirements and the parameters
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "localhost" #  we are running on localhost
        #pdesc.resource = "xsede.stampede" #  we are running on stampede
        pdesc.runtime = 10 # minutes
        pdesc.cores   = multiprocessing.cpu_count() # we use all the cores we have
        pdesc.cleanup = True  # delete all the files that are created automatically and we don't need anymore  when the job is done
      # pdesc.project  = 'TG-MCB090174'

        print "Submitting Compute Pilot to PilotManager"
        pilot = pmgr.submit_pilots(pdesc)


        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager"
        umgr = rp.UnitManager(session=session, scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        print 'Registering the callbacks so we can keep an eye on the CUs'
        umgr.register_callback(unit_state_cb)

        print "Registering Compute Pilot with Unit Manager"
        umgr.add_pilots(pilot)

        mylist = []

        for i in range(pdesc.cores):
            output_file = 'mandel_%d.gif' % i
            cudesc = rp.ComputeUnitDescription()
            # Pre-execs to configure environment for CU on localhost
            cudesc.pre_exec = ["virtualenv ./ve", "source ./ve/bin/activate", "pip install Pillow"]
            # Pre-execs to configure environment for CU on stampede
          # cudesc.pre_exec = ["module load python", 
          #                    "python ../virtualenv-1.9/virtualenv.py ./ve", 
          #                    "source ./ve/bin/activate", "pip install Pillow"]
            cudesc.executable     = "python"
            cudesc.arguments      = ['mandel_lines.py', imgX, imgY, xBeg, xEnd, yBeg, yEnd, pdesc.cores, i]
            cudesc.input_staging  = ['mandel_lines.py']
            cudesc.output_staging = output_file
            mylist.append(cudesc)

        print 'Submitting the CU to the Unit Manager...'
        mylist_units = umgr.submit_units(mylist)

        # wait for all units to finish
        umgr.wait_units()

        print "All Compute Units completed successfully! Now.."

        # stitch together the final image
        full = Image.new("RGB", (imgX, imgY))

        print "Stitching together the whole fractal to: mandelbrot_full.gif"

        y_pixel_slice = int(imgY / pdesc.cores)

        for i in range(pdesc.cores):
            part = Image.open('mandel_%d.gif' % i)
            box  = (0, i*y_pixel_slice, imgX, (i+1)*y_pixel_slice)
            full.paste(part.crop(box), box)

        full.save("mandelbrot_full.gif", "GIF")

        print 'Images is now saved at the working directory..'


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close (cleanup=False, terminate=False)

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------


#!/usr/bin/env python

"""
.. module:: sagapilot-profiler
   :platform: Unix
   :synopsis: A simple benchmark executor for SAGA-Pilot.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import sagapilot
import logging
import optparse
import pprint
import uuid

EXECUTABLE = "/bin/sleep"
ARGUMENTS = ["0"]

# RCONF points to the resource configuration files. Read more about resource 
# configuration files at http://saga-pilot.readthedocs.org/en/latest/machconf.html
RCONF  = ["https://raw.github.com/saga-project/saga-pilot/devel/configs/xsede.json",
          "https://raw.github.com/saga-project/saga-pilot/devel/configs/futuregrid.json"]

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
        pilot.uid, state)

    if state == sagapilot.states.FAILED:
        sys.exit(1)

#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    print "[Callback]: ComputeUnit '{0}' state changed to {1}.".format(
        unit.uid, state)
    if state == sagapilot.states.FAILED:
        print "            Log: %s" % unit.log[-1]

#-----------------------------------------------------------------------------
#
def create_input_file(size):
      return "/etc/bashrc"

#-----------------------------------------------------------------------------
#
def run_benchmark(database_url, resource, pilot_runtime, pilot_size, 
                  number_of_cus, input_files_per_cu, input_file_size, detach=False):
    
    if input_files_per_cu > 0:
        ifile_path = create_input_file(input_file_size)


    try: 
        session = sagapilot.Session(database_url=database_url)
        print "\n============================================================"
        print "| Session UID for this benchmark: %s | " % session.uid 
        print "============================================================\n"

        pilot_desc = sagapilot.ComputePilotDescription()
        pilot_desc.resource = resource
        pilot_desc.cores = pilot_size
        pilot_desc.runtime = pilot_runtime

        all_cus = list()
        for cu_num in range(0, number_of_cus):
          cu_descr = sagapilot.ComputeUnitDescription()
          cu_descr.executable = EXECUTABLE
          cu_descr.arguments = ARGUMENTS
          cu_descr.cores = 1

          if input_files_per_cu > 0:
              cu_descr.input_data = list()
          for if_num in range(0, input_files_per_cu):
              cu_descr.input_data.append(ifile_path)

          all_cus.append(cu_descr)

        pilot_mgr = sagapilot.PilotManager(session=session, resource_configurations=RCONF)
        pilot_mgr.register_callback(pilot_state_cb)
        pilot = pilot_mgr.submit_pilots(pilot_desc)

        unit_mgr = sagapilot.UnitManager(session=session, scheduler=sagapilot.SCHED_DIRECT_SUBMISSION)
        unit_mgr.register_callback(unit_state_change_cb)
        unit_mgr.add_pilots(pilot)
        unit_mgr.submit_units(all_cus)

        unit_mgr.wait_units()
        pilot_mgr.cancel_pilots()

        return 0

    except Exception, ex:
        print "BENCHMARK FAILED: %s" % ex
        return 1

#-----------------------------------------------------------------------------
#
def parse_commandline():

    usage = "usage: %prog -d -s [-n]"
    parser = optparse.OptionParser(usage=usage)

    parser.add_option('--mongodb-url',
                      dest='mongodb_url',
                      help='specifies the url of the MongoDB database.')

    parser.add_option('--resource',
                      dest='resource',
                      help='describe me.')

    parser.add_option('--pilot-size',
                      dest='pilot_size',
                      help='describe me.')

    parser.add_option('--pilot-runtime',
                      dest='pilot_runtime',
                      help='describe me.')

    parser.add_option('--number-of-cus',
                      dest='number_of_cus',
                      help='describe me.')

    parser.add_option('--input-files-per-cu',
                      dest='input_files_per_cu',
                      help='describe me.')

    parser.add_option('--input-file-size',
                      dest='input_file_size',
                      help='describe me.')

    # parse 
    (options, args) = parser.parse_args()

    if options.mongodb_url is None:
        parser.error("You must define MongoDB URL (--mongodb-url). Try --help for help.")
    elif options.resource is None:
        parser.error("You must define a resource name (--resource). Try --help for help.")
    elif options.pilot_size is None:
        parser.error("You must define the pilot size (--pilot-size). Try --help for help.")
    elif options.pilot_runtime is None:
        parser.error("You must define the pilot runtime (--pilot-runtime). Try --help for help.")
    elif options.number_of_cus is None:
        parser.error("You must define the number of CumputeUnits (--number-of-cus). Try --help for help.")
    elif options.input_files_per_cu is None:
        parser.error("You must define the number of input files per ComputeUnit (--input-files-per-cu). Try --help for help.")
    elif options.input_file_size is None:
        parser.error("You must define the size of the input files (--input-file-size). Try --help for help.")

    return options

#-----------------------------------------------------------------------------
#
if __name__ == "__main__":

    options = parse_commandline()

    ret = run_benchmark(
        database_url=options.mongodb_url, 
        resource=options.resource, 
        pilot_size=int(options.pilot_size),
        pilot_runtime=int(options.pilot_runtime),
        number_of_cus=int(options.number_of_cus),
        input_files_per_cu=int(options.input_files_per_cu),
        input_file_size=int(options.input_file_size)
    )

    sys.exit(ret)

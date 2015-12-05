#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import socket
import pprint

import radical.pilot as rp



# ------------------------------------------------------------------------------
#
# RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#

# The following lines check if you are giving the input in the correct format

if len(sys.argv) < 2:

    print "You must enter an your machine's IP address as an argument."
    sys.exit(1)


elif len(sys.argv) > 2:

    print "You entered too many arguments. You only need the IP of your machine"
    sys.exit(1)

else:

    #Checking if you entered your IP address correctly
    try:

        socket.inet_aton(sys.argv[ 1 ])
        machine_ip = sys.argv[1]
        print machine_ip

    except:

        print "That's not an IP address"
        sys.exit(1)



# Onto the main function...

if __name__ == '__main__':


    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()


    # The following lines are used to dynamically configure the IP address
    # of the Amazon EC2 machines. The pprint lines are used to display the
    # dynamic changes to the configuration files

    RESOURCE = "amazon_ec2.amazon_ec2"
    template_cfg = session.get_resource_config(RESOURCE)
#   pprint.pprint(template_cfg)

    actual_cfg = rp.ResourceConfig(RESOURCE, template_cfg)
    actual_cfg.ssh['filesystem_endpoint'] = 'sftp://' + machine_ip + '/'
    actual_cfg.ssh['job_manager_endpoint'] = 'ssh://' + machine_ip + '/'

    session.add_resource_config(actual_cfg)
#   pprint.pprint(session.get_resource_config(RESOURCE))


    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        c = rp.Context("ssh")
        c.user_id = "ubuntu"
        session.add_context(c)


        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                'resource'      : "amazon_ec2.amazon_ec2",
                'cores'         : 4,   # pilot size
                'runtime'       : 15,  # pilot runtime (min)
                'exit_on_error' : True,
                'access_schema' : "ssh"
                }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.

        n = 8   # number of units to run

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/cp'
            cud.arguments = ['-v', 'input_file_%03d.txt' % i, 'output_file.txt']
            cud.input_staging = {'source' : 'input_file.txt',
                                 'target' : 'input_file_%03d.txt' % i,
                                 'action' : rp.TRANSFER}
            cud.output_staging = {'source' : 'output_file.txt',
                                  'target' : '/home/ubuntu/bin/output_file_%03d.txt' % i,
                                  'action' : rp.MOVE}
            cuds.append(cud)


        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        umgr.wait_units()

        cuds2 = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud2 = rp.ComputeUnitDescription()
            cud2.executable = '/bin/cp'
            cud2.arguments = ['-v', 'output_file_inter.txt', 'output_file_final.txt']
            cud2.input_staging = {'source' : '/home/ubuntu/bin/output_file_%03d.txt' % i,
                                 'target' : 'output_file_inter.txt',
                                 'action' : rp.MOVE}
            cud2.output_staging = {'source' : 'output_file_final.txt',
                                  'target' : 'output_file_final_%03d.txt' % i,
                                  'action' : rp.TRANSFER}
            cuds2.append(cud2)

        umgr.submit_units(cuds2)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        umgr.wait_units()         

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print 'Caught Exception: %s\n' % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print 'Exit Requested: %s\n' % e
        raise

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        session.close()




#
#-------------------------------------------------------------------------------


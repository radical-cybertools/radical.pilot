#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys


import radical.pilot as rp
import radical.utils as ru
import saga as rs
import traceback 
import saga.filesystem as rsf
import saga.filesystem.constants as constants

verbose='INFO'

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#

cur_dir = os.path.dirname(os.path.abspath(__file__))
path_to_rp_config_file = './config.json'
local_sample_data = os.path.join(cur_dir, 'sample_data')
sample_data = [
    'single_file.txt',
    'single_folder',
    'multi_folder'
    ] 

if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.LogReporter(name='radical.pilot', level=verbose)
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()


    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                'resource'      : resource,
                'runtime'       : 15,  # pilot runtime (min)
                'exit_on_error' : True,
                'project'       : config[resource]['project'],
                'queue'         : config[resource]['queue'],
                'access_schema' : config[resource]['schema'],
                'cores'         : config[resource]['cores'],
                }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)
        # access_schema = 'ssh'
        # cfg_file = ru.read_json(path_to_rp_config_file)[resource]
        # remote_dir = rs.filesystem.Directory(cfg_file[access_schema]["filesystem_endpoint"],
        #                                          session=session)

        # session_id = 'rp.session.testing.local.0000'
        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of char-counting a simple file.  We first create the
        # file right here, and then use it as unit input data for each unit.
        # os.system('hostname >  input.dat')
        # os.system('date     >> input.dat')

        # n = 10   # number of units to run
        # report.info('create %d unit description(s)\n\t' % n)

        cuds = list()

        # this is a shortcut for:
        # cud.input_staging  = {'source': 'client:///input.dat', 
        #                       'target': 'unit:///input.dat',
        #                       'action': rp.Transfer}
        

        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        
        # Test for single file 
        cud = rp.ComputeUnitDescription()
        cud.executable     = '/bin/date'
        cud.input_staging  = {'source':os.path.join(local_sample_data, sample_data[0]), 
                              'target':'unit:///%s' % sample_data[0],
                              'action': rp.TRANSFER} 
        cud.output_staging = {'source':'unit:///%s' % sample_data[0], 
                              'target': './single_file_2.txt',
                              'action': rp.TRANSFER}
        
      
        cuds.append(cud)

        # Test for single folder
        cud = rp.ComputeUnitDescription()
        cud.executable     = '/bin/date'
        cud.input_staging  = {'source':os.path.join(local_sample_data, sample_data[1]), 
                              'target':'unit:///%s' % sample_data[1],
                              'action': rp.TRANSFER,
                              'flags': [rsf.RECURSIVE]} 

        cud.output_staging = {'source':'unit:///%s' % sample_data[1], 
                              'target': './single_folder_2',
                              'action': rp.TRANSFER,
                              'flags': [rsf.RECURSIVE]}
        cuds.append(cud)

        # Test for multiple folder
        
        cud = rp.ComputeUnitDescription()
        cud.executable     = 'date'
        cud.arguments      = ['multi_folder']
        cud.input_staging  = {'source':os.path.join(local_sample_data, sample_data[2]), 
                              'target':'unit:///%s' % sample_data[2],
                              'action': rp.TRANSFER,
                              'flags': [rsf.RECURSIVE]} 

        cud.output_staging = {'source':'unit:///%s' % sample_data[2], 
                              'target': './multi_folder_2',
                              'action': rp.TRANSFER,
                              'flags': [rsf.RECURSIVE]}                

        cuds.append(cud)
        report.progress()
        # report.ok('>>ok\n')

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        
        units = umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        # report.header('gather results')
        umgr.wait_units()
      
        # # delete the sample input files
        # os.system('rm input.dat')


    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        print traceback.format_exc()
        raise

    # except (KeyboardInterrupt, SystemExit) as e:
    #     # the callback called sys.exit(), and we can here catch the
    #     # corresponding KeyboardInterrupt exception for shutdown.  We also catch
    #     # SystemExit (which gets raised if the main threads exits for some other
    #     # reason).
    #     report.warn('exit requested\n')

        


    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.

        # Verify the actionables were done for single file transfer: 

        # config_loc = '../../src/radical/pilot/configs/resource_%s.json'%resource.split('.')[0]
        # path_to_rp_config_file = os.path.realpath(os.path.join(os.getcwd(),config_loc))
        # cfg_file = ru.read_json(path_to_rp_config_file)[resource.split('.')[1]]
        # access_schema = config[resource]['schema']

        # remote_dir = rs.filesystem.Directory(cfg_file[access_schema]["filesystem_endpoint"]+units[0].sandbox,
        #                                          session=session)
        # assert sample_data[0] in [x.path for x in remote_dir.list()]

        # # Verify the actionables were done for single folder transfer: 
        # assert sample_data[1] in [x.path for x in remote_dir.list()]

        # for x in remote_dir.list():
        #     if remote_dir.is_dir(x):
        #         child_x_dir = rs.filesystem.Directory(os.path.join(unit['unit_sandbox'],x.path) ,
        #                                                 session=session)
        #         assert sample_data[0] in [cx.path for cx in child_x_dir.list()]


        # # Verify the actionables were done for multiple folder transfer: 

        # for x in remote_dir.list():
        #     if remote_dir.is_dir(x):
        #         child_x_dir = rs.filesystem.Directory(os.path.join(unit['unit_sandbox'],x.path) ,
        #                                             session=session)

        #         assert sample_data[1] in [cx.path for cx in child_x_dir.list()]

        #         for y in child_x_dir.list():
        #             if child_x_dir.is_dir(y):
        #                 gchild_x_dir= rs.filesystem.Directory(os.path.join(unit['unit_sandbox'],x.path + '/' + y.path) ,
        #                                             session=session)

        #                 assert sample_data[0] in [gcx.path for gcx in gchild_x_dir.list()]

        # report.header('finalize')
        session.close(cleanup=True) 



    report.header()


#-------------------------------------------------------------------------------


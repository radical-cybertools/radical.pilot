#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__ = 'MIT'

import os
import sys


import radical.pilot as rp
import radical.utils as ru
import saga as rs
import traceback
import saga.filesystem as rsf
import saga.filesystem.constants as constants
import re
import saga.utils.pty_shell as rsups
from glob import glob
import shutil


# ------------------------------------------------------------------------------
# Setup for all tests
resource = 'local.localhost'
cur_dir = os.path.dirname(os.path.abspath(__file__))
local_sample_data = os.path.join(cur_dir, 'sample_data')
sample_data = [
    'single_file.txt',
    'single_folder',
    'multi_folder'
]


def test_integration():

    # --------------------------------------------------------------------------
    # RP script to launch the CUs
    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # read the config used for resource details
    config = ru.read_json('%s/../../examples/config.json' 
                          % os.path.dirname(os.path.abspath(__file__)))

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Define an [n]-core local pilot that runs for [x] minutes
    # Here we use a dict to initialize the description object
    pd_init = {
        'resource': resource,
        'runtime': 15,  # pilot runtime (min)
        'exit_on_error': True,
        'project': config[resource]['project'],
        'queue': config[resource]['queue'],
        'access_schema': config[resource]['schema'],
        'cores': config[resource]['cores'],
    }
    pdesc = rp.ComputePilotDescription(pd_init)

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Register the ComputePilot in a UnitManager object.
    umgr = rp.UnitManager(session=session)
    umgr.add_pilots(pilot)

    cuds = list()

    # Test for single file
    cud = rp.ComputeUnitDescription()
    cud.executable = '/bin/date'
    cud.input_staging = {'source': os.path.join(local_sample_data, sample_data[0]),
                         'target': 'unit:///%s' % sample_data[0],
                         'action': rp.TRANSFER}
    cud.output_staging = {'source': 'unit:///%s' % sample_data[0],
                          'target': './single_file_2.txt',
                          'action': rp.TRANSFER}

    cuds.append(cud)

    # Test for single folder
    cud = rp.ComputeUnitDescription()
    cud.executable = '/bin/date'
    cud.input_staging = {'source': os.path.join(local_sample_data, sample_data[1]),
                         'target': 'unit:///%s' % sample_data[1],
                         'action': rp.TRANSFER}

    cud.output_staging = {'source': 'unit:///%s' % sample_data[1],
                          'target': './single_folder_2',
                          'action': rp.TRANSFER}
    cuds.append(cud)

    # Test for multiple folder
    cud = rp.ComputeUnitDescription()
    cud.executable = '/bin/date'
    cud.input_staging = {'source': os.path.join(local_sample_data, sample_data[2]),
                         'target': 'unit:///%s' % sample_data[2],
                         'action': rp.TRANSFER}

    cud.output_staging = {'source': 'unit:///%s' % sample_data[2],
                          'target': './multi_folder_2',
                          'action': rp.TRANSFER}

    cuds.append(cud)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(cuds)

    # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
    umgr.wait_units()
    # ------------------------------------------------------------------------------------------------------------------

    # Extract info from RP config file
    #-------------------------------------------------------------------------------------------------------------------
    config_loc = '../../src/radical/pilot/configs/resource_%s.json' % resource.split('.')[0]
    path_to_rp_config_file = os.path.realpath(os.path.join(os.getcwd(), config_loc))
    cfg_file = ru.read_json(path_to_rp_config_file)[resource.split('.')[1]]
    access_schema = config[resource].get('schema', 'ssh')

    # Verify the actionables were done for single file transfer
    # ------------------------------------------------------------------------------------------------------------------
    remote_dir = rs.filesystem.Directory(units[0].sandbox,
                                         session=session)

    # Test on remote
    assert sample_data[0] in [x.path for x in remote_dir.list()]
    # Test on client
    local_file = sample_data[0].split('.')[0] + '_2.' + sample_data[0].split('.')[1]
    assert local_file in [os.path.basename(x) for x in glob('./*')]
    # ------------------------------------------------------------------------------------------------------------------

    # Verify the actionables were done for single folder transfer
    # ------------------------------------------------------------------------------------------------------------------
    remote_dir = rs.filesystem.Directory(units[1].sandbox,
                                         session=session)
    # Test on remote
    assert sample_data[1] in [x.path for x in remote_dir.list()]
    for x in remote_dir.list():
        if remote_dir.is_dir(x):
            child_x_dir = rs.filesystem.Directory(os.path.join(units[1].sandbox, x.path),
                                                  session=session)
            assert sample_data[0] in [cx.path for cx in child_x_dir.list()]
    # Test on client
    local_folder_1 = sample_data[1] + '_2'
    assert local_folder_1 in [os.path.basename(x) for x in glob('./*')]
    assert sample_data[0] in [os.path.basename(x) for x in glob('./%s/*' % local_folder_1)]
    # ------------------------------------------------------------------------------------------------------------------

    # Verify the actionables were done for multiple folder transfer:
    # ------------------------------------------------------------------------------------------------------------------
    remote_dir = rs.filesystem.Directory(units[2].sandbox,
                                         session=session)

    # Test on remote
    assert sample_data[2] in [x.path for x in remote_dir.list()]
    for x in remote_dir.list():
        if remote_dir.is_dir(x):
            child_x_dir = rs.filesystem.Directory(os.path.join(units[2].sandbox, x.path),
                                                  session=session)

            assert sample_data[1] in [cx.path for cx in child_x_dir.list()]

            for y in child_x_dir.list():
                if child_x_dir.is_dir(y):
                    gchild_x_dir = rs.filesystem.Directory(os.path.join(units[2].sandbox, x.path + '/' + y.path),
                                                           session=session)

                    assert sample_data[0] in [gcx.path for gcx in gchild_x_dir.list()]

    # Test on client
    local_folder_2 = sample_data[2] + '_2'
    assert local_folder_2 in [os.path.basename(x) for x in glob('./*')]
    assert sample_data[1] in [os.path.basename(x) for x in glob('./%s/*' % local_folder_2)]
    assert sample_data[0] in [os.path.basename(x) for x in glob('./%s/%s/*' % (local_folder_2, sample_data[1]))]
    # ------------------------------------------------------------------------------------------------------------------

    # Cleanup - remote
    # ------------------------------------------------------------------------------------------------------------------
    remote_dir = rs.filesystem.Directory(pilot.resource_sandbox, session=session)
    remote_dir.remove(pilot.pilot_sandbox, rsf.RECURSIVE)
    # ------------------------------------------------------------------------------------------------------------------

    # Cleanup - local
    # ------------------------------------------------------------------------------------------------------------------
    os.remove('./%s' % local_file)
    shutil.rmtree('./%s' % local_folder_1)
    shutil.rmtree('./%s' % local_folder_2)
    # ------------------------------------------------------------------------------------------------------------------

    session.close(cleanup=True)


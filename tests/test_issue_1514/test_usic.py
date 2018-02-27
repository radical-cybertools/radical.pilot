import os
import shutil
import json
import radical.utils as ru
import radical.pilot as rp
import saga as rs
from radical.pilot.umgr.staging_input.default import Default
import saga.filesystem as rsf
import pytest
import re
import saga.utils.pty_shell as rsups


try:
    import mock
except ImportError:
    from unittest import mock

# User Input for test
#-----------------------------------------------------------------------------------------------------------------------
resource_name = 'local.localhost'
access_schema = 'ssh'
#-----------------------------------------------------------------------------------------------------------------------


# Extract info from RP config file
#-----------------------------------------------------------------------------------------------------------------------
config_loc = '../../src/radical/pilot/configs/resource_%s.json' % resource_name.split('.')[0]
path_to_rp_config_file = os.path.realpath(os.path.join(os.getcwd(), config_loc))
cfg_file = ru.read_json(path_to_rp_config_file)[resource_name.split('.')[1]]

# Resolve environment variables in cfg
if '$' in cfg_file['default_remote_workdir']:
    shell = rsups.PTYShell(cfg_file[access_schema]["filesystem_endpoint"])
    _, out, _ = shell.run_sync('env')

    env = dict()
    for line in out.split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            k, v = line.split('=', 1)
            env[k] = v
        except:
            pass

    test = cfg_file['default_remote_workdir']
    for k, v in env.iteritems():
        test = re.sub(r'\$%s\b' % k, v, test)

    cfg_file['default_remote_workdir'] = test
#-----------------------------------------------------------------------------------------------------------------------


# Setup for all tests
#-----------------------------------------------------------------------------------------------------------------------
# Stating session id
session_id = 'rp.session.testing.local.0000'

# Sample data to be staged -- available in cwd
cur_dir = os.path.dirname(os.path.abspath(__file__))
local_sample_data = os.path.join(cur_dir, 'sample_data')
sample_data = [
    'single_file.txt',
    'single_folder',
    'multi_folder'
]

# Get the remote sandbox path from rp config files and
# reproduce same folder structure as during execution
rp_sandbox = os.path.join(cfg_file["default_remote_workdir"], 'radical.pilot.sandbox')
session_sandbox = os.path.join(rp_sandbox, session_id)
pilot_sandbox = os.path.join(session_sandbox, 'pilot.0000')
#-----------------------------------------------------------------------------------------------------------------------


# Setup to be done for every test
#-----------------------------------------------------------------------------------------------------------------------
def setUp():

    # Add SAGA method to only create directories on remote - don't transfer yet!
    session = rs.Session()
    remote_dir = rs.filesystem.Directory(cfg_file[access_schema]["filesystem_endpoint"],
                                         session=session)

    # Unit Configuration
    unit = dict()
    unit['uid'] = 'unit.00000'
    unit['resource_sandbox'] = session_sandbox
    unit['pilot_sandbox'] = pilot_sandbox
    unit['unit_sandbox'] = os.path.join(pilot_sandbox, 'unit.00000')

    # Create unit folder on remote - don't transfer yet!
    remote_dir.make_dir(unit['unit_sandbox'], flags=rsf.CREATE_PARENTS)

    return unit, session
#-----------------------------------------------------------------------------------------------------------------------


# Cleanup any folders and files to leave the system state
# as prior to the test
#-----------------------------------------------------------------------------------------------------------------------
def tearDown(session):

    # Clean entire session directory
    remote_dir = rs.filesystem.Directory(rp_sandbox, session=session)
    remote_dir.remove(session_sandbox, rsf.RECURSIVE)
#-----------------------------------------------------------------------------------------------------------------------


# Test umgr input staging of a single file
#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(Default, '__init__', return_value=None)
@mock.patch.object(Default, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_transfer_single_file_to_unit(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    # Get a unit to transfer data + session to use for RPC
    unit, session = setUp()

    # Instantiate the USIC
    component = Default(cfg=dict(), session=session)

    # Assign expected attributes of the component
    component._fs_cache = dict()
    component._prof = mocked_profiler
    component._session = session
    component._log = ru.get_logger('dummy')

    # Create an "actionable" with a single file staging
    # for our unit
    actionables = list()
    actionables.append({
        'uid': ru.generate_id('sd'),
        'source': os.path.join(local_sample_data, sample_data[0]),
        'action': rp.TRANSFER,
        'target': 'unit:///%s' % sample_data[0],
        'flags':    [],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Peek inside the remote directory to verify
    remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                         session=session)

    # Verify the actionables were done...
    assert sample_data[0] in [x.path for x in remote_dir.list()]

    # Tear-down the files and folders
    tearDown(session)

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)
#-----------------------------------------------------------------------------------------------------------------------


# Test umgr input staging of a single folder with a file
#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(Default, '__init__', return_value=None)
@mock.patch.object(Default, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_transfer_single_folder_to_unit(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    # Get a unit to transfer data for
    unit, session = setUp()

    # Instantiate the USIC
    component = Default(cfg=dict(), session=session)

    # Assign expected attributes of the component
    component._fs_cache = dict()
    component._prof = mocked_profiler
    component._session = session
    component._log = ru.get_logger('dummy')

    # Create an "actionable" with a folder containing a file
    # staged with our unit
    actionables = list()
    actionables.append({
        'uid': ru.generate_id('sd'),
        'source': os.path.join(local_sample_data, sample_data[1]),
        'action': rp.TRANSFER,
        'target': 'unit:///%s' % sample_data[1],
        'flags': [],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Peek inside the remote directory to verify
    remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                         session=session)

    # Verify the actionables were done...
    assert sample_data[1] in [x.path for x in remote_dir.list()]

    for x in remote_dir.list():
        if remote_dir.is_dir(x):
            child_x_dir = rs.filesystem.Directory(os.path.join(unit['unit_sandbox'], x.path),
                                                  session=session)

            assert sample_data[0] in [cx.path for cx in child_x_dir.list()]

    # Tear-down the files and folders
    tearDown(session)

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)
#-----------------------------------------------------------------------------------------------------------------------


# Test umgr input staging of a folder consisting of a folder consisting of a file
#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(Default, '__init__', return_value=None)
@mock.patch.object(Default, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_transfer_multiple_folders_to_unit(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    # Get a unit to transfer data for
    unit, session = setUp()

    # Instantiate the USIC
    component = Default(cfg=dict(), session=session)

    # Assign expected attributes of the component
    component._fs_cache = dict()
    component._prof = mocked_profiler
    component._session = session
    component._log = ru.get_logger('dummy')

    # Create an "actionable" with a folder containing another folder
    # containing a file staged with our unit
    actionables = list()
    actionables.append({
        'uid': ru.generate_id('sd'),
        'source': os.path.join(local_sample_data, sample_data[2]),
        'action': rp.TRANSFER,
        'target': 'unit:///%s' % sample_data[2],
        'flags':    [],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Peek inside the remote directory to verify
    remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                         session=session)

    # Verify the actionables were done...
    assert sample_data[2] in [x.path for x in remote_dir.list()]

    for x in remote_dir.list():
        if remote_dir.is_dir(x):
            child_x_dir = rs.filesystem.Directory(os.path.join(unit['unit_sandbox'], x.path),
                                                  session=session)

            assert sample_data[1] in [cx.path for cx in child_x_dir.list()]

            for y in child_x_dir.list():
                if child_x_dir.is_dir(y):
                    gchild_x_dir = rs.filesystem.Directory(os.path.join(unit['unit_sandbox'], x.path + '/' + y.path),
                                                           session=session)

                    assert sample_data[0] in [gcx.path for gcx in gchild_x_dir.list()]

    # Tear-down the files and folders
    tearDown(session)

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)
#-----------------------------------------------------------------------------------------------------------------------

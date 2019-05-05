import os
import shutil
import json
import radical.utils as ru
import radical.pilot as rp
import radical.saga as rs
from radical.pilot.umgr.staging_output.default import Default
import radical.saga.filesystem as rsf
import pytest
from glob import glob
import re
import radical.saga.utils.pty_shell as rsups

try:
    import mock
except ImportError:
    from unittest import mock


# User Input for test
#-----------------------------------------------------------------------------------------------------------------------
resource_name = 'local.localhost'
access_schema = 'ssh'
#-----------------------------------------------------------------------------------------------------------------------


# Resolve environment variables in cfg
#-----------------------------------------------------------------------------------------------------------------------
def parse_rwd(rwd, fs_endpoint):
    if '$' not in rwd:
        return rwd

    shell = rsups.PTYShell(fs_endpoint)
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

    parsed_rwd = rwd
    for k, v in env.iteritems():
        parsed_rwd = re.sub(r'\$%s\b' % k, v, parsed_rwd)

    return parsed_rwd
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

# Set location on client to download data
tgt_loc = '/tmp/'
#-----------------------------------------------------------------------------------------------------------------------


# Setup to be done for every test
#-----------------------------------------------------------------------------------------------------------------------
def setUp():

    # Add SAGA method to only create directories on remote - don't transfer yet!
    session = rp.Session()
    # Get FS endpoint from session object
    filesystem_endpoint = session.get_resource_config(resource_name)[access_schema]["filesystem_endpoint"]
    # Get default rwd from session object and parse it
    default_rwd = parse_rwd(session.get_resource_config(resource_name)["default_remote_workdir"], filesystem_endpoint)

    # Get the remote sandbox path from rp config files and
    # reproduce same folder structure as during execution
    rp_sandbox = os.path.join(default_rwd, 'radical.pilot.sandbox')
    session_sandbox = os.path.join(rp_sandbox, session_id)
    pilot_sandbox = os.path.join(session_sandbox, 'pilot.0000')

    # Unit Configuration
    unit = dict()
    unit['uid'] = 'unit.00000'
    unit['target_state'] = 'DONE'
    unit['resource_sandbox'] = session_sandbox
    unit['pilot_sandbox'] = pilot_sandbox
    unit['unit_sandbox'] = os.path.join(pilot_sandbox, 'unit.00000')

    # Create unit folder on remote - don't transfer yet!
    remote_dir = rs.filesystem.Directory(filesystem_endpoint, session=session)
    remote_dir.make_dir(unit['unit_sandbox'], flags=rsf.CREATE_PARENTS)

    remote_dir = rs.filesystem.Directory(unit['unit_sandbox'], session=session)

    # Move all files and folders into unit sandbox
    src_file = rs.filesystem.File(os.path.join(local_sample_data, sample_data[0]), session=session)
    src_file.copy(remote_dir.url.path)

    src_dir1 = rs.filesystem.File(os.path.join(local_sample_data, sample_data[1]), session=session)
    src_dir1.copy(remote_dir.url.path, rsf.CREATE_PARENTS | rsf.RECURSIVE)

    src_dir2 = rs.filesystem.File(os.path.join(local_sample_data, sample_data[2]), session=session)
    src_dir2.copy(remote_dir.url.path, rsf.CREATE_PARENTS | rsf.RECURSIVE)

    return unit, session
#-----------------------------------------------------------------------------------------------------------------------


# Cleanup any folders and files to leave the system state
# as prior to the test
#-----------------------------------------------------------------------------------------------------------------------
def tearDown(unit, session, data):

    # Clean entire session directory
    filesystem_endpoint = session.get_resource_config(resource_name)[access_schema]["filesystem_endpoint"]
    default_rwd = parse_rwd(session.get_resource_config(resource_name)["default_remote_workdir"], filesystem_endpoint)
    rp_sandbox = os.path.join(default_rwd, 'radical.pilot.sandbox')
    remote_dir = rs.filesystem.Directory(rp_sandbox, session=session)
    remote_dir.remove(unit['resource_sandbox'], rsf.RECURSIVE)
    # Clean tmp directory -- the target on the local machine
    try:
        os.remove(tgt_loc + data)
    except:
        shutil.rmtree(tgt_loc + data)
#-----------------------------------------------------------------------------------------------------------------------


# Test umgr output staging of a single file
#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(Default, '__init__', return_value=None)
@mock.patch.object(Default, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_transfer_single_file_from_unit(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    # Get a unit to transfer data for
    unit, session = setUp()

    # Instantiate the USIC
    component = Default(cfg=dict(), session=session)

    # Assign expected attributes of the component
    component._cache = dict()
    component._prof = mocked_profiler
    component._session = session
    component._log = ru.get_logger('dummy')

    # Create an "actionable" with a single file staging
    # for our unit
    actionables = list()
    actionables.append({
        'uid': ru.generate_id('sd'),
        'source': 'unit:///%s' % sample_data[0],
        'action': rp.TRANSFER,
        'target': tgt_loc,
        'flags':    [],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Verify the actionables were done...
    assert sample_data[0] in [os.path.basename(x) for x in glob('%s/*.*' % tgt_loc)]

    # Tear-down the files and folders
    tearDown(unit=unit, session=session, data=sample_data[0])

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
def test_transfer_single_folder_from_unit(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    # Get a unit to transfer data for
    unit, session = setUp()

    # Instantiate the USIC
    component = Default(cfg=dict(), session=session)

    # Assign expected attributes of the component
    component._cache = dict()
    component._prof = mocked_profiler
    component._session = session
    component._log = ru.get_logger('dummy')

    # Create an "actionable" with a folder containing a file
    # staged with our unit
    actionables = list()
    actionables.append({
        'uid': ru.generate_id('sd'),
        'source': 'unit:///%s' % sample_data[1],
        'action': rp.TRANSFER,
        'target': tgt_loc,
        'flags':    [],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Verify the actionables were done...
    assert sample_data[1] in [os.path.basename(x) for x in glob('%s/*' % tgt_loc)]
    assert sample_data[0] in [os.path.basename(x) for x in glob('%s/%s/*' % (tgt_loc, sample_data[1]))]

    # Tear-down the files and folders
    tearDown(unit=unit, session=session, data=sample_data[1])

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
def test_transfer_multiple_folders_from_unit(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    # Get a unit to transfer data for
    unit, session = setUp()

    # Instantiate the USIC
    component = Default(cfg=dict(), session=session)

    # Assign expected attributes of the component
    component._cache = dict()
    component._prof = mocked_profiler
    component._session = session
    component._log = ru.get_logger('dummy')

    # Create an "actionable" with a folder containing another folder
    # containing a file staged with our unit
    actionables = list()
    actionables.append({
        'uid': ru.generate_id('sd'),
        'source': 'unit:///%s' % sample_data[2],
        'action': rp.TRANSFER,
        'target': tgt_loc,
        'flags':    [],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Verify the actionables were done...
    assert sample_data[2] in [os.path.basename(x) for x in glob('%s/*' % tgt_loc)]
    assert sample_data[1] in [os.path.basename(x) for x in glob('%s/%s/*' % (tgt_loc, sample_data[2]))]
    assert sample_data[0] in [os.path.basename(x) for x in glob(
        '%s/%s/%s/*.*' % (tgt_loc, sample_data[2], sample_data[1]))]

    # Tear-down the files and folders
    tearDown(unit=unit, session=session, data=sample_data[2])

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)
#-----------------------------------------------------------------------------------------------------------------------

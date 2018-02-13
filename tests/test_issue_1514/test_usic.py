import os
import shutil
import json
import radical.utils as ru
import radical.pilot as rp
import saga as rs
from radical.pilot.umgr.staging_input.default import Default
import saga.filesystem as rsf
import pytest

try:
    import mock
except ImportError:
    from unittest import mock

# Resource to test on
resource_name = 'local.localhost'
# resource_name = 'xsede.supermic_ssh'
access_schema = 'ssh'

path_to_rp_config_file = './config.json'

# path_to_rp_config_file = os.path.realpath(os.path.join(os.getcwd(),
#                                         '../../src/radical/pilot/configs/resource_%s.json'%resource_name.split('.')[0]))

cfg_file = ru.read_json(path_to_rp_config_file)[resource_name.split('.')[1]]

# Stating session information
session_id = 'rp.session.testing.local.0000'

# Staging testing folder locations
cur_dir = os.path.dirname(os.path.abspath(__file__))
local_sample_data = os.path.join(cur_dir, 'sample_data')
sample_data = [
    'single_file.txt',
    'single_folder',
    'multi_folder'
]

# Get the remote sandbox path from rp config files
rp_sandbox = os.path.join(cfg_file["default_remote_workdir"], 'radical.pilot.sandbox')
session_sandbox = os.path.join(rp_sandbox, session_id)
pilot_sandbox = os.path.join(session_sandbox, 'pilot.0000')
staging_area = os.path.join(pilot_sandbox, 'staging_area')


def setUp():

    # Add SAGA method to only create directories on remote - don't transfer yet!
    session = rs.Session()
    remote_dir = rs.filesystem.Directory(cfg_file[access_schema]["filesystem_endpoint"],
                                         session=session)

    # Create the remote directories with all the parents
    remote_dir.make_dir(staging_area, flags=rs.filesystem.CREATE_PARENTS)

    # Unit Configuration
    unit = dict()
    unit['uid'] = 'unit.00000'
    unit['resource_sandbox'] = session_sandbox
    unit['pilot_sandbox'] = pilot_sandbox
    unit['unit_sandbox'] = os.path.join(pilot_sandbox, 'unit.00000')

    # Create unit folder on remote - don't transfer yet!
    remote_dir.make_dir(unit['unit_sandbox'], flags=rsf.CREATE_PARENTS)

    return unit, session


def tearDown():

    # # Clean entire rp_sandbox directory
    try:
        shutil.rmtree(rp_sandbox)
    except Exception as ex:
        print 'Need SAGA method to delete files on remote'


@mock.patch.object(Default, '__init__', return_value=None)
@mock.patch.object(Default, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_transfer_single_file_to_unit(
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

    # Create an "actionable" with a single file staging
    # for our unit
    actionables = list()
    actionables.append({
        'uid': ru.generate_id('sd'),
        'source': os.path.join(local_sample_data, sample_data[0]),
        'action': rp.TRANSFER,
        'target': 'unit:///%s' % sample_data[0],
        'flags':    [rsf.CREATE_PARENTS],
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
    tearDown()

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)


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
        'flags':    [rsf.CREATE_PARENTS, rsf.RECURSIVE],
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

    tearDown()

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)


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
        'flags':    [rsf.CREATE_PARENTS, rsf.RECURSIVE],
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

    tearDown()

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)

import os
import shutil
import json
import radical.utils as ru
import radical.pilot as rp
import saga as rs
from radical.pilot.umgr.staging_output.default import Default
import saga.filesystem as rsf
import pytest
from glob import glob

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
tgt_loc = '/tmp/'


def setUp():

    # Add SAGA method to only create directories on remote - don't transfer yet!
    session = rs.Session()
    remote_dir = rs.filesystem.Directory(cfg_file[access_schema]["filesystem_endpoint"],
                                         session=session)

    # Unit Configuration
    unit = dict()
    unit['uid'] = 'unit.00000'
    unit['target_state'] = 'DONE'
    unit['resource_sandbox'] = session_sandbox
    unit['pilot_sandbox'] = pilot_sandbox
    unit['unit_sandbox'] = os.path.join(pilot_sandbox, 'unit.00000')

    # Create unit folder on remote - don't transfer yet!
    remote_dir.make_dir(unit['unit_sandbox'], flags=rsf.CREATE_PARENTS)

    remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                         session=session)

    # Move all files and folders into unit sandbox
    src_file = rs.filesystem.File(os.path.join(local_sample_data, sample_data[0]), session=session)
    src_file.copy(remote_dir.url.path)

    src_dir1 = rs.filesystem.File(os.path.join(local_sample_data, sample_data[1]), session=session)
    src_dir1.copy(remote_dir.url.path, rsf.CREATE_PARENTS | rsf.RECURSIVE)

    src_dir2 = rs.filesystem.File(os.path.join(local_sample_data, sample_data[2]), session=session)
    src_dir2.copy(remote_dir.url.path, rsf.CREATE_PARENTS | rsf.RECURSIVE)

    return unit, session


def tearDown(session, data):

    # Clean entire session directory
    remote_dir = rs.filesystem.Directory(rp_sandbox, session=session)
    remote_dir.remove(session_sandbox, rsf.RECURSIVE)
    # Clean tmp directory
    try:
        os.remove(tgt_loc + data)
    except:
        shutil.rmtree(tgt_loc + data)


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
        'flags':    [rsf.CREATE_PARENTS],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Verify the actionables were done...
    assert sample_data[0] in [os.path.basename(x) for x in glob('/tmp/*.*')]

    # Tear-down the files and folders
    tearDown(session=session, data=sample_data[0])

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)


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
        'flags':    [rsf.CREATE_PARENTS, rsf.RECURSIVE],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Verify the actionables were done...
    assert sample_data[1] in [os.path.basename(x) for x in glob('/tmp/*')]
    assert sample_data[0] in [os.path.basename(x) for x in glob('/tmp/%s/*' % sample_data[1])]

    tearDown(session=session, data=sample_data[1])

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)


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
        'flags':    [rsf.CREATE_PARENTS, rsf.RECURSIVE],
        'priority': 0
    })

    # Call the component's '_handle_unit' function
    # Should perform all of the actionables in order
    component._handle_unit(unit, actionables)

    # Verify the actionables were done...
    assert sample_data[2] in [os.path.basename(x) for x in glob('/tmp/*')]
    assert sample_data[1] in [os.path.basename(x) for x in glob('/tmp/%s/*' % sample_data[2])]
    assert sample_data[0] in [os.path.basename(x) for x in glob('/tmp/%s/%s/*.*' % (sample_data[2], sample_data[1]))]

    tearDown(session=session, data=sample_data[2])

    # Verify tearDown
    with pytest.raises(rs.BadParameter):
        remote_dir = rs.filesystem.Directory(unit['unit_sandbox'],
                                             session=session)

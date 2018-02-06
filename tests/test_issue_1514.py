import os
import re
import shutil
import errno
import json
import random
import string
import unittest
import radical.utils as ru
import radical.pilot as rp
import saga as rs
from radical.pilot.umgr.staging_input.default import Default
import json


try: 
    import mock 
except ImportError: 
    from unittest import mock

# Transfer file or folder using SAGA
def transfer(src, dest, cfg_file, access_schema):
    # TODO: SAGA methods to transfer file of folder
    ctx = saga.Context("ssh")

    session = saga.Session()
    session.add_context(ctx)

# Resource to test on
resource_name = 'xsede.supermic'
access_schema = 'ssh'
path_to_rp_config_file = '../src/radical/pilot/configs/resource_%s.json'%resource_name.split('.')[0]
cfg_file = json.load(path_to_rp_config_file)[resource_name]

# Stating session information
session_id = 'rp.session.testing.local.0000'

# Staging testing folder locations
cur_dir = os.path.dirname(os.path.abspath(__file__))
local_sample_data = os.path.join(cur_dir, 'staging-testing-sandbox/sample-data')
sample_data = [
                    'single-file',
                    'single-folder',
                    'multi-folder'
                ]

# Get the remote sandbox path from rp config files
session_sandbox = os.path.join(cfg_file["default_remote_workdir"] + 'radical.pilot.sandbox', session_id)
pilot_sandbox =  os.path.join(session_sandbox, 'pilot.0000')
staging_area = os.path.join(pilot_sandbox, 'staging_area')


class Test_UMGR_Staging_Input_Component(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Recursively create sandbox, session and pilot folders
        os.makedirs(workdir)

    @classmethod
    def tearDownClass(cls):
        # Delete all test staging directories
        shutil.rmtree(session_sandbox)

    def setUp(self):                

        # Add SAGA method to only create directories on remote - don't transfer yet!       
        session = rs.Session()
        remote_dir = rs.filesystem.Directory(cfg_file[access_schema]["filesystem_endpoint"],
                                            session=session)

        # Create the remote directories with all the parents
        remote_dir.make_dir(session_sandbox, flags=rs.filesystem.CREATE_PARENTS)
        remote_dir.make_dir(pilot_sandbox, flags=rs.filesystem.CREATE_PARENTS)
        remote_dir.make_dir(staging_area, flags=rs.filesystem.CREATE_PARENTS)


        # # Copy sample data
        # for data in sample_data:
        #     transfer(   src = os.path.join(sample_data_folder, data), 
        #                 dest = os.path.join(staging_area, data),
        #                 cfg_file = cfg_file,
        #                 access_schema = access_schema)

        self.cfg = dict()
        self.cfg["cname"] = "UMGRStagingInputComponent"
        self.cfg["dburl"] = "mongodb://rp:rp@ds015335.mlab.com:15335/rp"
        self.cfg["pilot_id"] = "pilot.0000"
        "resource_cfg": {
    "default_remote_workdir": "$HOME",
    "filesystem_endpoint": "file://localhost/",
    "job_manager_endpoint": "fork://localhost/"
  },
  "resource_sandbox": "",
  "session_id": "",
  "session_sandbox": "",
  "staging_area": "staging_area",
  "staging_schema": "staging",
  "target": "local",
  "uid": "agent_0",
"workdir": ""



        self.cfg['session_id'] = session_id
        self.cfg['resource_sandbox'] = cfg_file["default_remote_workdir"]
        self.cfg['session_sandbox'] = session_sandbox
        self.cfg['pilot_sandbox'] = pilot_sandbox
        self.cfg['workdir'] = pilot_sandbox

        # Unit Configuration
        self.unit = dict()
        self.unit['uid'] = 'unit.00000'
        self.unit['resource_sandbox'] = session_sandbox # The key in the unit 
                                                        # directory should 
                                                        # probably read as 
                                                        # session_sandbox
        self.unit['pilot_sandbox'] = pilot_sandbox       
        self.unit['unit_sandbox'] = os.path.join(pilot_sandbox, 'unit.00000')
        
        # Unit output directory
        self.unit_directory = os.path.join(pilot_sandbox, self.unit['uid'])

        # Pilot staging directory
        self.pilot_directory = staging_area

        # Create unit folder
        remote_dir.make_dir(self.unit_directory, 
                            flags=rs.filesystem.CREATE_PARENTS)

    def tearDown(self):
        
        # Clean output directory
        shutil.rmtree(self.unit_directory)

        # Clean staging_area
        shutil.rmtree(os.path.join(workdir, 'staging_area'))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file(  self, 
                                mocked_init, 
                                mocked_method, 
                                mocked_profiler, 
                                mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///new-single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///new-single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.exists(os.path.join(self.pilot_directory, 'single-file'))) 
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///new-single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.exists(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_folder(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.COPY,
            'target': 'unit:///single-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_folder(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.LINK,
            'target': 'unit:///single-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_folder(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.MOVE,
            'target': 'unit:///single-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_folder_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.COPY,
            'target': 'unit:///new-single-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_folder_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.LINK,
            'target': 'unit:///new-single-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_folder_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.MOVE,
            'target': 'unit:///new-single-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/file-2')))

    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_folder_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.COPY,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_folder_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.LINK,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        

        self.assertTrue(os.path.exists(os.path.join(self.pilot_directory, 'single-folder')))
        self.assertTrue(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_folder_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.MOVE,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_to_directory(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///new-single-folder/single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_to_directory(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///new-single-folder/single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_directory, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_to_directory(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///new-single-folder/single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_to_directory_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///new-single-folder/new-single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_to_directory_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///new-single-folder/new-single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_directory, 'new-single-folder/new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_to_directory_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///new-single-folder/new-single-file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_to_directory_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///new-single-folder/',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_to_directory_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///new-single-folder/',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_directory, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_to_directory_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///new-single-folder/',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_multi_folder(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.COPY,
            'target': 'unit:///multi-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_multi_folder(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.LINK,
            'target': 'unit:///multi-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_multi_folder(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.MOVE,
            'target': 'unit:///multi-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_multi_folder_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.COPY,
            'target': 'unit:///new-multi-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.exists(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-2/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_multi_folder_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.LINK,
            'target': 'unit:///new-multi-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_directory, 'new-multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-2/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_multi_folder_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.MOVE,
            'target': 'unit:///new-multi-folder',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.unit_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'new-multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'new-multi-folder/folder-2/file-2')))

    
    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_multi_folder_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.COPY,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_multi_folder_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.LINK,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_multi_folder_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///multi-folder',
            'action': rp.MOVE,
            'target': '',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertFalse(os.path.exists(os.path.join(self.pilot_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_directory, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'multi-folder/folder-2/file-2')))


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStagingInputComponent)
    unittest.TextTestRunner(verbosity=2).run(suite)
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
local_sample_data = os.path.join(cur_dir, 'sample-data')
sample_data = [
                    'single_file.txt',
                    'single_folder',
                    'multi_folder'
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
        self.session = rs.Session()
        remote_dir = rs.filesystem.Directory(cfg_file[access_schema]["filesystem_endpoint"],
                                            session=self.session)

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
        self.cfg["resource_cfg"] =  {
                                        "default_remote_workdir": cfg_file[access_schema]["default_remote_workdir"],
                                        "filesystem_endpoint": cfg_file[access_schema]["filesystem_endpoint"],
                                        "job_manager_endpoint": cfg_file[access_schema]["job_manager_endpoint"]
                                    }
        self.cfg["staging_area"]  = "staging_area"
        self.cfg["target"] = "remote"
        self.cfg["uid"]  = "agent_0"

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
    def test_transfer_single_file_to_unit(  self, 
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
                                'source': os.path.join(local_sample_data, sample_data[0]),
                                'action': rp.TRANSFER,
                                'target': 'unit:///%s'%sample_data[0],
                                'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
                                'priority': 0
                            })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...      
        remote_dir = saga.filesystem.Directory(self.unit_directory,
                                                session=self.session)

        assert sample_data[0] in remote_dir.list()

    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_transfer_single_folder_to_unit(self, 
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
                                'source': os.path.join(local_sample_data, sample_data[1]),
                                'action': rp.TRANSFER,
                                'target': 'unit:///%s'%sample_data[1],
                                'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
                                'priority': 0
                            })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...      
        remote_dir = saga.filesystem.Directory(self.unit_directory,
                                                session=self.session)

        assert sample_data[1] in remote_dir.list()


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_transfer_multiple_folders_to_unit(self, 
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
                                'source': os.path.join(local_sample_data, sample_data[2]),
                                'action': rp.TRANSFER,
                                'target': 'unit:///%s'%sample_data[2],
                                'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
                                'priority': 0
                            })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...      
        remote_dir = saga.filesystem.Directory(self.unit_directory,
                                                session=self.session)

        assert sample_data[2] in remote_dir.list()


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_transfer_single_file_to_staging_area(  self, 
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
                                'source': os.path.join(local_sample_data, sample_data[0]),
                                'action': rp.TRANSFER,
                                'target': os.path.join(staging_area, sample_data[0]),
                                'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
                                'priority': 0
                            })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...      
        remote_dir = saga.filesystem.Directory(self.staging_area,
                                                session=self.session)

        assert sample_data[0] in remote_dir.list()


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_transfer_single_folder_to_staging_area(self, 
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
                                'source': os.path.join(local_sample_data, sample_data[1]),
                                'action': rp.TRANSFER,
                                'target': os.path.join(staging_area, sample_data[1]),
                                'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
                                'priority': 0
                            })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...      
        remote_dir = saga.filesystem.Directory(self.staging_area,
                                                session=self.session)

        assert sample_data[1] in remote_dir.list()

    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_transfer_multiple_folders_to_staging_area(self, 
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
                                'source': os.path.join(local_sample_data, sample_data[2]),
                                'action': rp.TRANSFER,
                                'target': os.path.join(staging_area, sample_data[2]),
                                'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
                                'priority': 0
                            })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...      
        remote_dir = saga.filesystem.Directory(self.staging_area,
                                                session=self.session)

        assert sample_data[2] in remote_dir.list()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(Test_UMGR_Staging_Input_Component)
    unittest.TextTestRunner(verbosity=2).run(suite)
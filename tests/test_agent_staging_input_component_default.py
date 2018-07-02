
import os
import shutil
import errno
import json
import random
import string
import unittest
import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.staging_input.default import Default

try: 
    import mock 
except ImportError: 
    from unittest import mock

# Copy file or folder
def copy(src, dest):
    try:
        shutil.copytree(src, dest)
    except OSError as e:
        # If the error was caused because the source wasn't a directory
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            print('Directory not copied. Error: %s' % e)

# Stating session information
session_id = 'rp.session.testing.local.0000'

# Staging testing folder locations
directory          = os.path.dirname(os.path.abspath(__file__))
resource_sandbox   = '%s/%s' % (directory, 'staging-testing-sandbox')
session_sandbox    = '%s/%s' % (resource_sandbox, session_id)
pilot_sandbox      = session_sandbox
workdir            = '%s/%s' % (pilot_sandbox, 'pilot.0000')

# Sample data & sample empty configuration
sample_data_folder = '%s/%s' % (resource_sandbox, 'sample-data')
cfg_file           = '%s/%s' % (sample_data_folder, 'sample_configuration_staging_input.json')
sample_data = [
    'single-file',
    'single-folder',
    'multi-folder'
]

class TestStagingInputComponent(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Recursively create sandbox, session and pilot folders
        os.makedirs(workdir)

    @classmethod
    def tearDownClass(cls):
        # Delete all test staging directories
        shutil.rmtree(session_sandbox)

    def setUp(self):
                
        # Recursively create sandbox and staging folders
        os.makedirs(os.path.join(workdir, 'staging_area'))

        # Copy sample data
        for data in sample_data:
            copy('%s/%s'    % (sample_data_folder, data), 
                 '%s/%s/%s' % (workdir, 'staging_area', data))

        # Component configuration
        with open(cfg_file) as fp:
            self.cfg = json.load(fp)
        self.cfg['session_id'] = session_id
        self.cfg['resource_sandbox'] = resource_sandbox
        self.cfg['session_sandbox'] = session_sandbox
        self.cfg['pilot_sandbox'] = pilot_sandbox
        self.cfg['workdir'] = workdir

        # Unit Configuration
        self.unit = dict()
        self.unit['uid'] = 'unit.00000'
        self.unit['unit_sandbox'] = os.path.join(self.cfg['workdir'], 'unit.00000')
        self.unit['pilot_sandbox'] = self.cfg['workdir']
        self.unit['resource_sandbox'] = self.cfg['resource_sandbox']

        # Unit output directory
        self.unit_sandbox = os.path.join(workdir, self.unit['uid'])

        # Pilot staging directory
        self.pilot_sandbox = os.path.join(workdir, 'staging_area')

        # Some other output directory
        self.output_directory = os.path.join(workdir, 'output_directory')

        # Create unit folder
        os.makedirs(self.unit_sandbox)

        # Create the other output directory
        os.makedirs(self.output_directory)

    def tearDown(self):
        
        # Clean unit output directory
        shutil.rmtree(self.unit_sandbox)

        # Clean staging_area
        shutil.rmtree(os.path.join(workdir, 'staging_area'))

        # Clean other output directory
        shutil.rmtree(self.output_directory)


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file'))) 
        self.assertTrue(os.path.islink(os.path.join(self.unit_sandbox, 'single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file'))) 
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/file-2')))

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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        

        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///new-single-folder/single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///new-single-folder/single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_sandbox, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///new-single-folder/single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///new-single-folder/new-single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///new-single-folder/new-single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_sandbox, 'new-single-folder/new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///new-single-folder/new-single-file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': 'unit:///new-single-folder/',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': 'unit:///new-single-folder/',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_sandbox, 'new-single-folder/single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_to_sandbox(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': 'unit:///new-single-folder/',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-single-folder/single-file')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_sandbox, 'new-multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.unit_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'new-multi-folder/folder-2/file-2')))

    
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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.islink(os.path.join(self.unit_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-2')))


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
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-1')))
        self.assertTrue(os.path.isdir(os.path.join(self.unit_sandbox, 'multi-folder/folder-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-1/file-2')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'multi-folder/folder-2/file-2')))


    ######################################################################
    ######################################################################
    # Test a different destination (other than the unit:// folder)
    # using self.output_directory
    ######################################################################
    ######################################################################


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_other_dir(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': os.path.join(self.output_directory, 'single-file'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_other_dir(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': os.path.join(self.output_directory, 'single-file'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file'))) 
        self.assertTrue(os.path.islink(os.path.join(self.output_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_other_dir(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': os.path.join(self.output_directory, 'single-file'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_other_dir_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': os.path.join(self.output_directory, 'new-single-file'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_other_dir_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': os.path.join(self.output_directory, 'new-single-file'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file'))) 
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_other_dir_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': os.path.join(self.output_directory, 'new-single-file'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_file_other_dir_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.COPY,
            'target': self.output_directory,
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_file_other_dir_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.LINK,
            'target': self.output_directory,
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_file_other_dir_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-file',
            'action': rp.MOVE,
            'target': self.output_directory,
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-file')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-file')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_folder_other_dir(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.COPY,
            'target': os.path.join(self.output_directory, 'single-folder'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_folder_other_dir(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.LINK,
            'target': os.path.join(self.output_directory, 'single-folder'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_folder_other_dir(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.MOVE,
            'target': os.path.join(self.output_directory, 'single-folder'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_folder_other_dir_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.COPY,
            'target': os.path.join(self.output_directory, 'new-single-folder'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_folder_other_dir_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.LINK,
            'target': os.path.join(self.output_directory, 'new-single-folder'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_folder_other_dir_rename(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.MOVE,
            'target': os.path.join(self.output_directory, 'new-single-folder'),
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'new-single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'new-single-folder/file-2')))

    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_copy_single_folder_other_dir_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.COPY,
            'target': self.output_directory,
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_link_single_folder_other_dir_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.LINK,
            'target': self.output_directory,
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        

        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertTrue(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-2')))


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_move_single_folder_other_dir_noname(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        actionables = list()
        actionables.append({
            'uid'   : ru.generate_id('sd'),
            'source': 'pilot:///single-folder',
            'action': rp.MOVE,
            'target': self.output_directory,
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0
        })
        
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables in order
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...        
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-1')))
        self.assertFalse(os.path.exists(os.path.join(self.pilot_sandbox, 'single-folder/file-2')))
        self.assertTrue(os.path.isdir(os.path.join(self.output_directory, 'single-folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-1')))
        self.assertTrue(os.path.isfile(os.path.join(self.output_directory, 'single-folder/file-2')))


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStagingInputComponent)
    unittest.TextTestRunner(verbosity=2).run(suite)

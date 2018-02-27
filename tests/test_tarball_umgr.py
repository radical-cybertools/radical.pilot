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
from radical.pilot.umgr.staging_input.default import Default
import saga as rs
import glob

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
directory = os.path.dirname(os.path.abspath(__file__))
resource_sandbox = os.path.join(directory, 'staging-testing-sandbox')
session_sandbox = os.path.join(resource_sandbox, session_id)
pilot_sandbox = os.path.join(session_sandbox, 'pilot.0000')
unit_sandbox = os.path.join(pilot_sandbox, 'unit.000000')
workdir = unit_sandbox

# Sample data & sample empty configuration
sample_data_folder = os.path.join(resource_sandbox, 'sample-data')
cfg_file = os.path.join(sample_data_folder, 'sample_configuration_staging_input_umgr.json')
sample_data = [
    #'unit.000000.tar'
    'file']

class TestStagingInputComponent(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
        # Recursively create sandbox, session and pilot folders
        #os.makedirs(workdir)

    @classmethod
    def tearDownClass(cls):
        # Delete all test staging directories
        shutil.rmtree(session_sandbox)

    def setUp(self):

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
        self.unit['uid'] = 'unit.000000'
        self.unit['unit_sandbox'] = os.path.join(self.cfg['workdir'])
        self.unit['pilot_sandbox'] = self.cfg['workdir']
        self.unit['resource_sandbox'] = self.cfg['resource_sandbox']

        # Unit output directory
        self.unit_directory = os.path.join(workdir)

        # Pilot staging directory
        self.pilot_directory = pilot_sandbox  #os.path.join(workdir, 'staging_area')

        # Some other output directory
        self.output_directory = unit_sandbox #os.path.join(workdir, 'output_directory')

        # Create unit folder
        os.makedirs(self.unit_directory)

        # Create the other output directory
        #os.makedirs(self.output_directory)

        shutil.copy("staging-testing-sandbox/sample-data/unit.000000.tar",os.path.join(workdir))

    def tearDown(self):
        
        # Clean unit output directory
        shutil.rmtree(self.unit_directory)

        # Clean staging_area
        #shutil.rmtree(os.path.join(workdir, 'staging_area'))

        # Clean other output directory
        #shutil.rmtree(self.output_directory)

    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    @mock.patch.object(rs.filesystem.Directory, '__init__') # mock init
    @mock.patch.object(rs.filesystem.Directory, 'make_dir') # mock make_dir
    @mock.patch.object(rs.filesystem.Directory, 'copy') # mock copy
    #@mock.patch.object(Default, 'register_input')
    #@mock.patch.object(Default, 'register_output')
    #@mock.patch.object(Default, 'register_subscriber')
    #@mock.patch.object(Default, '_fs_cache')
    #fs_cache=dict()
    #js_cache=dict()
    #pilots  =dict()

    #include all mocked things in order
    def test_tarball(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on, mocked_saga_dir_init, mocked_make_dir, mocked_copy):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        #component.initialize_child()
        component._fs_cache = dict()
        component._js_cache = dict()
        component._pilots = dict()
        actionables = list()
        actionables.append({
                            'uid'   : ru.generate_id('sd'),
                            'source': 'client:///file',
                            'action': rp.TARBALL,
                            'target': 'unit:///file',
                            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
                            'priority': 0
                                     })
        
        #print "unit_context", glob.glob(unit_sandbox+'/*')
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables 
        component._handle_unit(self.unit, actionables)
        print "unit_context", glob.glob(unit_sandbox+'/*')
        # Verify the actionables were done...
        self.assertTrue('rp.TARBALL' in actionables)
        #self.assertTrue(os.path.isfile(os.path.join(self.unit_directory, 'file')))
                                                                                                     

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStagingInputComponent)
    unittest.TextTestRunner(verbosity=2).run(suite)

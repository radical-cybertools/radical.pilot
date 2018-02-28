import os
import re
import shutil
import errno
import json
import random
import string
import unittest
import glob

import radical.utils as ru
import radical.pilot as rp
from radical.pilot.umgr.staging_input.default import Default
import saga as rs

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

directory = os.path.dirname(os.path.abspath(__file__))

resource_sandbox = os.path.join(directory, 'staging-testing-sandbox')
session_sandbox = os.path.join(resource_sandbox, session_id)
pilot_sandbox = os.path.join(session_sandbox, 'pilot.0000')
unit_sandbox = os.path.join(pilot_sandbox, 'unit.000000')


# Sample data & sample empty configuration
sample_data_folder = os.path.join(resource_sandbox, 'sample-data')
cfg_file = os.path.join(
    sample_data_folder, 'sample_configuration_staging_input_umgr.json')
sample_data = ['file']


class TestStagingInputComponent(unittest.TestCase):



    def setUp(self):

        # Component configuration
        with open(cfg_file) as fp:
            self.cfg = json.load(fp)
        self.cfg['session_id'] = session_id
        self.cfg['resource_sandbox'] = resource_sandbox
        self.cfg['session_sandbox'] = session_sandbox
        self.cfg['pilot_sandbox'] = pilot_sandbox
        self.cfg['unit_sandbox'] = unit_sandbox


        # Unit Configuration
        self.unit = dict()
        self.unit['uid'] = 'unit.000000'
        self.unit['unit_sandbox'] = os.path.join(self.cfg['unit_sandbox'])
        self.unit['pilot_sandbox'] = self.cfg['pilot_sandbox']
        self.unit['resource_sandbox'] = self.cfg['resource_sandbox']
        self.unit['description'] = {'input_staging': [{
            'uid': ru.generate_id('sd'),
            'source': 'client:///' + sample_data_folder + '/file',
            'action': rp.TARBALL,
            'target': 'unit:///file',
            'flags':    [rp.CREATE_PARENTS, rp.SKIP_FAILED],
            'priority': 0
        }]
        }

    

    def tearDown(self):

        # Clean unit output directory
        os.remove(self.unit['uid'] + '.tar')

    
    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    @mock.patch.object(rs.filesystem.Directory, 'make_dir')  # mock make_dir
    @mock.patch.object(rs.filesystem.Directory, 'copy')  # mock copy
    @mock.patch.object(os,'remove')
    # include all mocked things in order
    def test_tarball(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on, mocked_make_dir, mocked_copy,mocked_remove):
        component = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log = ru.get_logger('dummy')
        component._session = None
        component._fs_cache = dict()
        component._js_cache = dict()
        component._pilots = dict()
        actionables = list()
        tar_file = None
        actionables.append(self.unit['description']['input_staging'][0])

        # print "unit_context", glob.glob(unit_sandbox+'/*')
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables
        component._handle_unit(self.unit, actionables)
        # Verify the actionables were done...
        self.assertTrue(os.path.isfile(os.path.join(os.getcwd(), self.unit['uid'] + '.tar')))


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStagingInputComponent)
    unittest.TextTestRunner(verbosity=2).run(suite)


import os
import errno
import unittest

import radical.utils as ru
import radical.pilot as rp
import saga          as rs

from   radical.pilot.umgr.staging_input.default import Default


try:
    import mock
except ImportError:
    from unittest import mock


# Stating session information
session_id = 'rp.session.testing.local.0000'
directory  = os.path.dirname(os.path.abspath(__file__))
pwd        = os.getcwd()
reldir     = directory
if directory.startswith(pwd):
    reldir = directory[len(pwd)+1:]

resource_sandbox   = os.path.join(directory, 'staging-testing-sandbox')
session_sandbox    = os.path.join(resource_sandbox, session_id)
pilot_sandbox      = os.path.join(session_sandbox, 'pilot.0000')
unit_sandbox       = os.path.join(pilot_sandbox, 'unit.000000')
sample_data_folder = '%s/staging-testing-sandbox/sample-data' % reldir

# Sample data & sample empty configuration
cfg_file    = '%s/%s/sample-data/sample_configuration_staging_input_umgr.json' \
            % (reldir, 'staging-testing-sandbox')
sample_data = ['file']


class TestStagingInputComponent(unittest.TestCase):

    def setUp(self):

        # Component configuration
        self.cfg = ru.read_json(cfg_file)
        self.cfg['session_id']       = session_id
        self.cfg['resource_sandbox'] = resource_sandbox
        self.cfg['session_sandbox']  = session_sandbox
        self.cfg['pilot_sandbox']    = pilot_sandbox
        self.cfg['unit_sandbox']     = unit_sandbox


        # Unit Configuration
        self.unit = dict()
        self.unit['uid']              = 'unit.000000'
        self.unit['unit_sandbox']     = self.cfg['unit_sandbox']
        self.unit['pilot_sandbox']    = self.cfg['pilot_sandbox']
        self.unit['resource_sandbox'] = self.cfg['resource_sandbox']
        self.unit['description']      = {'input_staging': [{
            'uid':    ru.generate_id('sd'),
            'source': 'client://' + sample_data_folder + '/file',
            'action': rp.TARBALL,
            'target': 'unit:///file',
            'flags':  rp.DEFAULT_FLAGS,
            'priority': 0}]
        }

    
    def tearDown(self):

        # Clean unit output directory
        print '%s/%s.tar' % (self.unit['unit_sandbox'], self.unit['uid'])
        os.remove('%s/%s.tar' % (self.unit['unit_sandbox'], self.unit['uid']))

    
    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    # include all mocked things in order
    def test_tarball(self, mocked_init, mocked_method, mocked_profiler):

        component           = Default(cfg=self.cfg, session=None)
        component._prof     = mocked_profiler
        component._log      = ru.get_logger('dummy')
        component._session  = None
        component._fs_cache = dict()
        component._js_cache = dict()
        component._pilots   = dict()
        actionables         = list()
        tar_file            = None
        
        actionables.append(self.unit['description']['input_staging'][0])

        # print "unit_context", glob.glob(unit_sandbox+'/*')
        # Call the component's '_handle_unit' function
        # Should perform all of the actionables
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isfile('%s/%s.tar' % (self.unit['unit_sandbox'],
                                                      self.unit['uid'])))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    suite = unittest.TestLoader().loadTestsFromTestCase(TestStagingInputComponent)
    unittest.TextTestRunner(verbosity=2).run(suite)

# ------------------------------------------------------------------------------


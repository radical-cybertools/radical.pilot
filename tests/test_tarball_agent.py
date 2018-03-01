
import os
import glob
import json
import shutil
import unittest

import radical.utils as ru
import radical.pilot as rp

from radical.pilot.agent.staging_input.default import Default


try: 
    import mock 
except ImportError: 
    from unittest import mock

# Stating session information
session_id = 'rp.session.testing.local.0000'

# Staging testing folder locations
session_id = 'rp.session.testing.local.0000'
directory  = os.path.dirname(os.path.abspath(__file__))
pwd        = os.getcwd()
reldir     = directory
if directory.startswith(pwd):
    reldir = directory[len(pwd)+1:]

resource_sandbox   = os.path.join(directory,        'staging-testing-sandbox')
session_sandbox    = os.path.join(resource_sandbox, session_id)
pilot_sandbox      = os.path.join(session_sandbox,  'pilot.0000')
unit_sandbox       = os.path.join(pilot_sandbox,    'unit.000000')
sample_data_folder = '%s/staging-testing-sandbox/sample-data' % reldir

# Sample data & sample empty configuration
cfg_file    = '%s/%s/sample-data/sample_configuration_staging_input_umgr.json' \
            % (reldir, 'staging-testing-sandbox')
sample_data = ['unit.000000.tar']


# ------------------------------------------------------------------------------
#
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
        self.unit['pilot_sandbox']    = self.cfg['unit_sandbox']
        self.unit['resource_sandbox'] = self.cfg['resource_sandbox']

        self.session_sandbox = session_sandbox
        self.pilot_sandbox   = pilot_sandbox
        self.unit_sandbox    = unit_sandbox

        # Create unit folder
        os.makedirs(self.unit_sandbox)
        shutil.copy("%s/unit.000000.tar" % sample_data_folder, unit_sandbox)


    def tearDown(self):
        
        shutil.rmtree(self.session_sandbox)


    @mock.patch.object(Default, '__init__', return_value=None)
    @mock.patch.object(Default, 'advance')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_tarball(self, mocked_init, mocked_method, mocked_profiler, mocked_raise_on):

        component       = Default(cfg=self.cfg, session=None)
        component._prof = mocked_profiler
        component._log  = ru.get_logger('dummy')

        actionables = list()
        actionables.append({'uid'     : ru.generate_id('sd'),
                            'action'  : rp.TARBALL,
                            'flags'   : rp.DEFAULT_FLAGS,
                            'source'  : 'client:///wrongthing',
                            'target'  : 'unit:///unit.000000.tar',
                            'priority': 0
                           })

        # Call the component's '_handle_unit' function
        # Should perform all of the actionables 
        component._handle_unit(self.unit, actionables)

        # Verify the actionables were done...
        self.assertTrue(os.path.isfile(os.path.join(self.unit_sandbox, 'file')))
                                                                                                     
# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    suite = unittest.TestLoader().loadTestsFromTestCase(TestStagingInputComponent)
    unittest.TextTestRunner(verbosity=2).run(suite)

# ------------------------------------------------------------------------------


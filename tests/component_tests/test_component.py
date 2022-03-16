# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2020-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob
import os

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.utils.component import Component, ComponentManager


# ------------------------------------------------------------------------------
#
class TestComponent(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Component, '__init__', return_value=None)
    def test_output(self, mocked_init):

        component = Component(None, None)

        component._outputs = {'test_state': []}

        with self.assertRaises(ValueError):
            component.output(things='test_thing')

        with self.assertRaises(ValueError):
            component.output(things='test_thing', state='test_state2')

        component.output(things=None,   state='test_state')
        component.output(things=1,      state='test_state')
        component.output(things=[1, 2], state='test_state')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ComponentManager, '__init__', return_value=None)
    @mock.patch('radical.utils.sh_callout', return_value=('', '', 0))
    def test_cm_start_components(self, mocked_sh_callout, mocked_init):

        cfg = {
            'path'      : '/tmp',
            'heartbeat' : {'timeout': 10},
            'bridges'   : {},
            'components': {
                'update': {'count': 1}
            }
        }

        cm = ComponentManager(None)
        cm._uids = []
        cm._uid  = 'cm.0000'
        cm._sid  = 'session.0000'
        cm._cfg  = ru.Config(cfg=cfg)
        cm._log  = cm._prof = cm._hb = mock.Mock()
        cm._hb.wait_startup = mock.Mock(return_value=0)

        cm.start_components()

        for cname, ccfg in cfg['components'].items():
            for fname in glob.glob('%s/%s*.json' % (cfg['path'], cname)):
                self.assertTrue(os.path.exists(fname))

                ccfg_saved = ru.Config(path=fname)
                self.assertEqual(ccfg_saved.cmgr,  cm._uid)
                self.assertEqual(ccfg_saved.kind,  cname)
                self.assertEqual(ccfg_saved.count, ccfg['count'])

                os.unlink(fname)


if __name__ == '__main__':

    tc = TestComponent()
    tc.test_output()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

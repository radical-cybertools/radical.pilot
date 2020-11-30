
# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

__copyright__ = "Copyright 2020, http://radical.rutgers.edu"
__license__   = "MIT"

from unittest import TestCase

import os
import radical.utils as ru
import radical.pilot as rp

from   radical.pilot.utils.component import Component


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
class TestComponent(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Component, '__init__', return_value=None)
    def test_output(self, mocked_init):

        component = Component(None, None)

        component._outputs = {'test_state':[]}

        with self.assertRaises(ValueError):
            component.output(things='test_thing')

        with self.assertRaises(ValueError):
            component.output(things='test_thing', state='test_state2')

        component.output(things=None,  state='test_state')
        component.output(things=1,     state='test_state')
        component.output(things=[1,2], state='test_state')


# ------------------------------------------------------------------------------


# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = "Copyright 2020, http://radical.rutgers.edu"
__license__   = "MIT"

from unittest import TestCase
from unittest import mock

from radical.pilot.utils.component import Component


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


if __name__ == '__main__':

    tc = TestComponent()
    tc.test_output()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

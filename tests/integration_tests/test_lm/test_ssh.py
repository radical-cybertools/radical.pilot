# pylint: disable=protected-access, unused-argument, no-value-for-parameter
import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.ssh import SSH


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__',   return_value=None)
    def test_configure(self, mocked_init):

        component = SSH(name=None, cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.env_removables = []
        component._configure()

        self.assertEqual(component.launch_command, '/bin/ssh -o StrictHostKeyChecking=no -o ControlMaster=auto')
    # --------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

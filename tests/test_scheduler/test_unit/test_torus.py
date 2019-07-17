# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
import pytest
import radical.utils as ru
from radical.pilot.agent.scheduler.torus import Torus

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(Torus, '__init__', return_value=None)
def test_configure(mocked_init):

    component = Torus()
    component._lrms_cores_per_node = 15
    component._configure()

    assert component._cores_per_node == 15
    assert component.nodes == 'bogus'

    component = Torus()
    component._lrms_info = {'name': 'Test'}
    component._lrms_cores_per_node = 0
    with pytest.raises(RuntimeError):
        component._configure()

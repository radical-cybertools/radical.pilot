# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

import os
import radical.pilot
from radical.pilot.agent.scheduler.base import AgentSchedulingComponent

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(radical.pilot.utils.Component, '__init__', return_value=None)
@mock.patch('radical.utils.generate_id', return_value='test')
def test_init(mocked_init, mocked_generate_id):
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert not component._uniform_wl

    os.environ['RP_UNIFORM_WORKLOAD'] = 'true'
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert component._uniform_wl

    os.environ['RP_UNIFORM_WORKLOAD'] = 'YES'
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert component._uniform_wl

    os.environ['RP_UNIFORM_WORKLOAD'] = '1'
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert component._uniform_wl

# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import pytest
import unittest
from radical.pilot.agent.executing.base import AgentExecutingComponent

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#


class TestBase(unittest.TestCase):


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentExecutingComponent, '__init__', return_value=None)
    def test_create(self, mocked_init):

        component = AgentExecutingComponent()
        # Make sure to test *UNKNOWN* and or any *NOT* supported executors.
        cfg = {'spawners' : [{"spawner": "ORTE"},
                              {"spawner":"POPEN"},{"spawner":"SHELL"},
                              {"spawner":"UNKNOWN"},{"spawner":"SHELLFS"}]}

        for i in range(len(cfg['spawners'])):

            try:
                component.create(cfg= cfg['spawners'][i], session=None)
            except:
                with pytest.raises(RuntimeError):
                    raise



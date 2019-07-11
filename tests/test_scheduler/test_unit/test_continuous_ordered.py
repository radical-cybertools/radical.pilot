# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
import pytest
import radical.utils as ru
from radical.pilot.agent.scheduler.continuous_ordered import ContinuousOrdered

try:
    import mock
except ImportError:
    from unittest import mock
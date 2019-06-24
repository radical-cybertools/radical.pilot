# pylint: disable=protected-access, unused-argument
import os
import pytest
import radical.utils as ru
from radical.pilot.agent.rm.pbspro import PBSPro

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(Slurm, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes2'])
def test_configure(mocked_init, mocked_raise_on, mocked_expand_hostlist):


def test_configure_error(mocked_init, mocked_raise_on, mocked_expand_hostlist):


def test_parse_pbspro_vnodes(mocked_init, mocked_raise_on, mocked_expand_hostlist):


def test_parse_pbspro_vnodes_error(mocked_init, mocked_raise_on, mocked_expand_hostlist):
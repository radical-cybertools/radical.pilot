# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"
import os
import pytest
import unittest
import subprocess
import radical.utils as ru
from radical.pilot.agent.executing.popen import Popen

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#


class TestBase(unittest.TestCase):



    # ------------------------------------------------------------------------------
    #
    def setUp(self):

        tc = ru.read_json('/tests/test_executing/test_unit/test_cases/test_base.json')

        return tc


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    def test_handle_unit(self, mocked_init):
        tests = self.setUp()
        cu = dict()
        cu['uid'] = tests['unit']['uid'] 
        cu['description'] = tests['unit']['description'] 
        
        component = Popen()
        component._prof   = mock.Mock()
        component.publish = mock.Mock()
        component.advance = mock.Mock()
        component._log = ru.Logger('dummy')  
        test = component._handle_unit(cu)
        assert test == None


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    def test_check_running(self, mocked_init):
        tests = self.setUp()
        cu = dict()
        cu = tests['unit']
        cu['proc'] = subprocess.Popen(args       = '',
                                      executable = '/bin/sleep',
                                      stdin      = None,
                                      stdout     = None,
                                      stderr     = None,
                                      preexec_fn = os.setsid,
                                      close_fds  = True,
                                      shell      = True,
                                      cwd        = None)
        component = Popen()
        component._cus_to_watch = list()
        component._cus_to_cancel = list()
        component._cus_to_watch.append(cu)
        test = component._check_running()
        assert test == None



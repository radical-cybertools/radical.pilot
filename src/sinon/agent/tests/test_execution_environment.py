#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import os
import psutil
import tempfile
import unittest

from radical.agent import ExecutionEnvironment


#-----------------------------------------------------------------------------
#
class Test_ExecutionEnvironment(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        self.tmp_pbsnodes_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_pbsnodes_file.write('hostA\n')
        self.tmp_pbsnodes_file.write('hostA\n')
        self.tmp_pbsnodes_file.write('hostB\n')
        self.tmp_pbsnodes_file.write('hostB\n')
        self.tmp_pbsnodes_file.flush()

    def tearDown(self):
        os.remove(self.tmp_pbsnodes_file.name)

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__detect_nodes_with_pbs_nodefile(self):
        os.environ['PBS_NODEFILE'] = self.tmp_pbsnodes_file.name

        ee = ExecutionEnvironment()
        ee._detect_nodes()
        assert len(ee._raw_nodes) == 4
        assert ee.raw_nodes == ee._raw_nodes

        if os.environ.get('PBS_NODEFILE') is not None:
            del os.environ['PBS_NODEFILE']

    #-------------------------------------------------------------------------
    #
    def test__detect_nodes_without_pbs_nodefile(self):
        # test without PBS_NODEFILE set
        if os.environ.get('PBS_NODEFILE') is not None:
            del os.environ['PBS_NODEFILE']

        ee = ExecutionEnvironment()
        ee._detect_nodes()

        assert ee._raw_nodes == ['localhost']
        assert ee.raw_nodes == ee._raw_nodes

    #-------------------------------------------------------------------------
    #
    def test__discover_nodes_with_pbs_nodefile(self):
        os.environ['PBS_NODEFILE'] = self.tmp_pbsnodes_file.name

        ee = ExecutionEnvironment.discover()

        assert len(ee.nodes) == 2
        assert ee.nodes['hostA']['cores'] == psutil.NUM_CPUS
        assert ee.nodes['hostB']['memory'] == int(psutil.virtual_memory().total/1024/1024)

        # restore environment and delete tempfile
        if os.environ.get('PBS_NODEFILE') is not None:
            del os.environ['PBS_NODEFILE']

    #-------------------------------------------------------------------------
    #
    def test_discover_without_pbs_nodefile(self):
        # test without PBS_NODEFILE set
        if os.environ.get('PBS_NODEFILE') is not None:
            del os.environ['PBS_NODEFILE']

        ee = ExecutionEnvironment.discover()

        assert len(ee.nodes) == 1
        assert ee.nodes['localhost']['cores'] == psutil.NUM_CPUS
        assert ee.nodes['localhost']['memory'] == int(psutil.virtual_memory().total/1024/1024)

    #-------------------------------------------------------------------------
    #
    def test__detect_cores_and_memory(self):
        ee = ExecutionEnvironment()
        ee._detect_cores_and_memory()

        assert ee._cores_per_node == psutil.NUM_CPUS
        assert ee._memory_per_node == int(psutil.virtual_memory().total/1024/1024)


#-----------------------------------------------------------------------------
#
#def test_suite():
#    return unittest.TestSuite((
#            unittest.makeSuite(Test_A),
#        ))

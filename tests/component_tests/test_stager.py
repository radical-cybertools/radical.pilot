# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import tempfile

import threading     as mt
import radical.utils as ru

from radical.pilot.staging_directives import expand_staging_directives
from radical.pilot.utils.staging_helper import StagingHelper

from unittest import mock, TestCase


# ------------------------------------------------------------------------------
#
class StagerTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_handle_staging(self):

        stager = StagingHelper(log=mock.Mock(), prof=mock.Mock())

        base_dir = tempfile.gettempdir()
        src_dir  = os.path.join(base_dir, 'stager_test')
        src_file = os.path.join(src_dir, 'file_to_be_staged')
        src_file_content = 'to be staged'

        ru.rec_makedir(src_dir)
        with ru.ru_open(src_file, 'w') as fd:
            fd.write(src_file_content)

        # directory to directory

        tgt_dir = os.path.join('/tmp', 'dir00', 'dir01', '')
        sds = [{'uid'   : 'sd.0000',
                'source': src_dir,
                'target': tgt_dir}]
        # default action is transfer
        sds = expand_staging_directives(sds)
        for sd in sds:
            stager.handle_staging_directive(sd)

        self.assertTrue(os.path.isdir(tgt_dir))
        tgt_file = os.path.join(tgt_dir, 'stager_test', 'file_to_be_staged')
        print('===', tgt_file)
        self.assertTrue(os.path.isfile(tgt_file))

        # file to file

        tgt_file = os.path.join(src_dir, 'dir00', 'dir01', 'tgt_file')
        sds = [{'uid'   : 'sd.0001',
                'source': src_file,
                'target': tgt_file}]
        # default action is transfer
        sds = expand_staging_directives(sds)
        for sd in sds:
            stager.handle_staging_directive(sd)

        self.assertTrue(os.path.exists(tgt_file))
        with ru.ru_open(tgt_file) as fd:
            self.assertEqual(fd.readlines()[0], src_file_content)


# ------------------------------------------------------------------------------


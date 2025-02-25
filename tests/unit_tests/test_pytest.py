#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import sys

from unittest import TestCase


# ------------------------------------------------------------------------------
#
class TestDebug(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_pytest(self):

        version = os.environ.get("PYTEST_VERSION")
        sys.stdout.write("==== pytest version: %s\n" % version)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestDebug()
    tc.test_pytest()


# ------------------------------------------------------------------------------


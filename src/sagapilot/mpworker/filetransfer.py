"""
.. module:: sinon.mpworker.pilotlauncher
   :platform: Unix
   :synopsis: Implements the input / output transfer functionality.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import datetime
import traceback

from radical.utils import which

from sagapilot import states
from sagapilot.credentials import SSHCredential
from sagapilot.utils.logger import logger


# ----------------------------------------------------------------------------
#
# def transfer_input_func(unit_uid, session_dict, target_dir_url, transfer):


def transfer_input_func(unit_uid, transfer):
    """ Transfers a set of task input files to a specific pilot.
    """
    logger.warning("about to transfer %s", transfer)

    result = dict()

    result["unit_uid"] = unit_uid
    result["state"] = states.FAILED
    result["log"] = ["transfer_input_func() not implemented"]

    return result


# ----------------------------------------------------------------------------
#
def transfer_output_func(unit_uid, transfer):
    """ Transfers a set of task output files to the local machine.
    """
    logger.warning("about to transfer %s", transfer)

    result = dict()

    result["unit_uid"] = unit_uid
    result["state"] = states.FAILED
    result["log"] = ["transfer_output_func() not implemented"]

    return result
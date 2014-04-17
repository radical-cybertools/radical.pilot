"""
.. module:: radical.pilot.mpworker.pilotlauncher
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

from radical.pilot import states
from radical.pilot.credentials import SSHCredential
from radical.pilot.utils.logger import logger


# ----------------------------------------------------------------------------
#
# def transfer_input_func(unit_uid, session_dict, target_dir_url, transfer):


def transfer_input_func(pilot_uid, unit_uid, credentials, unit_sandbox, transfer):
    """ Transfers a set of task input files to a specific pilot.
    """
    #logger.warning("about to transfer %s to %s (cred: %s)" % (transfer, unit_sandbox, credentials))

    saga_session = saga.Session()

    for cred_dict in credentials:
        cred = SSHCredential.from_dict(cred_dict)
        saga_session.add_context(cred._context)

    logger.debug(
        "Using credentials %s for input file transfer." % str(saga_session))

    try:
        log = []
        # First, we need to create the WU's directory in case it doesn't exist yet.
        wu_dir = saga.filesystem.Directory(
            unit_sandbox,
            flags=saga.filesystem.CREATE_PARENTS,
            session=saga_session)
        wu_dir.close()

        logger.debug(
            "Created/opened ComputeUnit sandbox dir %s." % unit_sandbox)

        # Next we copy all input files to the target machine
        for t in transfer:

            st = t.split(">")
            abs_t = os.path.abspath(st[0].strip())

            input_file_url = saga.Url("file://localhost/%s" % abs_t)
            input_file = saga.filesystem.File(
                input_file_url,
                session=saga_session)

            if len(st) == 1:
                target = unit_sandbox
                input_file.copy(target)
            elif len(st) == 2:
                target = "%s/%s" % (unit_sandbox, st[1].strip()) 
                input_file.copy(target)
            else:
                input_file.close()
                raise Exception("Invalid transfer directive: %s" % t)

            input_file.close()

            log_msg = "Successfully transferred input file %s -> %s" % (input_file_url, target)

            log.append(log_msg)
            logger.debug(log_msg)

        result = dict()
        result["unit_uid"] = unit_uid
        result["pilot_uid"] = pilot_uid
        result["state"] = states.PENDING_EXECUTION
        result["log"] = log

    except Exception, ex:
        log.append(str(ex))
        logger.error("Couldn't transfer input file(s): %s", str(ex))

        result = dict()
        result["unit_uid"] = unit_uid
        result["pilot_uid"] = pilot_uid
        result["state"] = states.FAILED
        result["log"] = log

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

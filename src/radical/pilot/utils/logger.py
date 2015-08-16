#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: radical.pilot.utils.logger
   :platform: Unix
   :synopsis: Implementation of the logging facility.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import sys
import logging
import colorama


# ------------------------------------------------------------------------------
#
class ColorStreamHandler(logging.StreamHandler):
    """ 
    A colorized output SteamHandler 
    """

    colours = {'DEBUG'    : colorama.Fore.CYAN,
               'INFO'     : colorama.Fore.GREEN,
               'WARN'     : colorama.Fore.YELLOW,
               'WARNING'  : colorama.Fore.YELLOW,
               'ERROR'    : colorama.Fore.RED,
               'CRIT'     : colorama.Back.RED + colorama.Fore.WHITE,
               'CRITICAL' : colorama.Back.RED + colorama.Fore.WHITE
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, target):

        logging.StreamHandler.__init__(self, target)
        self._tty  = self.stream.isatty()
        self._term = getattr(self, 'terminator', '\n')

    # --------------------------------------------------------------------------
    #
    def emit(self, record):

        # only write in color when using a tty
        if self._tty:
            self.stream.write(self.colours[record.levelname] \
                             + self.format(record)           \
                             + colorama.Style.RESET_ALL      \
                             + self._term)
        else:
            self.stream.write(self.format(record) + self._term)

      # self.flush()



# ------------------------------------------------------------------------------
#
def get_logger(name, target=None, level=None):
    """
    Get a logging handle.

    'name'   is used to identify log entries on this handle.
    'target' is a comma separated list (or Python list) of specifiers, where
             specifiers are:
             '-'      : stdout
             '1'      : stdout
             'stdout' : stdout
             '='      : stderr
             '2'      : stderr
             'stderr' : stderr
             '.'      : logfile named ./<name>.log
             <string> : logfile named <string>
    'level'  log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """

    if not target: target = os.environ.get('RADICAL_PILOT_LOG_TARGETS', '-')
    if not level : level  = os.environ.get('RADICAL_PILOT_VERBOSE', 'INFO')

    if not isinstance(target, list):
        target = target.split(',')

    logger    = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s: ' \
                                  '%(name)-15s: ' \
                                  '%(processName)-15s: ' \
                                  '%(threadName)-15s: ' \
                                  '%(levelname)-8s: ' \
                                  '%(message)s')
    # start with an empty slate
    for h in logger.handlers[:]:
        logger.removeHandler(h)

    # and then add handler for all targets
    for t in target:
        if t in ['-', '1', 'stdout']:
            handle = ColorStreamHandler(sys.stdout)
        elif t in ['=', '2', 'stderr']:
            handle = ColorStreamHandler(sys.stderr)
        elif t in ['.']:
            handle = logging.StreamHandler("./%s.log" % name)
        else:
            handle = logging.FileHandler(t)
        handle.setFormatter(formatter)
        logger.addHandler(handle)

    logger.setLevel(level)
    logger.propagate = 0

  # import radical.utils as ru
  # import saga          as rs

  # pwd  = os.path.dirname (__file__)
  # root = "%s/.." % pwd
  # rp_version, _, _, _, _ = ru.get_version ([root, pwd])
  #
  # logger.info("Using RADICAL-Utils version %s", rs.version)
  # logger.info("Using RADICAL-SAGA  version %s", rs.version)
  # logger.info("Using RADICAL-Pilot version %s", rp_version)

    return logger


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------


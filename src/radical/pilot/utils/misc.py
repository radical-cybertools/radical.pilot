
import os
import sys
import copy
import time
import errno
import datetime
import pymongo
import netifaces
import threading
import multiprocessing

import radical.utils as ru
from   radical.pilot.states import *

# ------------------------------------------------------------------------------
#
# max number of cu out/err chars to push to tail
MAX_IO_LOGLENGTH = 1024
def tail(txt, maxlen=MAX_IO_LOGLENGTH):

    # shorten the given string to the last <n> characters, and prepend
    # a notification.  This is used to keep logging information in mongodb
    # manageable(the size of mongodb documents is limited).

    if not txt:
        return txt

    if len(txt) > maxlen:
        return "[... CONTENT SHORTENED ...]\n%s" % txt[-maxlen:]
    else:
        return txt


# ------------------------------------------------------------------------------
#
def get_rusage():

    import resource

    self_usage  = resource.getrusage(resource.RUSAGE_SELF)
    child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)

    rtime = time.time()
    utime = self_usage.ru_utime  + child_usage.ru_utime
    stime = self_usage.ru_stime  + child_usage.ru_stime
    rss   = self_usage.ru_maxrss + child_usage.ru_maxrss

    return "real %3f sec | user %.3f sec | system %.3f sec | mem %.2f kB" \
         % (rtime, utime, stime, rss)


# ------------------------------------------------------------------------------
#
def rec_makedir(target):

    # recursive makedir which ignores errors if dir already exists

    try:
        os.makedirs(target)

    except OSError as e:
        # ignore failure on existing directory
        if e.errno == errno.EEXIST and os.path.isdir(os.path.dirname(target)):
            pass
        else:
            raise


# ------------------------------------------------------------------------------
#
_hostip = None
def hostip(req=None, black_list=None, pref_list=None, logger=None):
    """
    Look up the ip number for a given requested interface name.
    If interface is not given, do some magic.
    """

    # we only determine hostip once
    global _hostip
    if _hostip:
        return _hostip

    # List of interfaces that we probably dont want to bind to by default
    if not black_list:
        black_list = ['sit0', 'lo']

    # Known intefaces in preferred order
    if not pref_list:
        pref_list = [
            'ipogif0', # Cray's
            'br0'      # SuperMIC
        ]

    gateways = netifaces.gateways()
    if  not 'default' in gateways or \
        not gateways['default']:
        return '127.0.0.1'

    # we always add the currently used interface to the preferred ones
    default = gateways['default'][netifaces.AF_INET][1]
    if default not in pref_list:
        pref_list.append(default)

    # Get a list of all network interfaces
    all = netifaces.interfaces()

    if logger:
        logger.debug("Network interfaces detected: %s", all)

    pref = None
    # If we got a request, see if it is in the list that we detected
    if req and req in all:
        # Requested is available, set it
        pref = req
    else:
        # No requested or request not found, create preference list
        potentials = [iface for iface in all if iface not in black_list]

    # If we didn't select an interface already
    if not pref:
        # Go through the sorted list and see if it is available
        for iface in pref_list:
            if iface in all:
                # Found something, get out of here
                pref = iface
                break

    # If we still didn't find something, grab the first one from the
    # potentials if it has entries
    if not pref and potentials:
        pref = potentials[0]

    # If there were no potentials, see if we can find one in the blacklist
    if not pref:
        for iface in black_list:
            if iface in all:
                pref = iface

    if logger:
        logger.debug("Network interfaces selected: %s", pref)

    # Use IPv4, because, we can ...
    af = netifaces.AF_INET
    ip = netifaces.ifaddresses(pref)[af][0]['addr']

    # we fall back to localhost
    if not ip:
        ip = '127.0.0.1'

    if logger:
        logger.debug("Network ip address detected: %s", ip)

    # cache for next invocation
    _hostip = ip
    return ip


# ----------------------------------------------------------------------------------


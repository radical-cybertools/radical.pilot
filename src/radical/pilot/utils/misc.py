
import os
import sys
import copy
import time
import datetime
import pymongo
import threading
import multiprocessing

import radical.utils as ru
from   radical.pilot.states import *

# ------------------------------------------------------------------------------
#
# time stamp for profiling etc.
#
def timestamp():
    # human readable absolute UTC timestamp for log entries in database
    return datetime.datetime.utcnow()

def timestamp_epoch():
    # absolute timestamp as seconds since epoch
    return float(time.time())

# absolute timestamp in seconds since epocj pointing at start of
# bootstrapper (or 'now' as fallback)
timestamp_zero = float(os.environ.get('TIME_ZERO', time.time()))

print "timestamp zero: %s" % timestamp_zero

def timestamp_now():
    # relative timestamp seconds since TIME_ZERO (start)
    return float(time.time()) - timestamp_zero


# ------------------------------------------------------------------------------
#
# profiling support
#
# If 'RADICAL_PILOT_PROFILE' is set in environment, we log timed events.
#
if 'RADICAL_PILOT_PROFILE' in os.environ:
    profile_rp = True
    _profile_handle = open('agent.prof', 'a')
else:
    profile_rp = False
    _profile_handle = sys.stdout


# ------------------------------------------------------------------------------
#
# FIXME: AGENT_MODE should not live here...
AGENT_THREADS   = 'threading'
AGENT_PROCESSES = 'multiprocessing'
AGENT_MODE      = AGENT_THREADS

def prof(etype, uid="", msg="", logger=None):

    # record a timed event.  We record the thread ID, the uid of the affected
    # object, an event type, and a log message.
    #
    # TODO: should this move to utils?  Or at least RP utils, so that we can
    # also use it for the application side?

    # TODO: Why are we logging events when profiling is disabled?
    if logger:
        logger("%s (%10s) : %s", etype, msg, uid)

    if not profile_rp:
        return

    now = timestamp_now()

    if   AGENT_MODE == AGENT_THREADS  : tid = threading.current_thread().name
    elif AGENT_MODE == AGENT_PROCESSES: tid = os.getpid()
    else: raise Exception('Unknown Agent Mode')

    # NOTE: Don't forget to sync any format changes in the bootstrapper
    # and downstream analysis tools too!
    _profile_handle.write("%.4f,%s,%s,%s,%s\n" % (now, tid, uid, etype, msg))


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
def blowup(config, cus, component, logger=None):
    # for each cu in cu_list, add 'factor' clones just like it, just with
    # a different ID (<id>.clone_001)

    # TODO: I dont like it that there is non blow-up semantics in the blow-up function.
    # Probably want to put the conditional somewhere else.
    if not isinstance (cus, list) :
        cus = [cus]

    if not profile_rp:
        return cus

    factor = config['blowup_factor'].get (component, 1)
    drop   = config['drop_clones']  .get (component, 1)

    ret = list()

    for cu in cus :

        uid = cu['_id']

        if drop >= 1:
            # drop clones --> drop matching uid's
            if '.clone_' in uid :
                prof ('drop clone', msg=component, uid=uid)
                continue

        if drop >= 2:
            # drop everything, even original units
            prof ('drop', msg=component, uid=uid)
            continue

        factor -= 1
        if factor :
            for idx in range(factor) :

                cu_clone = copy.deepcopy (dict(cu))
                clone_id = '%s.clone_%05d' % (str(cu['_id']), idx+1)

                for key in cu_clone :
                    if isinstance (cu_clone[key], basestring) :
                        cu_clone[key] = cu_clone[key].replace (uid, clone_id)

                idx += 1
                ret.append (cu_clone)
                prof('add clone', msg=component, uid=clone_id)

        # append the original unit last, to  increase the likelyhood that
        # application state only advances once all clone states have also
        # advanced (they'll get pushed onto queues earlier)
        ret.append (cu)

    return ret


# ------------------------------------------------------------------------------
#
def get_rusage():

    import resource

    self_usage  = resource.getrusage(resource.RUSAGE_SELF)
    child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)

    rtime = time.time() - timestamp_zero
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
def get_mongodb(mongodb_url, mongodb_name, mongodb_auth):

    mongo_client = pymongo.MongoClient(mongodb_url)
    mongo_db     = mongo_client[mongodb_name]

    # do auth on username *and* password (ignore empty split results)
    if mongodb_auth:
        username, passwd = mongodb_auth.split(':')
        mongo_db.authenticate(username, passwd)

    return mongo_db



# ------------------------------------------------------------------------------



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
def get_mongodb(mongodb_url, mongodb_name, mongodb_auth):

    mongo_client = pymongo.MongoClient(mongodb_url)
    mongo_db     = mongo_client[mongodb_name]

    # do auth on username and password, if available
    if mongodb_auth:
        mongo_db.authenticate(mongodb_auth.split(':'))

    return mongo_db

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

# print "timestamp zero: %s" % timestamp_zero

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
    profile_rp     = True
    _profile_handle = open('agent.prof', 'a')
else:
    profile_rp     = False
    _profile_handle = sys.stdout


# ------------------------------------------------------------------------------
#
_profile_tags  = dict()
_profile_freqs = dict()

def prof(etype, uid="", msg="", tag="", logger=None):

    # record a timed event.  We record the thread ID, the uid of the affected
    # object, a log message, event type, and a tag.  Whenever a tag changes (to
    # a non-None value), the time since the last tag change is added.  This can
    # be used to derive, for example, the duration which a uid spent in
    # a certain state.  Time intervals between the same tags (but different
    # uids) are recorded, too.
    #
    # TODO: should this move to utils?  Or at least RP utils, so that we can
    # also use it for the application side?

    if logger:
        logger("%s -- %s (%s): %s", etype, msg, uid, tag)


    if not profile_rp:
        return


    logged = False
    now    = timestamp_now()

    # FIXME: performance penalty due to repeated calls.  tid should be part of
    # the signature...
    tid = threading.current_thread().name
    if tid == 'MainThread' :
        tid = os.getpid ()

    if uid and tag:

        if not uid in _profile_tags:
            _profile_tags[uid] = {'tag'  : "",
                                 'time' : 0.0 }

        old_tag = _profile_tags[uid]['tag']

        if tag != old_tag:

            tagged_time = now - _profile_tags[uid]['time']

            _profile_tags[uid]['tag' ] = tag
            _profile_tags[uid]['time'] = timestamp_now()

            _profile_handle.write("> %12.4f : %-20s : %12.4f : %-17s : %-24s : %-40s : %s\n" \
                                  % (tagged_time, tag, now, tid, uid, etype, msg))
            logged = True


            if not tag in _profile_freqs:
                _profile_freqs[tag] = {'last'  : now,
                                      'diffs' : list()}
            else:
                diff = now - _profile_freqs[tag]['last']
                _profile_freqs[tag]['diffs'].append(diff)
                _profile_freqs[tag]['last' ] = now

              # freq = sum(_profile_freqs[tag]['diffs']) / len(_profile_freqs[tag]['diffs'])
              #
              # _profile_handle.write("> %12s : %-20.4f : %12s : %-17s : %-24s : %-40s : %s\n" \
              #                       % ('frequency', freq, '', '', '', '', ''))



    if not logged:
        _profile_handle.write("  %12s : %-20s : %12.4f : %-17s : %-24s : %-40s : %s\n" \
                              % (' ' , ' ', now, tid, uid, etype, msg))
  
    # FIXME: disable flush on production runs
    _profile_handle.flush()
     

# ------------------------------------------------------------------------------
#
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
def blowup(config, cus, component):
    # for each cu in cu_list, add 'factor' clones just like it, just with
    # a different ID (<id>.clone_001)

    if not isinstance (cus, list) :
        cus = [cus]

    if not profile_rp:
        return cus

    factor = config['blowup_factor'].get (component, 1)
    drop   = config['drop_clones']  .get (component, False)

    ret = list()

    for cu in cus :

        uid = cu['_id']

        if drop :
            if '.clone_' in uid :
                prof ('drop', uid=uid)
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
                prof('cloned unit ingest (%s)' % component, uid=clone_id)

        # append the original unit last, to  increase the likelyhood that
        # application state only advances once all clone states have also
        # advanced (they'll get pushed onto queues earlier)
        ret.append (cu)

    return ret


# ------------------------------------------------------------------------------
#
def rusage():

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


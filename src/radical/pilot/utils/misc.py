
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
# profile class
#
class Profiler (object):

    # we want the profiler to be a singleton, to have consistent timing over all
    # threads.  Each process needs to re-inialize the profiler if it was not
    # inherited in any other way.  We thus append pid and thread id to the
    # profiler output files
    __metaclass__ = ru.Singleton


    # --------------------------------------------------------------------------
    #
    def __init__ (self, target, uid=None, logger=None):

        # this init is only called once (globally).  We synchronize clocks and
        # set timestamp_zero

        # we only profile if so instructed
        if 'RADICAL_PILOT_PROFILE' in os.environ:
            self._enabled = True
        else:
            self._enabled = False
            return

        self._target  = target
        self._handles = dict()
        self._logger  = logger

        self._ts_zero, self._ts_abs = self._timestamp_init()

        # log initialization event
        tid = threading.current_thread().name
        pid = os.getpid()
        _ = self._get_handle (pid, tid)


    # ------------------------------------------------------------------------------
    #
    @property
    def enabled(self):

        return self._enabled


    # ------------------------------------------------------------------------------
    #
    def flush(self):

        if self._enabled:
            for pid in self._handles:
                for tid in self._handles[pid]:
                    self._handles[pid][tid].flush()


    # ------------------------------------------------------------------------------
    #
    def prof(self, etype, uid="", msg="", timestamp=None, logger=None):

        if not self._enabled:
            return

        if         logger:       logger("%s (%10s) : %s", etype, msg, uid)
        elif self._logger: self._logger("%s (%10s) : %s", etype, msg, uid)

        tid = threading.current_thread().name
        pid = os.getpid()

        if timestamp != None:
            if timestamp > (100 * 1000 * 1000):
                # older than 3 years (time after 1973) 
                # --> this is an absolute timestamp
                timestamp = timestamp - self._ts_zero
            else:
                # this is a relative timestamp -- leave as is
                pass
        else:
            # no timestamp provided -- use 'now'
            timestamp = self._timestamp_now()

        # NOTE: Don't forget to sync any format changes in the bootstrapper
        #       and downstream analysis tools too!
        handle = self._get_handle (pid, tid)
        handle.write("%.4f,%s:%s,%s,%s,%s\n" % (timestamp, pid, tid, uid, etype, msg))


    # --------------------------------------------------------------------------
    #
    def _get_handle (self, pid, tid):

        # NOTE: Don't forget to sync any format changes in the bootstrapper
        #       and downstream analysis tools too!

        if not pid in self._handles:
            self._handles[pid] = dict()

        if not tid in self._handles[pid]:

            if self._target == '-':
                handle = sys.stdout
            else:
                timestamp = self._timestamp_now()
                handle = open("%s.%s.%s.prof" % (self._target, pid, tid), 'a')

                # write header and time normalization info
                handle.write("#time,name,uid,event,msg\n")
                handle.write("%.4f,%s:%s,%s,%s,%s\n" % \
                        (timestamp, pid, tid, "", 'sync abs',\
                         "%s:%s:%s" % (time.time(), self._ts_zero, self._ts_abs)))

            self._handles[pid][tid] = handle

        return self._handles[pid][tid]


    # --------------------------------------------------------------------------
    #
    def _timestamp_init(self):
        """
        return a tuple of [system time, absolute time]
        """

        # retrieve absolute timestamp from an external source
        #
        # We first try to contact a network time service for a timestamp, if that
        # fails we use the current system time.
        try:
            import ntplib
            response = ntplib.NTPClient().request('0.pool.ntp.org')
            timestamp_sys  = response.orig_time
            timestamp_abs  = response.tx_time
            return [timestamp_sys, timstamp_abs]
        except:
            t = time.time()
            return [t,t]


    # --------------------------------------------------------------------------
    #
    def _timestamp_now(self):

        # relative timestamp seconds since TIME_ZERO (start)
        return float(time.time()) - self._ts_zero


# --------------------------------------------------------------------------
#
# methods to keep the new Profiler class backward compatible
#
_p = None
def prof_init(target, uid="", logger=None):
    global _p
    _p = Profiler (target, uid, logger)

def prof(etype, uid="", msg="", timestamp=None, logger=None):
    global _p
    _p and _p.prof(etype=etype, uid=uid, msg=msg, timestamp=timestamp, logger=logger)

def flush_prof():
    global _p
    _p and _p.flush()

# --------------------------------------------------------------------------
#
def timestamp():
    # human readable absolute UTC timestamp for log entries in database
    return datetime.datetime.utcnow()

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
    #
    # This method also *drops* clones as needed!
    #
    # return value: [list of original and expanded CUs, list of dropped CUs]

    # TODO: I dont like it that there is non blow-up semantics in the blow-up function.
    # Probably want to put the conditional somewhere else.
    if not isinstance (cus, list) :
        cus = [cus]

    # blowup is only enabled on profiling
    global _p
    if not _p or not _p.enabled: 
        return

    factor = config['blowup_factor'].get (component, 1)
    drop   = config['drop_clones']  .get (component, 1)

    prof ("debug", msg="%s drops with %s" % (component, drop))

    cloned  = list()
    dropped = list()

    for cu in cus :

        uid = cu['_id']

        if drop >= 1:
            # drop clones --> drop matching uid's
            if '.clone_' in uid :
                prof ('drop clone', msg=component, uid=uid)
                dropped.append(cu)
                continue

        if drop >= 2:
            # drop everything, even original units
            prof ('drop', msg=component, uid=uid)
            dropped.append(cu)
            continue

        if factor < 0:
            # FIXME: we should print a warning or something?
            # Anyway, we assume the default here, ie. no blowup, no drop.
            factor = 1

        for idx in range(factor-1) :

            cu_clone = copy.deepcopy (dict(cu))
            clone_id = '%s.clone_%05d' % (str(cu['_id']), idx+1)

            for key in cu_clone :
                if isinstance (cu_clone[key], basestring) :
                    cu_clone[key] = cu_clone[key].replace (uid, clone_id)

            idx += 1
            cloned.append(cu_clone)
            prof('add clone', msg=component, uid=clone_id)

        # For any non-zero factor, append the original unit -- factor==0 lets us
        # drop the cu.
        #
        # Append the original cu last, to increase the likelyhood that
        # application state only advances once all clone states have also
        # advanced (they'll get pushed onto queues earlier).  This cannot be
        # relied upon, obviously.
        if factor > 0: cloned.append(cu)

    return cloned, dropped


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
def get_mongodb(mongodb_url, mongodb_name, mongodb_auth):

    mongo_client = pymongo.MongoClient(mongodb_url)
    mongo_db     = mongo_client[mongodb_name]

    # do auth on username *and* password (ignore empty split results)
    if mongodb_auth:
        username, passwd = mongodb_auth.split(':')
        mongo_db.authenticate(username, passwd)

    return mongo_db



# ------------------------------------------------------------------------------


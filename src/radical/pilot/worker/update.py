
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import time
import pprint
import threading

import radical.utils as ru

from .. import utils     as rpu
from .. import states    as rps
from .. import constants as rpc


# ==============================================================================
#
DEFAULT_BULK_COLLECTION_TIME = 1.0 # seconds


# ==============================================================================
#
class Update(rpu.Worker):
    """
    An UpdateWorker pushes CU and Pilot state updates to mongodb.  Its instances
    compete for update requests on the update_queue.  Those requests will be
    triplets of collection name, query dict, and update dict.  Update requests
    will be collected into bulks over some time (BULK_COLLECTION_TIME), to
    reduce number of roundtrips.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id('update.%(counter)s', ru.ID_CUSTOM)

        rpu.Worker.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg, session):

        return cls(cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._session_id = self._cfg['session_id']
        self._dburl      = self._cfg['dburl']
        self._owner      = self._cfg['owner']

        _, db, _, _, _   = ru.mongodb_connect(self._dburl)
        self._mongo_db   = db
        self._cinfo      = dict()            # collection cache
        self._lock       = threading.RLock() # protect _cinfo

        self.register_subscriber(rpc.STATE_PUBSUB, self._state_cb)
        self.register_idle_cb(self._idle_cb, 
                              timeout=self._cfg.get('bulk_collection_time', 1.0))


    # --------------------------------------------------------------------------
    #
    def _timed_bulk_execute(self, cinfo, flush=False):

        # is there any bulk to look at?
        if not cinfo['bulk']:
            return False

        now = time.time()
        age = now - cinfo['last']

        # only push if collection time has been exceeded
        if not age > self._cfg.get('bulk_collection_time', DEFAULT_BULK_COLLECTION_TIME):
            return False

        try:
            res = cinfo['bulk'].execute()
        except Exception as e:
            self._log.exception('mongodb error: %s', e)
            raise
        self._log.debug("bulk update result: %s", res)

        self._prof.prof('update bulk pushed (%d)' % (len(cinfo['uids'])),
                        uid=self._owner)
        for entry in cinfo['uids']:
            uid   = entry[0]
            ttype = entry[1]
            state = entry[2]
            if state:
                self._prof.prof('update', msg='%s update pushed (%s)' \
                                % (ttype, state), uid=uid)
            else:
                self._prof.prof('update', msg='%s update pushed' % ttype, 
                                uid=uid)

        cinfo['last'] = now
        cinfo['bulk'] = None
        cinfo['uids'] = list()

        return True


    # --------------------------------------------------------------------------
    #
    def _idle_cb(self):

        action = 0
        with self._lock:
            for cname in self._cinfo:
                action += self._timed_bulk_execute(self._cinfo[cname])

        return bool(action)


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, topic, msg):
        """

        # FIXME: this documentation is not final, nor does it reflect reality!

        'msg' is expected to be of the form ['cmd', 'thing'], where 'thing' is
        an entity to update in the DB, and 'cmd' specifies the mode of update.

        'things' are expected to be dicts with a 'type' and 'uid' field.  If
        either one does not exist, an exception is raised.

        Supported types are:

          - unit
          - pilot

        supported 'cmds':

          - delete      : delete can be delayed until bulk is collected/flushed
          - update      : update can be delayed until bulk is collected/flushed
          - state       : update can be delayed until bulk is collected/flushed
                          only state and state history are updated
          - delete_flush: delete is sent immediately (possibly in a bulk)
          - update_flush: update is sent immediately (possibly in a bulk)
          - state_flush : update is sent immediately (possibly in a bulk)
                          only state and state history are updated
          - flush       : flush pending bulk

        The 'thing' can contains '$set' and '$push' fields, which will then be
        used as given.  For all other fields, we use the following convention:

          - scalar values: use '$set'
          - dict   values: use '$set'
          - list   values: use '$push'

        That implies that all potential 'list' types should be defined in the
        initial 'thing' insert as such, as (potentially empty) lists.

        For 'cmd' in ['state', 'state_flush'], only the 'uid' and 'state' fields
        of the given 'thing' are used, all other fields are ignored.  If 'state'
        does not exist, an exception is raised.
        """

        cmd   = msg['cmd']
        thing = msg['arg']

      # cmds = ['delete',       'update',       'state',
      #         'delete_flush', 'update_flush', 'state_flush', 'flush']
        if cmd not in ['update']:
            self._log.info('ignore cmd %s', cmd)
            return


        # FIXME: we don't have any error recovery -- any failure to update 
        #        state in the DB will thus result in an exception here and tear
        #        down the module.

        # got a new request.  Add to bulk (create as needed),
        # and push bulk if time is up.
        uid       = thing['uid']
        ttype     = thing['type']
        state     = thing['state']
        timestamp = thing.get('state_timestamp', rpu.timestamp())

        if 'clone' in uid:
            # we don't push clone states to DB
            return

        self._prof.prof('get', msg="update %s state to %s" % (ttype, state), 
                        uid=uid)

        if not state:
            # nothing to push
            self._prof.prof('get', msg="update %s state ignored" % ttype, uid=uid)
            return

        # create an update document
        update_dict          = dict()
        update_dict['$set']  = dict()
        update_dict['$push'] = dict()

        for key,val in thing.iteritems():
            update_dict['$set'][key] = val

        # we never set _id, states (to avoid index clash, duplicated ops)
        if '_id' in update_dict['$set']:
            del(update_dict['$set']['_id'])
        if 'states' in update_dict['$set']:
            del(update_dict['$set']['states'])

        # we set state, put (more importantly) we push the state onto the
        # 'states' list, so that we can later get state progression in sync with
        # the state model, even if they have been pushed here out-of-order
        update_dict['$push']['states'] = state

        # check if we handled the collection before.  If not, initialize
        # FIXME: we only have one collection now -- simplify!
        cname = self._session_id

        with self._lock:
            if not cname in self._cinfo:
                self._cinfo[cname] = {
                        'coll' : self._mongo_db[cname],
                        'bulk' : None,
                        'last' : time.time(),  # time of last push
                        'uids' : list()
                        }


            # check if we have an active bulk for the collection.  If not,
            # create one.
            cinfo = self._cinfo[cname]

            if not cinfo['bulk']:
                cinfo['bulk'] = cinfo['coll'].initialize_ordered_bulk_op()


            # push the update request onto the bulk
          # print '--> %s: %s' % (uid, pprint.pformat(update_dict))

            cinfo['uids'].append([uid, ttype, state])
            cinfo['bulk'].find  ({'uid'  : uid, 
                                  'type' : ttype}) \
                         .update(update_dict)
            self._prof.prof('bulk', msg='bulked (%s)' % state, uid=uid)

            # attempt a timed update
            self._timed_bulk_execute(cinfo)


# ------------------------------------------------------------------------------


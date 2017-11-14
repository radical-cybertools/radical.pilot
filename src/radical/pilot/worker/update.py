
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import time
import threading
import pymongo

import radical.utils as ru

from .. import utils     as rpu
from .. import constants as rpc


# ==============================================================================
#
DEFAULT_BULK_COLLECTION_TIME =  1.0  # seconds
DEFAULT_BULK_COLLECTION_SIZE =  100  # seconds


# ==============================================================================
#
class Update(rpu.Worker):
    """
    An UpdateWorker pushes CU and Pilot state updates to mongodb.  Its instances
    compete for update requests on the update_queue.  Those requests will be
    triplets of collection name, query dict, and update dict.  Update requests
    will be collected into bulks over some time (BULK_COLLECTION_TIME) and
    number (BULK_COLLECTION_SIZE) to reduce number of roundtrips.
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

        # TODO: get db handle from a connected session
        _, db, _, _, _   = ru.mongodb_connect(self._dburl)
        self._mongo_db   = db
        self._coll       = self._mongo_db[self._session_id]
        self._bulk       = self._coll.initialize_ordered_bulk_op()
        self._last       = time.time()        # time of last bulk push
        self._uids       = list()             # list of collected uids
        self._lock       = threading.RLock()  # protect _bulk

        self._bct        = self._cfg.get('bulk_collection_time',
                                          DEFAULT_BULK_COLLECTION_TIME)
        self._bcs        = self._cfg.get('bulk_collection_size',
                                          DEFAULT_BULK_COLLECTION_SIZE)

        self.register_subscriber(rpc.STATE_PUBSUB, self._state_cb)
        self.register_timed_cb(self._idle_cb, timer=self._bct)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._session._log.debug(' === %s stop called', self._uid)
        super(Update, self).stop()


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        self.unregister_timed_cb(self._idle_cb)
        self.unregister_subscriber(rpc.STATE_PUBSUB, self._state_cb)


    # --------------------------------------------------------------------------
    #
    def _timed_bulk_execute(self, flush=False):

        # is there anything to execute?
        if not self._uids:
            return False

        now = time.time()
        age = now - self._last

        # only push if flush is forced, or when collection time or size 
        # have been exceeded
        if  not flush \
            and age < self._bct \
            and len(self._uids) < self._bcs:
            return False

        try:
            res = self._bulk.execute()
            self._log.debug("bulk update result: %s", res)
        except pymongo.errors.OperationFailure as e:
            self._log.exception('bulk exec error: %s' % e.details)
            raise
        except Exception as e:
            self._log.exception('mongodb error: %s', e)
            raise

        self._prof.prof('update_pushed', msg='bulk size: %d' % len(self._uids))

        for entry in self._uids:

            uid   = entry[0]
            state = entry[2]

            self._prof.prof('update_pushed', msg=state, uid=uid)

        # empty bulk, refresh state
        self._last = now
        self._bulk = self._coll.initialize_ordered_bulk_op()
        self._uids = list()

        return True


    # --------------------------------------------------------------------------
    #
    def _idle_cb(self):

        with self._lock:
             self._timed_bulk_execute()

        return True


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

        cmd    = msg['cmd']
        things = msg['arg']

      # cmds = ['delete',       'update',       'state',
      #         'delete_flush', 'update_flush', 'state_flush', 'flush']
        if cmd not in ['update']:
            self._log.info('ignore cmd %s', cmd)
            return True

        if not isinstance(things, list):
            things = [things]


        # FIXME: we don't have any error recovery -- any failure to update
        #        state in the DB will thus result in an exception here and tear
        #        down the module.
        for thing in things:

            # got a new request.  Add to bulk (create as needed),
            # and push bulk if time is up.
            uid   = thing['uid']
            ttype = thing['type']
            state = thing['state']

            if 'clone' in uid:
                # we don't push clone states to DB
                return True

            self._prof.prof('update_request', msg=state, uid=uid)

            if not state:
                # nothing to push
                return True

            # create an update document
            update_dict          = dict()
            update_dict['$set']  = dict()
            update_dict['$push'] = dict()

            for key,val in thing.iteritems():
                # we never set _id, states (to avoid index clash, duplicated ops)
                if key not in ['_id', 'states']:
                    update_dict['$set'][key] = val

            # we set state, put (more importantly) we push the state onto the
            # 'states' list, so that we can later get state progression in sync with
            # the state model, even if they have been pushed here out-of-order
            update_dict['$push']['states'] = state

            with self._lock:

                # push the update request onto the bulk
                self._uids.append([uid, ttype, state])
                self._bulk.find  ({'uid'  : uid, 
                                   'type' : ttype}) \
                          .update(update_dict)

        with self._lock:
            # attempt a timed update
            self._timed_bulk_execute()

        return True


# ------------------------------------------------------------------------------


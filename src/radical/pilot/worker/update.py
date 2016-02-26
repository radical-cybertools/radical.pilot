
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import time
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
    def __init__(self, cfg, session=None):

        rpu.Worker.__init__(self, 'UpdateWorker', cfg, session)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg, session=None):

        return cls(cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._session_id    = self._cfg['session_id']
        self._mongodb_url   = self._cfg['mongodb_url']
        self._owner         = self._cfg['owner']

        _, db, _, _, _      = ru.mongodb_connect(self._mongodb_url)
        self._mongo_db      = db
        self._cinfo         = dict()            # collection cache
        self._lock          = threading.RLock() # protect _cinfo
        self._state_cache   = dict()            # used to preserve state ordering

        self.declare_subscriber('state', 'state_pubsub', self.state_cb)
        self.declare_idle_cb(self.idle_cb, timeout=self._cfg.get('bulk_collection_time'))

        # communicate successful startup
        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        # communicate finalization
        self.publish('command', {'cmd' : 'final',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    def _ordered_state_update(self, thing, state, timestamp=None):
        """
        The update worker can receive states for a specific thing in any order.
        If states are pushed straight to theh DB, the state attribute of a thing
        may not reflect the actual state.  This should be avoided by re-ordering
        on the client side DB consumption -- but until that is implemented we
        enforce ordered state pushes to MongoDB.  We do it like this:

          - for each thing arriving in the update worker
            - check if new state is final
              - yes: push update, but never push any update again (only update
                hist)
              - no:
                check if all expected earlier states are pushed already
                - yes: push this state also
                - no:  only update state history
        """
        # FIXME: this is specific to agent side updates, but needs to be
        #        generalized for the full state model.  It should disappear once
        #        we introduce a tailing cursor for state notifications -- then
        #        notifications can again be out of order, and the DB docs only
        #        contain time-stamped state histories.

        s2i = {rps.NEW                          :  1,

               rps.PMGR_LAUNCHING_PENDING       :  2,
               rps.PMGR_LAUNCHING               :  3,
               rps.PMGR_ACTIVE_PENDING          :  4,
               rps.PMGR_ACTIVE                  :  5,

               rps.UMGR_SCHEDULING_PENDING      :  6,
               rps.UMGR_SCHEDULING              :  7,
               rps.UMGR_STAGING_INPUT_PENDING   :  8,
               rps.UMGR_STAGING_INPUT           :  9,
               rps.AGENT_STAGING_INPUT_PENDING  : 10,
               rps.AGENT_STAGING_INPUT          : 11,
               rps.AGENT_SCHEDULING_PENDING     : 12,
               rps.AGENT_SCHEDULING             : 13,
               rps.AGENT_EXECUTING_PENDING      : 14,
               rps.AGENT_EXECUTING              : 15,
               rps.AGENT_STAGING_OUTPUT_PENDING : 16,
               rps.AGENT_STAGING_OUTPUT         : 17,
               rps.UMGR_STAGING_OUTPUT_PENDING  : 18,
               rps.UMGR_STAGING_OUTPUT          : 19,

               rps.DONE                         : 20,
               rps.CANCELING                    : 21,
               rps.CANCELED                     : 22,
               rps.FAILED                       : 23
               }
        i2s = {v:k for k,v in s2i.items()}
        s_max = rps.FAILED

        if not timestamp:
            timestamp = rpu.timestamp()

        uid   = thing['uid']
        ttype = thing['type']

      # self._log.debug(" === inp %s: %s" % (uid, state))

        if uid not in self._state_cache:
            if ttype == 'unit':
                if 'agent' in self._owner:
                    # the agent gets the units in this state
                    init_state = rps.AGENT_STAGING_INPUT_PENDING
                else:
                    # this is the umgr then
                    init_state = rps.NEW
            else:
                # pilot starts in NEW for the pmgr
                init_state = rps.NEW

            # populate state cache
            self._state_cache[uid] = {'unsent' : list(),
                                      'final'  : False,
                                      'last'   : rps.NEW}
        # check state cache
        cache = self._state_cache[uid]

        # if thing is already final, we don't push state
        if cache['final']:
          # self._log.debug(" === fin %s: %s" % (uid, state))
            return None

        # if thing becomes final, push state and remember it
        if state not in [rps.DONE, rps.FAILED, rps.CANCELED]:

            # check if we have any consecutive list beyond 'last' in unsent
            cache['unsent'].append(state)
          # self._log.debug(" === lst %s: %s %s" % (uid, cache['last'], cache['unsent']))
            new_state = None
            for i in range(s2i[cache['last']]+1, s2i[s_max]):
              # self._log.debug(" === chk %s: %s in %s" % (uid, i2s[i], cache['unsent']))
                if i2s[i] in cache['unsent']:
                    new_state = i2s[i]
                    cache['unsent'].remove(i2s[i])
                  # self._log.debug(" === uns %s: %s" % (uid, new_state))
                else:
                  # self._log.debug(" === brk %s: %s" % (uid, new_state))
                    break
    
            if new_state:
              # self._log.debug(" === new %s: %s" % (uid, new_state))
                state = new_state

        if not state:
            # all for nothing
            return None

        if state in [rps.DONE, rps.FAILED, rps.CANCELED]:
          # self._log.debug(" === FIN %s: %s" % (uid, state))
            cache['final'] = True
            cache['last']  = state

        # ok, we actually have something to update
      # self._log.debug(" === set %s: %s" % (uid, state))
        cache['last'] = state
        return state


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

        res = cinfo['bulk'].execute()
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
    def idle_cb(self):

        action = 0
        with self._lock:
            for cname in self._cinfo:
                action += self._timed_bulk_execute(self._cinfo[cname])

        return bool(action)


    # --------------------------------------------------------------------------
    #
    def state_cb(self, topic, msg):
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
        #
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

        # we need to update something -- prepafe update doc
        update_dict         = dict()
        update_dict['$set'] = dict()

        # if the thing is scheduled to be pushed completely, then do so.
        if thing.get('$all'):
            
            # get rid of the marker
            # we can also savely ignore the '$set' marker
            del(thing['$all'])
            del(thing['$set'])

            for key,val in thing.iteritems():
                update_dict['$set'][key] = val

        # if the thing has keys specified which are specifically to be set
        # in the database, then do so
        elif thing.get('$set'):

            # get rid of the marker
            update_dict = {'$set' : {}}

            for key in thing.get('$set', []):
                update_dict['$set'][key] = thing[key]


        # make sure our state transitions adhere to the state models
        state = self._ordered_state_update(thing, state, timestamp)
        update_dict['$set']['state'] = state

        # we never set _id
        if '_id' in update_dict['$set']:
            del(update_dict['$set']['_id'])


        # check if we handled the collection before.  If not, initialize
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
            import pprint
            print '--> %s: %s' % (uid, pprint.pformat(update_dict))

            cinfo['uids'].append([uid, ttype, state])
            cinfo['bulk'].find  ({'uid'  : uid, 
                                  'type' : ttype}) \
                         .update(update_dict)
            self._prof.prof('bulk', msg='bulked (%s)' % state, uid=uid)

            # attempt a timed update
            self._timed_bulk_execute(cinfo)


# ------------------------------------------------------------------------------


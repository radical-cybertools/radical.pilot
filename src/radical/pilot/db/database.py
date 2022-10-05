# pylint: disable=protected-access

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'


import time
import pymongo

import radical.utils     as ru

from .. import states    as rps


# ------------------------------------------------------------------------------
#
class DBSession(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, dburl, cfg, log, connect=True):
        '''
        Creates a new session

        A session is a MongoDB collection which contains documents of
        different types:

        session : document describing this rp.Session (singleton)
        pmgr    : document describing a rp.PilotManager
        pilots  : document describing a rp.Pilot
        tmgr    : document describing a rp.TaskManager
        tasks   : document describing a rp.Task
        '''

        self._dburl      = dburl
        self._log        = log
        self._mongo      = None
        self._db         = None
        self._created    = time.time()
        self._connected  = None
        self._closed     = None
        self._c          = None
        self._can_remove = False

        if not connect:
            return

        # mpongodb_connect wants a string at the moment
        self._mongo, self._db, _, _, _ = ru.mongodb_connect(str(dburl))

        if not self._mongo or not self._db:
            raise RuntimeError('Could not connect to database at %s' % dburl)

        self._connected = time.time()

        self._c = self._db[sid]  # creates collection (lazily)

        # If session exists, we assume this is a reconnect, otherwise we create
        # the session entry.
        # NOTE: hell will break loose if session IDs are not unique!
        if not self._c.count():

            # make 'uid', 'type' and 'state' indexes, as we frequently query
            # based on combinations of those.  Only 'uid' is unique
            pma = pymongo.ASCENDING
            self._c.create_index([('uid',   pma)], unique=True,  sparse=False)
            self._c.create_index([('type',  pma)], unique=False, sparse=False)
            self._c.create_index([('state', pma)], unique=False, sparse=False)

            # insert the session doc
            self._can_delete = True
            self._c.insert({'type'      : 'session',
                            '_id'       : sid,
                            'uid'       : sid,
                            'cfg'       : cfg.as_dict(),
                            'created'   : self._created,
                            'connected' : self._connected})
            self._can_remove = True

        else:
            docs = self._c.find({'type' : 'session',
                                 'uid'  : sid})
            if not docs.count():
                raise ValueError('cannot reconnect to session %s' % sid)

            doc = docs[0]
            self._can_delete = False
            self._created    = doc['created']
            self._connected  = time.time()

            # FIXME: get bridge addresses from DB?  If not, from where?


    # --------------------------------------------------------------------------
    #
    @property
    def dburl(self):
        '''
        Returns the session db url.
        '''
        return self._dburl


    # --------------------------------------------------------------------------
    #
    def get_db(self):
        '''
        Returns the session db.
        '''
        return self._db


    # --------------------------------------------------------------------------
    #
    @property
    def created(self):
        '''
        Returns the creation time
        '''
        return self._created


    # --------------------------------------------------------------------------
    #
    @property
    def connected(self):
        '''
        Returns the connection time
        '''
        return self._connected


    # --------------------------------------------------------------------------
    #
    @property
    def closed(self):
        '''
        Returns the close time
        '''
        return self._closed


    # --------------------------------------------------------------------------
    #
    @property
    def is_connected(self):

        return (self._connected is not None)


    # --------------------------------------------------------------------------
    #
    def close(self, delete=True):
        '''
        close the session
        '''
        if self.closed:
            return None
          # raise RuntimeError('No active session.')

        self._closed = time.time()

        # only the Session which created the collection can delete it!
        if delete and self._can_remove:
            self._log.info('delete session')
            self._c.drop()

        if self._mongo:
            self._mongo.close()

        self._closed = time.time()
        self._c = None


    # --------------------------------------------------------------------------
    #
    def insert_pmgr(self, pmgr_doc):
        '''
        Adds a pilot managers doc
        '''
        if self.closed:
            return None
          # raise Exception('No active session.')

        pmgr_doc['_id']  = pmgr_doc['uid']
        pmgr_doc['type'] = 'pmgr'

        # FIXME: evaluate retval
        self._c.insert(ru.as_dict(pmgr_doc))



    # --------------------------------------------------------------------------
    #
    def get_pmgrs(self, pmgr_ids=None):
        '''
        Get pilot manager docs
        '''

        if self.closed:
            raise Exception('No active session.')


        if not pmgr_ids:
            cursor = self._c.find({'type' : 'pmgr'})

        else:
            if not isinstance(pmgr_ids, list):
                pmgr_ids = [pmgr_ids]

            cursor = self._c.find({'type' : 'pmgr',
                                   'uid'  : {'$in': pmgr_ids}})

        # make sure we return every pmgr doc only once
        # https://www.quora.com/How-did-mongodb-return-duplicated-but-different-documents
        ret  = {doc['uid'] : doc for doc in cursor}
        docs = ret.values()

        return docs


    # --------------------------------------------------------------------------
    #
    def insert_pilots(self, pilot_docs):
        '''
        Adds new pilot documents to the database.
        '''

        # FIXME: explicit bulk vs. insert(multi=True)

        if self.closed:
            return None
          # raise Exception('No active session.')

        bulk = self._c.initialize_ordered_bulk_op()

        for doc in pilot_docs:
            doc['_id']     = doc['uid']
            doc['type']    = 'pilot'
            doc['control'] = 'pmgr'
            doc['states']  = [doc['state']]
            doc['cmds']    = list()
            bulk.insert(ru.as_dict(doc))

        try:
            bulk.execute()

        except pymongo.errors.OperationFailure as e:
            self._log.exception('pymongo error: %s' % e.details)
            raise RuntimeError ('pymongo error: %s' % e.details) from e


    # --------------------------------------------------------------------------
    #
    def pilot_command(self, cmd, arg=None, pids=None):
        '''
        send a command and arg to a set of pilots
        '''

        if self.closed:
            return None
          # raise Exception('session is closed')

        if not self._c:
            raise Exception('session is disconnected ')

        if pids and not isinstance(pids, list):
            pids = [pids]

        try:
            cmd_spec = {'cmd': cmd,
                    'arg': arg}

            self._log.debug('insert cmd: %s %s %s', pids, cmd, arg)

            # FIXME: evaluate retval
            if pids:
                self._c.update({'type'  : 'pilot',
                                'uid'   : {'$in' : pids}},
                               {'$push' : {'cmds': cmd_spec}},
                               multi=True)
            else:
                self._c.update({'type'  : 'pilot'},
                               {'$push' : {'cmds': cmd_spec}},
                               multi=True)

        except pymongo.errors.OperationFailure as e:
            self._log.exception('pymongo error: %s' % e.details)
            raise RuntimeError ('pymongo error: %s' % e.details) from e


    # --------------------------------------------------------------------------
    #
    def pilot_rpc(self, pid, rpc, args=None):
        '''
        Send am RPC command and arguments to a pilot and wait for the response.
        This is a synchronous operation at this point, and it is not thread safe
        to have multiple concurrent RPC calls.
        '''

        if self.closed:
            raise Exception('session is closed')

        if not self._c:
            raise Exception('session is disconnected ')

        try:
            rpc_id  = ru.generate_id('rpc')
            rpc_req = {'uid' : rpc_id,
                       'rpc' : rpc,
                       'arg' : args}

            # send the request to the pilot - this replaces any former request
            # not yet picked up by the pilot.
            self._c.update({'type'  : 'pilot',
                            'uid'   : pid},
                           {'$set'  : {'rpc_req': rpc_req}})

            # wait for reply to arrive
            while True:
                time.sleep(0.1)  # FIXME: tuneable

                # pick up any response and purge it from the DB
                retdoc = self._c.find_and_modify(
                            query ={'uid' : pid},
                            fields=['rpc_res'],
                            update={'$set': {'rpc_res': None}})

                if not retdoc:
                    # no response found
                    continue

                rpc_res = retdoc.get('rpc_res')
                if not rpc_res:
                    # response was empty
                    continue

                self._log.debug('rpc result: %s', rpc_res['ret'])

                if rpc_res['ret']:
                    # NOTE: we could raise a pickled exception - but how useful
                    #       would a pilot exception stack be on the client side?
                    raise RuntimeError('rpc failed: %s' % rpc_res['err'])

                return rpc_res['ret']


        except pymongo.errors.OperationFailure as e:
            self._log.exception('pymongo error: %s' % e.details)
            raise RuntimeError ('pymongo error: %s' % e.details) from e


    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pmgr_uid=None, pilot_ids=None):
        '''
        Get a pilot
        '''
        if self.closed:
            raise Exception('No active session.')

        if not pmgr_uid and not pilot_ids:
            raise Exception("pmgr_uid and pilot_ids can't both be None.")

        if not pilot_ids:
            cursor = self._c.find({'type' : 'pilot',
                                   'pmgr' : pmgr_uid})
        else:

            if not isinstance(pilot_ids, list):
                pilot_ids = [pilot_ids]

            cursor = self._c.find({'type' : 'pilot',
                                   'uid'  : {'$in': pilot_ids}})

        # make sure we return every pilot doc only once
        # https://www.quora.com/\
        #         How-did-mongodb-return-duplicated-but-different-documents
        ret  = {doc['uid'] : doc for doc in cursor}
        docs = list(ret.values())

        # for each doc, we make sure the pilot state is according to the state
        # model, ie. is the largest of any state the pilot progressed through
        for doc in docs:
            doc['state'] = rps._pilot_state_collapse(doc['states'])

        return docs


    # --------------------------------------------------------------------------
    #
    def get_tasks(self, tmgr_uid, task_ids=None):
        '''
        Get yerself a bunch of tasks.

        return dict {uid:task}
        '''
        if self.closed:
            return None
          # raise Exception('No active session.')

        # we only pull tasks which are not yet owned by the tmgr

        if not task_ids:
            cursor = self._c.find({'type'   : 'task',
                                   'tmgr'   : tmgr_uid,
                                   'control': {'$ne' : 'tmgr'},
                                   })

        else:
            cursor = self._c.find({'type'   : 'task',
                                   'tmgr'   : tmgr_uid,
                                   'uid'    : {'$in' : task_ids},
                                   'control': {'$ne' : 'tmgr'  },
                                   })

        # make sure we return every task doc only once
        # https://www.quora.com/ \
        #         How-did-mongodb-return-duplicated-but-different-documents
        ret = {doc['uid'] : doc for doc in cursor}
        docs = list(ret.values())

        # for each doc, we make sure the task state is according to the state
        # model, ie. is the largest of any state the task progressed through
        for doc in docs:
            doc['state'] = rps._task_state_collapse(doc['states'])

        return docs


    # --------------------------------------------------------------------------
    #
    def insert_tmgr(self, tmgr_doc):
        '''
        Adds a task managers document
        '''
        if self.closed:
            return None
          # raise Exception('No active session.')

        tmgr_doc['_id']  = tmgr_doc['uid']
        tmgr_doc['type'] = 'tmgr'

        # FIXME: evaluate retval
        self._c.insert(ru.as_dict(tmgr_doc))



    # --------------------------------------------------------------------------
    #
    def get_tmgrs(self, tmgr_ids=None):
        '''
        Get task manager docs
        '''

        if self.closed:
            raise Exception('No active session.')


        if not tmgr_ids:
            cursor = self._c.find({'type' : 'tmgr'})

        else:
            if not isinstance(tmgr_ids, list):
                tmgr_ids = [tmgr_ids]

            cursor = self._c.find({'type' : 'tmgr',
                                   'uid'  : {'$in': tmgr_ids}})

        # make sure we return every tmgr doc only once
        # https://www.quora.com/How-did-mongodb-return-duplicated-but-different-documents
        ret  = {doc['uid'] : doc for doc in cursor}
        docs = ret.values()

        return docs


    # --------------------------------------------------------------------------
    #
    def insert_tasks(self, task_docs):
        '''
        Adds new task documents to the database.
        '''

        # FIXME: explicit bulk vs. insert(multi=True)

        if self.closed:
            return None
          # raise Exception('No active session.')

        # We can only insert DB bulks up to a certain size, which is hardcoded
        # here.  In principle, the update should go to the update worker anyway
        # -- but as long as we use the DB as communication channel, we need to
        # make sure that the insert is executed before handing off control over
        # the task to other components, thus the synchronous insert call.
        # (FIXME)
        bcs = 1024  # bulk_collection_size
        cur = 0     # bulk index

        while True:

            subset = task_docs[cur : cur + bcs]
            bulk   = self._c.initialize_ordered_bulk_op()
            cur   += bcs

            if not subset:
                # all tasks are done
                break

            for doc in subset:
                doc['_id']     = doc['uid']
                doc['control'] = 'tmgr'
                doc['states']  = [doc['state']]
                doc['cmd']     = list()
                bulk.insert(ru.as_dict(doc))

            try:
                res = bulk.execute()
                self._log.debug('bulk task insert result: %s', res)
                # FIXME: evaluate res

            except pymongo.errors.OperationFailure as e:
                self._log.exception('pymongo error: %s' % e.details)
                raise RuntimeError ('pymongo error: %s' % e.details) from e


    # --------------------------------------------------------------------------
    #
    def tailed_find(self, collection, pattern, fields, cb, cb_data=None):
        '''
        open a collection in capped mode, and create a tailing find-cursor with
        the given pattern on it.  For all returned documents, invoke the given
        callback as:

          cb(docs, cb_data=None)

        where 'docs' is a list of None, one or more matching documents.
        Specifically, the callback is also invoked when *no* document currently
        matches the pattern.  Documents are returned as partial docs, which only
        contain the set of field names given.  If 'fields' is an empty list
        though, then complete documents are returned.

        This method is blocking, and will never return.  It is adviseable to
        call it in a thread.
        '''
        raise NotImplementedError('duh!')


    # --------------------------------------------------------------------------
    #
    def tailed_control(self, collection, control, pattern, cb, cb_data=None):
        '''
        open a collection in capped mode, and create a tailing find-cursor on
        it, where the find searches for the pattern:

          pattern.extent({ 'control' : control + '_pending' })

        For any matching document, the 'control' field is updated to 'control',
        ie. the 'pending' postfix is removed.  The resulting documents are
        passed to the given callback as

          cb(docs, cb_data=None)

        where 'docs' is a list of None, one or more matching documents.
        Specifically, the callback is also invoked when *no* document currently
        matches the pattern.  The documents are returned in full, ie. with all
        available fields.

        This method is blocking, and will never return.  It is adviseable to
        call it in a thread.
        '''
        raise NotImplementedError('duh!')


# ------------------------------------------------------------------------------


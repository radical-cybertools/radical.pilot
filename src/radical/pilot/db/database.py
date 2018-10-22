
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import sys
import copy
import time
import pymongo

import radical.utils  as ru
import saga           as rs

from .. import states as rps
from .. import utils  as rpu


# ------------------------------------------------------------------------------
#
class DB(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, cfg, connect=True):
        '''
        Creates a new database connection 

        A session is a MongoDB collection which contains documents of
        different types:

        session : document describing this rp.Session (singleton)
        pmgr    : document describing a rp.PilotManager 
        pilots  : document describing a rp.Pilot
        umgr    : document describing a rp.UnitManager
        units   : document describing a rp.Unit
        '''

        self._session    = session
        self._sid        = self._session.uid
        self._log        = self._session.get_logger(name=self._sid)

        self._cfg        = cfg
        self._dburl      = ru.Url(self._cfg['dburl'])
        self._mongo      = None
        self._db         = None
        self._created    = time.time()
        self._connected  = None
        self._closed     = None
        self._c          = None

        if not connect:
            return

        # the url path element is required as db name
        if  self._dburl.path in [None, '', '/']:
            raise ValueError("db name missing in DBURL '%s'" % self._dburl)

        self._log.info("using database %s" % self._dburl)

        # mpongodb_connect wants a string at the moment
        self._mongo, self._db, _, _, _ = ru.mongodb_connect(str(self._dburl))

        if not self._mongo or not self._db:
            raise RuntimeError('DB connection error on %s' % self._dburl)

        self._c = self._db[self._sid]  # creates collection (lazily)


        # If session exists, we assume this is a reconnect, otherwise we create
        # the session entry.
        if not self._c.count():

            # make 'uid', 'type' and 'state' indexes, as we frequently query
            # based on combinations of those.  Only 'uid' is unique
            self._c.create_index([('uid',   pymongo.ASCENDING)], 
                                 unique=True,  sparse=False)
            self._c.create_index([('type',  pymongo.ASCENDING)],
                                 unique=False, sparse=False)
            self._c.create_index([('state', pymongo.ASCENDING)],
                                 unique=False, sparse=False)

            py_version_detail = sys.version.replace("\n", " ")
            version_info = {'radical_stack' : {'rp': rpu.version_detail,
                                               'rs':  rs.version_detail,
                                               'ru':  ru.version_detail,
                                               'py':  py_version_detail}}

            # insert the session doc
            self._c.insert({'type'      : 'session',
                            '_id'       : self._sid,
                            'uid'       : self._sid,
                            'cfg'       : copy.deepcopy(cfg),
                            'created'   : self._created,
                            'connected' : self._connected, 
                            'metadata'  : [version_info]})
        else:
            docs = self._c.find({'type' : 'session', 
                                 'uid'  : self._sid})
            if not docs.count():
                raise ValueError('cannot reconnect to session %s' % self._sid)

            doc = docs[0]
            self._created    = doc['created']

        self._connected  = time.time()


    # --------------------------------------------------------------------------
    #
    @property
    def dburl(self):
        """ 
        Returns the session db url.
        """
        return self._dburl


    # --------------------------------------------------------------------------
    #
    def get_db(self):
        """
        Returns the session db.
        """
        return self._db


    # --------------------------------------------------------------------------
    #
    @property
    def created(self):
        """
        Returns the creation time
        """
        return self._created


    # --------------------------------------------------------------------------
    #
    @property
    def connected(self):
        """
        Returns the connection time
        """
        return self._connected


    # --------------------------------------------------------------------------
    #
    @property 
    def closed(self): 
        """ 
        Returns the close time 
        """ 
        return self._closed


    # --------------------------------------------------------------------------
    #
    @property
    def is_connected(self):

        return (self._connected is not None)


    # --------------------------------------------------------------------------
    #
    def close(self, delete=False):
        """ 
        close the session
        """
        if self._closed:
            raise RuntimeError('No active session.')

        self._log.debug('closing')
        self._closed = time.time()

        if delete:
            self._log.info('delete session')
            self._c.drop()

        if self._mongo:
            self._mongo.close()

        self._c = None


    # --------------------------------------------------------------------------
    #
    def insert_metadata(self, metadata):
        '''
        Insert metadata into an active session.
        '''

        if not isinstance(metadata, dict):
            raise TypeError('metadata must be a dict!')

        self._c.update({'type'  : 'session',
                        'uid'   : self._sid},
                       {'$push' : {'metadata': metadata}})


    # --------------------------------------------------------------------------
    #
    def insert_pmgr(self, pmgr_doc):
        '''
        Adds a pilot managers doc
        '''

        if self._closed:
            return None
          # raise Exception('No active session.')

        pmgr_doc['_id']  = pmgr_doc['uid']
        pmgr_doc['type'] = 'pmgr'

        _ = self._c.insert(pmgr_doc)

        # FIXME: evaluate result


    # --------------------------------------------------------------------------
    #
    def insert_pilots(self, pilot_docs):
        """
        Adds new pilot documents to the database.
        """

        # FIXME: explicit bulk vs. insert(multi=True)

        if self._closed:
            return None
          # raise Exception('No active session.')

        bulk = self._c.initialize_ordered_bulk_op()

        for doc in pilot_docs:
            doc['_id']     = doc['uid']
            doc['type']    = 'pilot'
            doc['control'] = 'pmgr'
            doc['states']  = [doc['state']]
            doc['cmd']     = list()
            bulk.insert(doc)

        try:
            res = bulk.execute()
            self._log.debug('bulk pilot insert result: %s', res)
            # FIXME: evaluate res

        except pymongo.errors.OperationFailure as e:
            self._log.exception('pymongo error: %s' % e.details)
            raise RuntimeError ('pymongo error: %s' % e.details)


    # --------------------------------------------------------------------------
    #
    def pilot_command(self, cmd, arg, pids=None):
        """
        send a command and arg to a set of pilots
        """

        if self._closed:
            return None
          # raise Exception('session is closed')

        if not self._c:
            raise Exception('session is disconnected ')

        if pids and not isinstance(pids, list):
            pids = [pids]

        try:
            cmd_spec = {'cmd' : cmd,
                        'arg' : arg}

            # FIXME: evaluate res
            if pids:
                _ = self._c.update({'type'  : 'pilot',
                                    'uid'   : {'$in' : pids}},
                                   {'$push' : {'cmd' : cmd_spec}},
                                   multi=True)
            else:
                _ = self._c.update({'type'  : 'pilot'},
                                   {'$push' : {'cmd' : cmd_spec}},
                                   multi=True)

        except pymongo.errors.OperationFailure as e:
            self._log.exception('pymongo error: %s' % e.details)
            raise RuntimeError ('pymongo error: %s' % e.details)


    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pmgr_uid=None, pilot_ids=None):
        """
        Get a pilot
        """
        if self._closed:
            return
          # raise Exception('No active session.')

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
        docs = ret.values()

        # for each doc, we make sure the pilot state is according to the state
        # model, ie. is the largest of any state the pilot progressed through
        for doc in docs:
            doc['state'] = rps._pilot_state_collapse(doc['states'])

        return docs


    # --------------------------------------------------------------------------
    #
    def get_units(self, umgr_uid, unit_ids=None):
        """
        Get yerself a bunch of compute units.

        return dict {uid:unit}
        """
        if self._closed:
            return None
          # raise Exception("No active session.")

        # we only pull units which are not yet owned by the umgr

        if not unit_ids:
            cursor = self._c.find({'type'   : 'unit',
                                   'umgr'   : umgr_uid,
                                   'control': {'$ne' : 'umgr'},
                                   })

        else:
            cursor = self._c.find({'type'   : 'unit',
                                   'umgr'   : umgr_uid,
                                   'uid'    : {'$in' : unit_ids},
                                   'control': {'$ne' : 'umgr'  },
                                   })

        # make sure we return every unit doc only once
        # https://www.quora.com/How-did-mongodb-return-duplicated-but-different-documents
        ret = {doc['uid'] : doc for doc in cursor}
        docs = ret.values()

        # for each doc, we make sure the unit state is according to the state
        # model, ie. is the largest of any state the unit progressed through
        for doc in docs:
            doc['state'] = rps._unit_state_collapse(doc['states'])

        return docs


    # --------------------------------------------------------------------------
    #
    def insert_umgr(self, umgr_doc):
        """ 
        Adds a unit managers document
        """
        if self._closed:
            return None
          # raise Exception('No active session.')

        umgr_doc['_id']  = umgr_doc['uid']
        umgr_doc['type'] = 'umgr'

        _ = self._c.insert(umgr_doc)

        # FIXME: evaluate result


    # --------------------------------------------------------------------------
    #
    def insert_units(self, unit_docs):
        """
        Adds new unit documents to the database.
        """

        # FIXME: explicit bulk vs. insert(multi=True)

        if self._closed:
            return None
          # raise Exception('No active session.')

        # We can only insert DB bulks up to a certain size, which is hardcoded
        # here.  In principle, the update should go to the update worker anyway
        # -- but as long as we use the DB as communication channel, we need to
        # make sure that the insert is executed before handing off control over
        # the unit to other components, thus the synchronous insert call.
        # (FIXME)
        bcs = 1024  # bulk_collection_size
        cur = 0     # bulk index

        while True:

            subset = unit_docs[cur : cur + bcs]
            bulk   = self._c.initialize_ordered_bulk_op()
            cur   += bcs

            if not subset:
                # all units are done
                break

            for doc in subset:
                doc['_id']     = doc['uid']
                doc['type']    = 'unit'
                doc['control'] = 'umgr'
                doc['states']  = [doc['state']]
                doc['cmd']     = list()
                bulk.insert(doc)

            try:
                res = bulk.execute()
                self._log.debug('bulk unit insert result: %s', res)
                # FIXME: evaluate res

            except pymongo.errors.OperationFailure as e:
                self._log.exception('pymongo error')
                raise RuntimeError('pymongo error: %s' % e.details)

    # --------------------------------------------------------------------------
    #
    def tailed_find(self, collection, pattern, fields, cb, cb_data=None):
        """
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
        """
        raise NotImplementedError('duh!')


    # --------------------------------------------------------------------------
    #
    def tailed_control(self, collection, control, pattern, cb, cb_data=None):
        """
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
        """
        raise NotImplementedError('duh!')


# ------------------------------------------------------------------------------


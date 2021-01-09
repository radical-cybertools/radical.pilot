
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.saga            as rs
import radical.saga.filesystem as rsfs
import radical.utils           as ru

from .. import utils           as rpu
from .. import constants       as rpc
from .. import states          as rps


# ------------------------------------------------------------------------------
#
class Stager(rpu.Worker):
    '''
    A Stager will receive staging requests, perform the respective staging
    actions, and will respond with a completion message.  At the moment, the
    stager uses the CONTROL pubsub to communicate with components which require
    its staging capabilities.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._cache_lock    = ru.Lock()
        self._saga_fs_cache = dict()

        rpu.Worker.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._sid   = self._cfg['sid']
        self._dburl = self._cfg['dburl']

        self.register_input(rps.NEW, rpc.STAGER_REQUEST_QUEUE, self.work)
        self.register_publisher(rpc.STAGER_RESPONSE_PUBSUB)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg, session):

        return cls(cfg, session)


    # --------------------------------------------------------------------------
    #
    def work(self, sds):
        '''
        receive staging requests, execute them, and reply with completion
        messages on the control channel
        '''

        self._log.debug('stager got  sds: %s ', [sd['uid'] for sd in sds])

        # handle the staging descriptions as bulk and report collective
        # completion.
        #
        # NOTE: should we allow bulk reshuffeling here?

        self._handle_staging(sds)

        # NOTE: we send completion notifications via pubsub, as we have no
        #       direct channel to whoever requested the trransfer.  That may
        #       raise scalability problems in the long run.
        self.publish(rpc.STAGER_RESPONSE_PUBSUB,
                     {'cmd': 'staging_result',
                      'arg': {'sds': sds}})
        self._log.debug('stager done sds: %s ', [sd['uid'] for sd in sds])

        return True


    # --------------------------------------------------------------------------
    #
    def _handle_staging(self, sds):

        # Iterate over all directives
        for sd in sds:

            # TODO: respect flags in directive

            action  = sd['action']
            flags   = sd['flags']
            uid     = sd['uid']
            src     = sd['source']
            tgt     = sd['target']
            prof_id = sd.get('prof_id')   # staging on behalf of this entity

            assert(action in [rpc.COPY, rpc.LINK, rpc.MOVE, rpc.TRANSFER])

            self._prof.prof('staging_start', uid=prof_id, msg=uid)

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                self._prof.prof('staging_fail', uid=prof_id, msg=uid)
                raise ValueError("invalid action '%s' on stager" % action)

            self._log.info('transfer %s', src)
            self._log.info('      to %s', tgt)

            # open the staging directory for the target, and cache it
            # url used for cache: tgt url w/o path
            tmp      = rs.Url(tgt)
            tmp.path = '/'
            key      = str(tmp)

            with self._cache_lock:
                if key in self._saga_fs_cache:
                    fs = self._saga_fs_cache[key]

                else:
                    fs = rsfs.Directory(key, session=self._session)
                    self._saga_fs_cache[key] = fs

            fs.copy(src, tgt, flags=flags)

            sd['state'] = rps.DONE

            self._prof.prof('staging_stop', uid=prof_id, msg=uid)


# ------------------------------------------------------------------------------


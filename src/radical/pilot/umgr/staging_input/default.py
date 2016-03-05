
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import shutil

import saga          as rs
import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import UMGRStagingInputComponent


# ==============================================================================
#
class Default(UMGRStagingInputComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        UMGRStagingInputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        # we keep a cache of SAGA dir handles
        self._cache = dict()

        self.register_input(rps.UMGR_STAGING_INPUT_PENDING,
                            rpc.UMGR_STAGING_INPUT_QUEUE, self.work)

        # FIXME: this queue is inaccessible, needs routing via mongodb
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING, None)


    # --------------------------------------------------------------------------
    #
    def work(self, unit):

        self.advance(unit, rps.UMGR_STAGING_INPUT, publish=True, push=False)

        uid = unit['uid']

        # check if we have any staging directives to be enacted in this
        # component
        actionables = list()
        for entry in unit.get('input_staging', []):

            action = entry['action']
            flags  = entry['flags']
            src    = ru.Url(entry['source'])
            tgt    = ru.Url(entry['target'])

            if action in [rpc.TRANSFER] and src.schema in ['file']:
                actionables.append([src, tgt, flags])

        if actionables:

            # we have actionable staging directives, and thus we need a unit
            # sandbox.
            sandbox = rs.Url(unit["sandbox"])
            self._prof.prof("create sandbox", msg=str(sandbox))

            # url used for cache (sandbox url w/o path)
            tmp = rs.Url(sandbox)
            tmp.path = '/'
            key = str(tmp)

            if key not in self._cache:
                self._cache[key] = rs.filesystem.Directory(tmp, 
                        session=self._session)

            saga_dir = self._cache[key]
            saga_dir.make_dir(sandbox, flags=rs.filesystem.CREATE_PARENTS)

            self._prof.prof("created sandbox", uid=uid)


            # Loop over all transfer directives and execute them.
            for src, tgt, flags in actionables:

                self._prof.prof('umgr staging in', msg=src, uid=uid)

                if rpc.CREATE_PARENTS in flags:
                    copy_flags = rs.filesystem.CREATE_PARENTS
                else:
                    copy_flags = 0

                saga_dir.copy(src, target, flags=copy_flags)

                self._prof.prof('umgr staged  in', msg=src, uid=uid)


        # all staging is done -- pass on to the agent
        # At this point, the unit will leave the umgr, we thus dump it
        # completely into the DB
        unit['$all'] = True
        self.advance(unit, rps.AGENT_STAGING_INPUT_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


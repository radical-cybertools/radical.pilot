
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

from .base import UMGRStagingOutputComponent


# ==============================================================================
#
class Default(UMGRStagingOutputComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        UMGRStagingOutputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        # we keep a cache of SAGA dir handles
        self._cache = dict()

        self.register_input(rps.UMGR_STAGING_OUTPUT_PENDING, 
                            rpc.UMGR_STAGING_OUTPUT_QUEUE, self.work)

        # we don't need an output queue -- units will be final


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.UMGR_STAGING_OUTPUT, publish=True, push=False)

        for unit in units:

            self._handle_unit(unit)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit):

        uid = unit['uid']

        # check if we have any staging directives to be enacted in this
        # component
        actionables = list()
        for entry in unit.get('out_staging', []):

            action = entry['action']
            flags  = entry['flags']
            src    = ru.Url(entry['source'])
            tgt    = ru.Url(entry['target'])

            if action in [rpc.TRANSFER] and tgt.schema in ['file']:
                actionables.append([src, tgt, flags])

        if actionables:

            # we have actionable staging directives
            # url used for cache (sandbox url w/o path)
            tmp = rs.Url(unit["sandbox"])
            tmp.path = '/'
            key = str(tmp)

            if key not in self._cache:
                self._cache[key] = rs.filesystem.Directory(tmp, 
                        session=self._session)

            saga_dir = self._cache[key]


            # Loop over all transfer directives and execute them.
            for src, tgt, flags in actionables:

                self._prof.prof('umgr staging out', msg=src, uid=uid)

                if rpc.CREATE_PARENTS in flags:
                    copy_flags = rs.filesystem.CREATE_PARENTS
                else:
                    copy_flags = 0

                saga_dir.copy(src, tgt, flags=copy_flags)

                self._prof.prof('umgr staged  out', msg=src, uid=uid)


        # all staging is done -- at this point the unit is final
        unit['$all']    = True
        unit['control'] = None
        self.advance(unit, unit['target_state'], publish=True, push=True)


# ------------------------------------------------------------------------------


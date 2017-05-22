
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

        for unit in units:
            self._log.debug(" === work on %s in %s @ %s", unit['uid'], unit['state'], unit['control'])
        self.advance(units, rps.UMGR_STAGING_OUTPUT, publish=True, push=False)

        # we first filter out any units which don't need any output staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.
        
        no_staging_units = list()
        staging_units    = list()

        for unit in units:

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in unit['description'].get('output_staging', []):

                action = sd['action']
                flags  = sd['flags']
                src    = ru.Url(sd['source'])
                tgt    = ru.Url(sd['target'])

                if action in [rpc.TRANSFER] and src.schema in ['file']:
                    actionables.append([src, tgt, flags])

            if actionables:
                staging_units.append([unit, actionables])
            else:
                no_staging_units.append(unit)


        if no_staging_units:

            # nothing to stage -- transition into final state.
            for unit in no_staging_units:
                unit['$all']    = True
                unit['control'] = None
                unit['state']   = unit['target_state']

            self.advance(no_staging_units, publish=True, push=True)

        for unit,actionables in staging_units:
            self._handle_unit(unit, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit, actionables):

        uid = unit['uid']

        # we have actionable staging directives
        # url used for cache (sandbox url w/o path)
        sandbox  = rs.Url(unit["unit_sandbox"])
        tmp      = rs.Url(unit["unit_sandbox"])
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

            self._log.debug('')
            self._log.debug(src)
            self._log.debug(tgt)

            saga_dir.copy(src, tgt, flags=copy_flags)

            self._prof.prof('umgr staged  out', msg=src, uid=uid)


        # all staging is done -- at this point the unit is final
        unit['$all']    = True
        unit['control'] = None
        unit['state']   = unit['target_state']
        self.advance(unit, publish=True, push=True)


# ------------------------------------------------------------------------------


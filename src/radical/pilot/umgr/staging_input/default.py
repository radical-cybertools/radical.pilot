
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
    """
    This component performs all umgr side input staging directives for compute
    units.  It gets units from the umgr_staging_input_queue, in
    UMGR_STAGING_INPUT_PENDING state, will advance them to UMGR_STAGING_INPUT
    state while performing the staging, and then moves then to the respective
    final state.
    """

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
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.UMGR_STAGING_INPUT, publish=True, push=False)

        # we first filter out any units which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.
        
        no_staging_units = list()
        staging_units    = list()

        for unit in units:

            # no matter if we perform any staging or not, we will push the full
            # unit info to the DB on the next advance, and will pass control to
            # the agent.
            unit['$all']    = True
            unit['control'] = 'agent_pending'

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in unit['description'].get('input_staging', []):

                action = sd['action']
                flags  = sd['flags']
                src    = ru.Url(sd['source'])
                tgt    = ru.Url(sd['target'])

                if action == rpc.TRANSFER and src.schema == 'file':
                    actionables.append([src, tgt, flags])

            if actionables:
                staging_units.append([unit, actionables])
            else:
                no_staging_units.append(unit)


        if no_staging_units:
            self.advance(no_staging_units, rps.AGENT_STAGING_INPUT_PENDING, 
                         publish=True, push=True)

        for unit,actionables in staging_units:
            self._handle_unit(unit, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit, actionables):

        uid = unit['uid']

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

            # FIXME: this should be a proper test for absoluteness of URL
            if not tgt.path.startswith('/'):
                tgt.path = '%s/%s' % (sandbox.path, tgt.path)

            saga_dir.copy(src, tgt, flags=copy_flags)

            self._prof.prof('umgr staged  in', msg=src, uid=uid)

        # staging is done, we can advance the unit at last
        self.advance(unit, rps.AGENT_STAGING_INPUT_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


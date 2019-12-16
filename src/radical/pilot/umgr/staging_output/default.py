
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.saga as rs

from ...   import states             as rps
from ...   import constants          as rpc
from ...   import staging_directives as rpsd

from .base import UMGRStagingOutputComponent


# ------------------------------------------------------------------------------
#
class Default(UMGRStagingOutputComponent):
    """
    This component performs all umgr side output staging directives for compute
    units.  It gets units from the umgr_staging_output_queue, in
    UMGR_STAGING_OUTPUT_PENDING state, will advance them to UMGR_STAGING_OUTPUT
    state while performing the staging, and then moves then to the respective
    final state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        UMGRStagingOutputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # we keep a cache of SAGA dir handles
        self._cache = dict()

        self.register_input(rps.UMGR_STAGING_OUTPUT_PENDING,
                            rpc.UMGR_STAGING_OUTPUT_QUEUE, self.work)

        # we don't need an output queue -- units will be final


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        for key in self._cache:
            self._cache[key].close()


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.UMGR_STAGING_OUTPUT, publish=True, push=False)

        # we first filter out any units which don't need any output staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.

        no_staging_units = list()
        staging_units    = list()

        for unit in units:

            # no matter if we perform any staging or not, we will push the full
            # unit info to the DB on the next advance, since the units will be
            # final
            unit['$all']    = True
            unit['control'] = None

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in unit['description'].get('output_staging', []):

                if sd['action'] == rpc.TRANSFER:
                    actionables.append(sd)

            if actionables:
                staging_units.append([unit, actionables])
            else:
                no_staging_units.append(unit)


        if no_staging_units:

            # nothing to stage -- transition into final state.
            for unit in no_staging_units:
                unit['state'] = unit['target_state']
            self.advance(no_staging_units, publish=True, push=True)

        for unit,actionables in staging_units:
            self._handle_unit(unit, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit, actionables):

        uid = unit['uid']

        src_context = {'pwd'      : unit['unit_sandbox'],       # !!!
                       'unit'     : unit['unit_sandbox'],
                       'pilot'    : unit['pilot_sandbox'],
                       'resource' : unit['resource_sandbox']}
        tgt_context = {'pwd'      : os.getcwd(),                # !!!
                       'unit'     : unit['unit_sandbox'],
                       'pilot'    : unit['pilot_sandbox'],
                       'resource' : unit['resource_sandbox']}

        # url used for cache (sandbox url w/o path)
        tmp      = rs.Url(unit["unit_sandbox"])
        tmp.path = '/'
        key      = str(tmp)

        if key not in self._cache:
            self._cache[key] = rs.filesystem.Directory(tmp,
                    session=self._session)
        saga_dir = self._cache[key]


        # Loop over all transfer directives and execute them.
        for sd in actionables:

          # action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            self._prof.prof('staging_out_start', uid=uid, msg=did)

            self._log.debug('src: %s', src)
            self._log.debug('tgt: %s', tgt)

            src = rpsd.complete_url(src, src_context, self._log)
            tgt = rpsd.complete_url(tgt, tgt_context, self._log)

            self._log.debug('src: %s', src)
            self._log.debug('tgt: %s', tgt)

            # Check if the src is a folder, if true
            # add recursive flag if not already specified
            if saga_dir.is_dir(src.path):
                flags |= rs.filesystem.RECURSIVE

            # Always set CREATE_PARENTS
            flags |= rs.filesystem.CREATE_PARENTS

            saga_dir.copy(src, tgt, flags=flags)
            self._prof.prof('staging_out_stop', uid=uid, msg=did)

        # all staging is done -- at this point the unit is final
        unit['state'] = unit['target_state']
        self.advance(unit, publish=True, push=True)


# ------------------------------------------------------------------------------


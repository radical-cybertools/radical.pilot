
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

from ...staging_directives import complete_url


# ==============================================================================
#
class Default(UMGRStagingInputComponent):
    """
    This component performs all umgr side input staging directives for compute
    units.  It gets units from the umgr_staging_input_queue, in
    UMGR_STAGING_INPUT_PENDING state, will advance them to UMGR_STAGING_INPUT
    state while performing the staging, and then moves then to the 
    AGENT_SCHEDULING_PENDING state, passing control to the agent.
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
    def finalize_child(self):

        try:
            for key in self._cache:
                self._cache[key].close()
        except:
            pass
            

    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.UMGR_STAGING_INPUT, publish=False, push=False)

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

                if sd['action'] == rpc.TRANSFER:
                    actionables.append(sd)

            if actionables:
                staging_units.append([unit, actionables])
            else:
                no_staging_units.append(unit)


        if no_staging_units:

            # nothing to stage, push to the agent
            self.advance(no_staging_units, rps.AGENT_STAGING_INPUT_PENDING, 
                         publish=True, push=True)

        for unit,actionables in staging_units:
            self._handle_unit(unit, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit, actionables):

        # FIXME: we should created unit sandboxes in a bulk

        uid = unit['uid']

        self._prof.prof("create_sandbox_start", uid=uid)

        src_context = {'pwd'      : os.getcwd(),                # !!!
                       'unit'     : unit['unit_sandbox'], 
                       'pilot'    : unit['pilot_sandbox'], 
                       'resource' : unit['resource_sandbox']}
        tgt_context = {'pwd'      : unit['unit_sandbox'],       # !!!
                       'unit'     : unit['unit_sandbox'], 
                       'pilot'    : unit['pilot_sandbox'], 
                       'resource' : unit['resource_sandbox']}

        # we have actionable staging directives, and thus we need a unit
        # sandbox.
        sandbox = rs.Url(unit["unit_sandbox"])
        tmp     = rs.Url(unit["unit_sandbox"])

        # url used for cache (sandbox url w/o path)
        tmp.path = '/'
        key = str(tmp)
        self._log.debug('key %s / %s', key, tmp)

        if key not in self._cache:
            self._cache[key] = rs.filesystem.Directory(tmp, 
                                             session=self._session)

        saga_dir = self._cache[key]
        saga_dir.make_dir(sandbox, flags=rs.filesystem.CREATE_PARENTS)
        self._prof.prof("create_sandbox_stop", uid=uid)

        # Loop over all transfer directives and execute them.
        for sd in actionables:

            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            self._prof.prof('staging_in_start', uid=uid, msg=did)

            src = complete_url(src, src_context, self._log)
            tgt = complete_url(tgt, tgt_context, self._log)

            if rpc.CREATE_PARENTS in flags:
                copy_flags = rs.filesystem.CREATE_PARENTS
            else:
                copy_flags = 0

            saga_dir.copy(src, tgt, flags=copy_flags)
            self._prof.prof('staging_in_stop', uid=uid, msg=did)

        # staging is done, we can advance the unit at last
        self.advance(unit, rps.AGENT_STAGING_INPUT_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


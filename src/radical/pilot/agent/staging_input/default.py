
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

from .base import AgentStagingInputComponent

from ...staging_directives import complete_url


# ==============================================================================
#
class Default(AgentStagingInputComponent):
    """
    This component performs all agent side input staging directives for compute
    units.  It gets units from the agent_staging_input_queue, in
    AGENT_STAGING_INPUT_PENDING state, will advance them to AGENT_STAGING_INPUT
    state while performing the staging, and then moves then to the
    AGENT_SCHEDULING_PENDING state, into the agent_scheduling_queue.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentStagingInputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_STAGING_INPUT_PENDING,
                            rpc.AGENT_STAGING_INPUT_QUEUE, self.work)

        self.register_output(rps.AGENT_SCHEDULING_PENDING, 
                             rpc.AGENT_SCHEDULING_QUEUE)


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_STAGING_INPUT, publish=True, push=False)

        ru.raise_on('work bulk')

        # we first filter out any units which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.
        
        no_staging_units = list()
        staging_units    = list()

        for unit in units:

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in unit['description'].get('input_staging', []):

                if sd['action'] in [rpc.LINK, rpc.COPY, rpc.MOVE]:
                    actionables.append(sd)

            if actionables:
                staging_units.append([unit, actionables])
            else:
                no_staging_units.append(unit)


        if no_staging_units:
            self.advance(no_staging_units, rps.AGENT_SCHEDULING_PENDING,
                         publish=True, push=True)

        for unit,actionables in staging_units:
            self._handle_unit(unit, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit, actionables):

        ru.raise_on('work unit')

        uid = unit['uid']

        # NOTE: see documentation of cu['sandbox'] semantics in the ComputeUnit
        #       class definition.
        sandbox = unit['unit_sandbox']

        # By definition, this compoentn lives on the pilot's target resource.
        # As such, we *know* that all staging ops which would refer to the
        # resource now refer to file://localhost, and thus translate the unit,
        # pilot and resource sandboxes into that scope.  Some assumptions are
        # made though:
        #
        #   * paths are directly translatable across schemas
        #   * resource level storage is in fact accessible via file://
        #
        # FIXME: this is costly and should be cached.

        unit_sandbox     = ru.Url(unit['unit_sandbox'])
        pilot_sandbox    = ru.Url(unit['pilot_sandbox'])
        resource_sandbox = ru.Url(unit['resource_sandbox'])

        unit_sandbox.schema     = 'file'
        pilot_sandbox.schema    = 'file'
        resource_sandbox.schema = 'file'

        unit_sandbox.host       = 'localhost'
        pilot_sandbox.host      = 'localhost'
        resource_sandbox.host   = 'localhost'

        src_context = {'pwd'      : str(unit_sandbox),       # !!!
                       'unit'     : str(unit_sandbox), 
                       'pilot'    : str(pilot_sandbox), 
                       'resource' : str(resource_sandbox)}
        tgt_context = {'pwd'      : str(unit_sandbox),       # !!!
                       'unit'     : str(unit_sandbox), 
                       'pilot'    : str(pilot_sandbox), 
                       'resource' : str(resource_sandbox)}


        # we can now handle the actionable staging directives
        for sd in actionables:

            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            self._prof.prof('staging_begin', uid=uid, msg=did)

            assert(action in [rpc.COPY, rpc.LINK, rpc.MOVE, rpc.TRANSFER])

            # we only handle staging which does *not* include 'client://' src or
            # tgt URLs - those are handled by the umgr staging components
            if '://' in src and src.startswith('client://'):
                self._log.debug('skip staging for src %s', src)
                self._prof.prof('staging_end', uid=uid, msg=did)
                continue

            if '://' in tgt and tgt.startswith('client://'):
                self._log.debug('skip staging for tgt %s', tgt)
                self._prof.prof('staging_end', uid=uid, msg=did)
                continue

            src = complete_url(src, src_context, self._log)
            tgt = complete_url(tgt, tgt_context, self._log)

            self._log.debug(' === src: %s', src)
            self._log.debug(' === tgt: %s', tgt)

            assert(tgt.schema == 'file'), 'staging tgt must be file://'

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                assert(src.schema == 'file'), 'staging src expected as file://'


            # SAGA will take care of dir creation - but we do it manually
            # for local ops (copy, link, move)
            if rpc.CREATE_PARENTS in flags and action != rpc.TRANSFER:
                tgtdir = os.path.dirname(tgt.path)
                if tgtdir != sandbox:
                    # TODO: optimization point: create each dir only once
                    self._log.debug("mkdir %s" % tgtdir)
                    rpu.rec_makedir(tgtdir)

            if   action == rpc.COPY: shutil.copyfile(src.path, tgt.path)
            elif action == rpc.LINK: os.symlink     (src.path, tgt.path)
            elif action == rpc.MOVE: shutil.move    (src.path, tgt.path)
            elif action == rpc.TRANSFER:

                # FIXME: we only handle srm staging right now, and only for
                #        a specific target proxy. Other TRANSFER directives are
                #        left to umgr input staging.  We should use SAGA to
                #        attempt all staging ops which do not target the client
                #        machine.
                if src.schema == 'srm':
                    # FIXME: cache saga handles
                    srm_dir = rs.filesystem.Directory('srm://proxy/?SFN=bogus')
                    srm_dir.copy(src, tgt)
                    srm_dir.close()
                else:
                    self._log.error('no transfer for %s -> %s', src, tgt)
                    self._prof.prof('staging_end', uid=uid, msg=did)
                    raise NotImplementedError('unsupported transfer %s' % src)

            self._prof.prof('staging_end', uid=uid, msg=did)

        # all staging is done -- pass on to the scheduler
        self.advance(unit, rps.AGENT_SCHEDULING_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


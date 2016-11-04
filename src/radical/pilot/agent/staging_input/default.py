
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
            for entry in unit.get('input_staging', []):

                action = entry['action']
                flags  = entry['flags']
                src    = ru.Url(entry['source'])
                tgt    = ru.Url(entry['target'])

                if action in [rpc.LINK, rpc.COPY, rpc.MOVE]:
                    actionables.append([src, tgt, action, flags])

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
        sandbox = '%s/%s' % (self._pwd, uid)

        # we have actionables, thus we need sandbox and staging area
        # TODO: optimization: sandbox,staging_area might already exist
        staging_area = '%s/%s' % (self._pwd, self._cfg['staging_area'])

        self._prof.prof("create  sandbox", uid=uid, msg=sandbox)
        rpu.rec_makedir(sandbox)
        self._prof.prof("created sandbox", uid=uid)

        self._prof.prof("create  staging_area", uid=uid, msg=staging_area)
        # FIXME: this is only required once
        rpu.rec_makedir(staging_area)
        self._prof.prof("created staging_area", uid=uid)

        # Loop over all transfer directives and execute them.
        for src, tgt, action, flags in actionables:

            self._prof.prof('agent staging in', msg=src, uid=uid)

            # Handle special 'staging' schema
            if src.schema == self._cfg['staging_schema']:
                # remove leading '/' to convert into rel path
                source = os.path.join(staging_area, src.path[1:])
            else:
                source = src.path

            target = os.path.join(sandbox, tgt.path)

            if rpc.CREATE_PARENTS in flags:
                tgtdir = os.path.dirname(target)
                if tgtdir != sandbox:
                    # TODO: optimization point: create each dir only once
                    self._log.debug("mkdir %s" % tgtdir)
                    rpu.rec_makedir(tgtdir)

            self._log.info("%sing %s to %s", action, src, tgt)

            # for local files, check for existence first
            if action in [rpc.LINK, rpc.COPY, rpc.MOVE]:
                if not os.path.isfile(source):
                    # check if NON_FATAL flag is set, in that case ignore
                    # missing files
                    if rpc.NON_FATAL in flags:
                        self._log.warn("ignoring that source %s does not exist.", source)
                        continue
                    else:
                        log_message = "source %s does not exist." % source
                        self._log.error(log_message)
                        raise Exception(log_message)

            if   action == rpc.LINK: os.symlink     (source, target)
            elif action == rpc.COPY: shutil.copyfile(source, target)
            elif action == rpc.MOVE: shutil.move    (source, target)
            else:
                # FIXME: implement TRANSFER mode
                raise NotImplementedError('unsupported action %s' % action)

            self._log.info("%s'ed %s to %s" % (action, source, target))


        # all staging is done -- pass on to the scheduler
        self.advance(unit, rps.AGENT_SCHEDULING_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


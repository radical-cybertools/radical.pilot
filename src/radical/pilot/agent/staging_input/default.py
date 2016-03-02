
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import shutil

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

        self.declare_input(rps.AGENT_STAGING_INPUT_PENDING,
                           rpc.AGENT_STAGING_INPUT_QUEUE, self.work)

        self.declare_output(rps.AGENT_SCHEDULING_PENDING, 
                            rpc.AGENT_SCHEDULING_QUEUE)


    # --------------------------------------------------------------------------
    #
    def work(self, unit):

        self.advance(unit, rps.AGENT_STAGING_INPUT, publish=True, push=False)

        uid     = unit['uid']
        sandbox = ru.Url(unit["sandbox"]).path

        # prepare stdout/stderr
        stdout_file = unit['description'].get('stdout') or 'STDOUT'
        stderr_file = unit['description'].get('stderr') or 'STDERR'

        unit['stdout_file'] = os.path.join(sandbox, stdout_file)
        unit['stderr_file'] = os.path.join(sandbox, stderr_file)

        # check if we have any staging directives to be enacted in this
        # component
        actionables = list()
        for entry in unit.get('input_staging', []):

            action = entry['action']
            flags  = entry['flags']
            src    = ru.Url(entry['source'])
            tgt    = ru.Url(entry['target'])

            if action in [rpc.LINK, rps.COPY, rpc.MOVE]:
                actionables.append([src, tgt, flags])

        if actionables:

            # we have actionables, thus we need sandbox and staging area
            # TODO: optimization: sandbox,staging_area might already exist
            pilot_sandbox = ru.Url(self._cfg['pilot_sandbox']).path
            staging_area  = os.path.join(pilot_sandbox, cfg['staging_area'])

            self._prof.prof("create  sandbox", uid=uid, msg=sandbox)
            rpu.rec_makedir(sandbox)
            self._prof.prof("created sandbox", uid=uid)

            self._prof.prof("create  staging_area", uid=uid, msg=staging_area)
            rpu.rec_makedir(staging_area)
            self._prof.prof("created staging_area", uid=uid)

            # Loop over all transfer directives and execute them.
            for src, tgt, flags in actionables:

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


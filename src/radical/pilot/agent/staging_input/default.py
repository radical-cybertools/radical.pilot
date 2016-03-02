
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

        workdir      = os.path.join(self._cfg['workdir'], '%s' % unit['uid'])
        gtod         = os.path.join(self._cfg['workdir'], 'gtod')
        staging_area = os.path.join(self._cfg['workdir'], self._cfg['staging_area'])
        staging_ok   = True

        unit['workdir']     = workdir
        unit['stdout']      = ''
        unit['stderr']      = ''
        unit['opaque_clot'] = None
        # TODO: See if there is a more central place to put this
        unit['gtod']        = gtod

        stdout_file       = unit['description'].get('stdout')
        stdout_file       = stdout_file if stdout_file else 'STDOUT'
        stderr_file       = unit['description'].get('stderr')
        stderr_file       = stderr_file if stderr_file else 'STDERR'

        unit['stdout_file'] = os.path.join(workdir, stdout_file)
        unit['stderr_file'] = os.path.join(workdir, stderr_file)

        # create unit workdir
        rpu.rec_makedir(workdir)
        self._prof.prof('unit mkdir', uid=unit['uid'])

        for directive in unit['Agent_Input_Directives']:

            self._prof.prof('Agent input_staging queue', uid=unit['uid'],
                     msg="%s -> %s" % (str(directive['source']), str(directive['target'])))

            # Perform input staging
            self._log.info("unit input staging directives %s for unit: %s to %s",
                           directive, unit['uid'], workdir)

            # Convert the source_url into a SAGA Url object
            source_url = ru.Url(directive['source'])

            # Handle special 'staging' schema
            if source_url.schema == self._cfg['staging_schema']:
                self._log.info('Operating from staging')
                # Remove the leading slash to get a relative path from the staging area
                rel2staging = source_url.path.split('/',1)[1]
                source = os.path.join(staging_area, rel2staging)
            else:
                self._log.info('Operating from absolute path')
                source = source_url.path

            # Get the target from the directive and convert it to the location
            # in the workdir
            target = directive['target']
            abs_target = os.path.join(workdir, target)

            # Create output directory in case it doesn't exist yet
            rpu.rec_makedir(os.path.dirname(abs_target))

            self._log.info("Going to '%s' %s to %s", directive['action'], source, abs_target)

            if   directive['action'] == rpc.LINK: os.symlink     (source, abs_target)
            elif directive['action'] == rpc.COPY: shutil.copyfile(source, abs_target)
            elif directive['action'] == rpc.MOVE: shutil.move    (source, abs_target)
            else:
                # FIXME: implement TRANSFER mode
                raise NotImplementedError('Action %s not supported' % directive['action'])

            log_message = "%s'ed %s to %s - success" % (directive['action'], source, abs_target)
            self._log.info(log_message)

        # all staging is done -- pass on to the scheduler
        self.advance(unit, rps.AGENT_SCHEDULING_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


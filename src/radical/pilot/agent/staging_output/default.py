
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import shutil

import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentStagingOutputComponent


# ==============================================================================
#
class Default(AgentStagingOutputComponent):
    """
    This component performs all agent side output staging directives for compute
    units.  It gets units from the agent_staging_output_queue, in
    AGENT_STAGING_OUTPUT_PENDING state, will advance them to
    AGENT_STAGING_OUTPUT state while performing the staging, and then moves then
    to the UMGR_STAGING_OUTPUT_PENDING state, which at the moment requires the
    state change to be published to MongoDB (no push into a queue).

    Note that this component also collects stdout/stderr of the units (which
    can also be considered staging, really).
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.Component.__init__(self, 'AgentStagingOutputComponent', cfg)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg):

        return cls(cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self.declare_input (rps.AGENT_STAGING_OUTPUT_PENDING, rpc.AGENT_STAGING_OUTPUT_QUEUE)
        self.declare_worker(rps.AGENT_STAGING_OUTPUT_PENDING, self.work)

        # we don't need an output queue -- units are picked up via mongodb
        self.declare_output(rps.PENDING_OUTPUT_STAGING, None) # drop units

        self.declare_publisher('state', rpc.AGENT_STATE_PUBSUB)

        # all components use the command channel for control messages
        self.declare_publisher ('command', rpc.AGENT_COMMAND_PUBSUB)

        # communicate successful startup
        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        # communicate finalization
        self.publish('command', {'cmd' : 'final',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    def work(self, cu):

        self.advance(cu, rps.AGENT_STAGING_OUTPUT, publish=True, push=False)

        staging_area = os.path.join(self._cfg['workdir'], self._cfg['staging_area'])
        staging_ok   = True

        workdir = cu['workdir']

        ## parked from unit state checker: unit postprocessing
        if os.path.isfile(cu['stdout_file']):
            with open(cu['stdout_file'], 'r') as stdout_f:
                try:
                    txt = unicode(stdout_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stdout contains binary data -- use file staging directives"

                cu['stdout'] += rpu.tail(txt)

        if os.path.isfile(cu['stderr_file']):
            with open(cu['stderr_file'], 'r') as stderr_f:
                try:
                    txt = unicode(stderr_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stderr contains binary data -- use file staging directives"

                cu['stderr'] += rpu.tail(txt)

        if 'RADICAL_PILOT_PROFILE' in os.environ:
            if os.path.isfile("%s/PROF" % cu['workdir']):
                try:
                    with open("%s/PROF" % cu['workdir'], 'r') as prof_f:
                        txt = prof_f.read()
                        for line in txt.split("\n"):
                            if line:
                                x1, x2, x3 = line.split()
                                self._prof.prof(x1, msg=x2, timestamp=float(x3), uid=cu['_id'])
                except Exception as e:
                    self._log.error("Pre/Post profiling file read failed: `%s`" % e)

        # NOTE: all units get here after execution, even those which did not
        #       finish successfully.  We do that so that we can make
        #       stdout/stderr available for failed units.  But at this point we
        #       don't need to advance those units anymore, but can make them
        #       final.
        if cu['target_state'] != rps.DONE:
            self.advance(cu, cu['target_state'], publish=True, push=False)
            return


        try:
            # all other units get their (expectedly valid) output files staged
            for directive in cu['Agent_Output_Directives']:

                self._prof.prof('Agent output_staging', uid=cu['_id'],
                         msg="%s -> %s" % (str(directive['source']), str(directive['target'])))

                # Perform output staging
                self._log.info("unit output staging directives %s for cu: %s to %s",
                        directive, cu['_id'], workdir)

                # Convert the target_url into a RU Url object
                target_url = ru.Url(directive['target'])

                # Handle special 'staging' scheme
                if target_url.scheme == self._cfg['staging_scheme']:
                    self._log.info('Operating from staging')
                    # Remove the leading slash to get a relative path from
                    # the staging area
                    rel2staging = target_url.path.split('/',1)[1]
                    target = os.path.join(staging_area, rel2staging)
                else:
                    self._log.info('Operating from absolute path')
                    # FIXME: will this work for TRANSFER mode?
                    target = target_url.path

                # Get the source from the directive and convert it to the location
                # in the workdir
                source = str(directive['source'])
                abs_source = os.path.join(workdir, source)

                # Create output directory in case it doesn't exist yet
                # FIXME: will this work for TRANSFER mode?
                rpu.rec_makedir(os.path.dirname(target))

                self._log.info("Going to '%s' %s to %s", directive['action'], abs_source, target)

                if directive['action'] == rpc.LINK:
                    # This is probably not a brilliant idea, so at least give a warning
                    os.symlink(abs_source, target)
                elif directive['action'] == rpc.COPY:
                    shutil.copyfile(abs_source, target)
                elif directive['action'] == rpc.MOVE:
                    shutil.move(abs_source, target)
                else:
                    # FIXME: implement TRANSFER mode
                    raise NotImplementedError('Action %s not supported' % directive['action'])

                log_message = "%s'ed %s to %s - success" %(directive['action'], abs_source, target)
                self._log.info(log_message)

        except Exception as e:
            self._log.exception("staging output failed -> unit failed")
            staging_ok = False


        # Agent output staging is done (or failed)
        if staging_ok:
          # self.advance(cu, rps.UMGR_STAGING_OUTPUT_PENDING, publish=True, push=True)
            self.advance(cu, rps.PENDING_OUTPUT_STAGING, publish=True, push=False)
        else:
            self.advance(cu, rps.FAILED, publish=True, push=False)



# ------------------------------------------------------------------------------


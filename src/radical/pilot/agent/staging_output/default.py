
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
    def __init__(self, cfg, session):

        AgentStagingOutputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_STAGING_OUTPUT_PENDING, 
                            rpc.AGENT_STAGING_OUTPUT_QUEUE, self.work)

        # we don't need an output queue -- units are picked up via mongodb
        self.register_output(rps.UMGR_STAGING_OUTPUT_PENDING, None) # drop units


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_STAGING_OUTPUT, publish=True, push=False)

        for unit in units:

            self._handle_unit(unit)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit):

        uid = unit['uid']

        # NOTE: see documentation of cu['sandbox'] semantics in the ComputeUnit
        #       class definition.
        sandbox = '%s/%s' % (self._pwd, uid)

        ## parked from unit state checker: unit postprocessing
        if os.path.isfile(unit['stdout_file']):
            with open(unit['stdout_file'], 'r') as stdout_f:
                try:
                    txt = unicode(stdout_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stdout is binary -- use file staging"

                unit['stdout'] += rpu.tail(txt)

        if os.path.isfile(unit['stderr_file']):
            with open(unit['stderr_file'], 'r') as stderr_f:
                try:
                    txt = unicode(stderr_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stderr is binary -- use file staging"

                unit['stderr'] += rpu.tail(txt)

        if 'RADICAL_PILOT_PROFILE' in os.environ:
            if os.path.isfile("%s/PROF" % sandbox):
                try:
                    with open("%s/PROF" % sandbox, 'r') as prof_f:
                        txt = prof_f.read()
                        for line in txt.split("\n"):
                            if line:
                                x1, x2, x3 = line.split()
                                self._prof.prof(x1, msg=x2, timestamp=float(x3),
                                        uid=unit['uid'])
                except Exception as e:
                    self._log.error("Pre/Post profiling file read failed: `%s`" % e)


        # From here on, any state update will hand control over to the umgr
        # again.  The next unit update should thus push *all* unit details, not
        # only state.
        unit['$all']    = True
        unit['control'] = 'umgr_pending'

        # NOTE: all units get here after execution, even those which did not
        #       finish successfully.  We do that so that we can make
        #       stdout/stderr available for failed units.  But at this point we
        #       don't need to advance those units anymore, but can make them
        #       final.
        if unit['target_state'] != rps.DONE:
            self.advance(unit, state=unit['target_state'], publish=True, push=False)
            return

        # check if we have any staging directives to be enacted in this
        # component
        actionables = list()
        for sd in unit['description'].get('output_staging', []):

            src    = ru.Url(sd['source'])
            tgt    = ru.Url(sd['target'])
            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']

            actionables.append([src, tgt, action, flags,  did])


        if actionables:

            # we have actionables, thus we need staging area
            # TODO: optimization: staging_area might already exist
            staging_area = '%s/%s' % (self._pwd, self._cfg['staging_area'])

            self._prof.prof("create  staging_area", uid=uid, msg=staging_area)
            rpu.rec_makedir(staging_area)
            self._prof.prof("created staging_area", uid=uid)

            # Loop over all transfer directives and execute them.
            for src, tgt, action, flags, did in actionables:

                self._prof.prof('begin', uid=uid, msg=did)

                # Handle special 'staging' schema
                if tgt.schema == self._cfg['staging_schema']:
                    self._log.info('Operating to staging')
                    rel2staging = tgt.path.split('/',1)[1]
                    target = os.path.join(staging_area, rel2staging)
                elif action != rpc.TRANSFER:
                    self._log.info('Operating to absolute path')
                    target = tgt.path
                    assert(target.startswith('/'))
                elif action == rpc.TRANSFER:
                    self._log.info('Operating to remote location')
                    target = tgt

                # make sure the src path is either absolute or relative to the
                # sandbox
                if not src.path.startswith('/'):
                    src.path = '%s/%s' % (sandbox, src.path)

                if rpc.CREATE_PARENTS in flags and action != rpc.TRANSFER:
                    tgtdir = os.path.dirname(target)
                    if tgtdir != sandbox:
                        # TODO: optimization point: create each dir only once
                        self._log.debug("mkdir %s" % tgtdir)
                        rpu.rec_makedir(tgtdir)

                if   action == rpc.LINK: os.symlink     (source, target)
                elif action == rpc.COPY: shutil.copyfile(source, target)
                elif action == rpc.MOVE: shutil.move    (source, target)
                elif action == rpc.TRANSFER:

                    # we only handle srm staging right now -- other TRANSFER
                    # directives are left to umgr output staging
                    if tgt.schema == 'srm':
                        src_url = rs.Url(source)
                        src_url.schema = 'file'
                        srm_dir = rs.filesystem.Directory('srm://proxy/?SFN=bogus')
                        srm_dir.copy(src_url, target)
                else:
                    raise NotImplementedError('unsupported action %s' % action)

                self._prof.prof('end', uid=uid, msg=did)

        # TODO: don't raise for non-fatal staging

        # all agent staging is done -- pass on to umgr output staging
        self.advance(unit, rps.UMGR_STAGING_OUTPUT_PENDING, publish=True, push=False)


# ------------------------------------------------------------------------------
	

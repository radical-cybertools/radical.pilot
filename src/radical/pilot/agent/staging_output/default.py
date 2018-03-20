
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import errno
import shutil

import saga          as rs
import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentStagingOutputComponent

from ...staging_directives import complete_url


# ------------------------------------------------------------------------------
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

        ru.raise_on('work bulk')

        # we first filter out any units which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.
        
        no_staging_units = list()
        staging_units    = list()

        for unit in units:

            uid = unit['uid']

            # From here on, any state update will hand control over to the umgr
            # again.  The next unit update should thus push *all* unit details,
            # not only state.
            unit['$all']    = True 
            unit['control'] = 'umgr_pending'

            # we always dig for stdout/stderr
            self._handle_unit_stdio(unit)

            # NOTE: all units get here after execution, even those which did not
            #       finish successfully.  We do that so that we can make
            #       stdout/stderr available for failed units (see
            #       _handle_unit_stdio above).  But we don't need to perform any
            #       other staging for those units, and in fact can make them
            #       final.
            if unit['target_state'] != rps.DONE:
                unit['state'] = unit['target_state']
                self._log.debug('unit %s skips staging (%s)', uid, unit['state'])
                no_staging_units.append(unit)
                continue

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in unit['description'].get('output_staging', []):
                if sd['action'] in [rpc.LINK, rpc.COPY, rpc.MOVE]:
                    actionables.append(sd)

            if actionables:
                # this unit needs some staging
                staging_units.append([unit, actionables])
            else:
                # this unit does not need any staging at this point, and can be
                # advanced
                unit['state'] = rps.UMGR_STAGING_OUTPUT_PENDING
                no_staging_units.append(unit)

        if no_staging_units:
            self.advance(no_staging_units, publish=True, push=True)

        for unit,actionables in staging_units:
            self._handle_unit_staging(unit, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_unit_stdio(self, unit):

        sandbox = ru.Url(unit['unit_sandbox']).path
        uid     = unit['uid']

        self._prof.prof('staging_stdout_start', uid=uid)

        # TODO: disable this at scale?
        if os.path.isfile(unit['stdout_file']):
            with open(unit['stdout_file'], 'r') as stdout_f:
                try:
                    txt = unicode(stdout_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stdout is binary -- use file staging"

                unit['stdout'] += rpu.tail(txt)

        self._prof.prof('staging_stdout_stop',  uid=uid)
        self._prof.prof('staging_stderr_start', uid=uid)

        # TODO: disable this at scale?
        if os.path.isfile(unit['stderr_file']):
            with open(unit['stderr_file'], 'r') as stderr_f:
                try:
                    txt = unicode(stderr_f.read(), "utf-8")
                except UnicodeDecodeError:
                    txt = "unit stderr is binary -- use file staging"

                unit['stderr'] += rpu.tail(txt)

        self._prof.prof('staging_stderr_stop', uid=uid)
        self._prof.prof('staging_uprof_start', uid=uid)

        unit_prof = "%s/%s.prof" % (sandbox, uid)

        if os.path.isfile(unit_prof):
            try:
                with open(unit_prof, 'r') as prof_f:
                    txt = prof_f.read()
                    for line in txt.split("\n"):
                        if line:
                            ts, event, comp, tid, _uid, state, msg = line.split(',')
                            self._prof.prof(timestamp=float(ts), event=event,
                                            comp=comp, tid=tid, uid=_uid,
                                            state=state, msg=msg)
            except Exception as e:
                self._log.error("Pre/Post profile read failed: `%s`" % e)

        self._prof.prof('staging_uprof_stop', uid=uid)


    # --------------------------------------------------------------------------
    #
    def _handle_unit_staging(self, unit, actionables):

        ru.raise_on('work unit')

        uid = unit['uid']

        # NOTE: see documentation of cu['sandbox'] semantics in the ComputeUnit
        #       class definition.
        sandbox = ru.Url(unit['unit_sandbox']).path

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

            self._prof.prof('staging_out_start', uid=uid, msg=did)

            assert(action in [rpc.COPY, rpc.LINK, rpc.MOVE, rpc.TRANSFER]), \
                              'invalid staging action'

            # we only handle staging which does *not* include 'client://' src or
            # tgt URLs - those are handled by the umgr staging components
            if src.startswith('client://'):
                self._log.debug('skip staging for src %s', src)
                self._prof.prof('staging_out_skip', uid=uid, msg=did)
                continue

            if tgt.startswith('client://'):
                self._log.debug('skip staging for tgt %s', tgt)
                self._prof.prof('staging_out_skip', uid=uid, msg=did)
                continue

            # Fix for when the target PATH is empty
            # we assume current directory is the unit staging 'unit://'
            # and we assume the file to be copied is the base filename of the source
            if tgt is None: tgt = ''
            if tgt.strip() == '':
                tgt = 'unit:///{}'.format(os.path.basename(src))
            # Fix for when the target PATH is exists *and* it is a folder
            # we assume the 'current directory' is the target folder
            # and we assume the file to be copied is the base filename of the source
            elif os.path.exists(tgt.strip()) and os.path.isdir(tgt.strip()):
                tgt = os.path.join(tgt, os.path.basename(src))
                

            src = complete_url(src, src_context, self._log)
            tgt = complete_url(tgt, tgt_context, self._log)

            # Currently, we use the same schema for files and folders.
            assert(src.schema == 'file'), 'staging src must be file://'

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                assert(tgt.schema == 'file'), 'staging tgt expected as file://'

            # SAGA will take care of dir creation - but we do it manually
            # for local ops (copy, link, move)
            if flags & rpc.CREATE_PARENTS and action != rpc.TRANSFER:
                tgtdir = os.path.dirname(tgt.path)
                if tgtdir != sandbox:
                    self._log.debug("mkdir %s", tgtdir)
                    rpu.rec_makedir(tgtdir)

            if   action == rpc.COPY: 
                try:
                    shutil.copytree(src.path, tgt.path)
                except OSError as exc: 
                    if exc.errno == errno.ENOTDIR:
                        shutil.copy(src.path, tgt.path)
                    else: 
                        raise
                
            elif action == rpc.LINK:
                # Fix issue/1513 if link source is file and target is folder
                # should support POSIX standard where link is created
                # with the same name as the source
                if os.path.isfile(src.path) and os.path.isdir(tgt.path):
                    os.symlink     (src.path, os.path.join(tgt.path, os.path.basename(src.path)))
                else: # default behavior
                    os.symlink     (src.path, tgt.path)
            elif action == rpc.MOVE: shutil.move    (src.path, tgt.path)
            elif action == rpc.TRANSFER: pass
                # This is currently never executed. Commenting it out.
                # Uncomment and implement when uploads directly to remote URLs
                # from units are supported.
                # FIXME: we only handle srm staging right now, and only for
                #        a specific target proxy. Other TRANSFER directives are
                #        left to umgr output staging.  We should use SAGA to
                #        attempt all staging ops which do not target the client
                #        machine.
                # if tgt.schema == 'srm':
                #     # FIXME: cache saga handles
                #     srm_dir = rs.filesystem.Directory('srm://proxy/?SFN=bogus')
                #     srm_dir.copy(src, tgt)
                #     srm_dir.close()
                # else:
                #     self._log.error('no transfer for %s -> %s', src, tgt)
                #     self._prof.prof('staging_out_fail', uid=uid, msg=did)
                #     raise NotImplementedError('unsupported transfer %s' % tgt)

            self._prof.prof('staging_out_stop', uid=uid, msg=did)

        # all agent staging is done -- pass on to umgr output staging
        self.advance(unit, rps.UMGR_STAGING_OUTPUT_PENDING, publish=True, push=False)


# ------------------------------------------------------------------------------


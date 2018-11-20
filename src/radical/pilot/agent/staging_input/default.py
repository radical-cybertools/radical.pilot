
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import errno
import shutil
import tarfile

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

                if sd['action'] in [rpc.LINK, rpc.COPY, rpc.MOVE, rpc.TARBALL]:
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

            self._prof.prof('staging_in_start', uid=uid, msg=did)

            assert(action in [rpc.COPY, rpc.LINK, rpc.MOVE, rpc.TRANSFER, rpc.TARBALL])

            # we only handle staging which does *not* include 'client://' src or
            # tgt URLs - those are handled by the umgr staging components
            if src.startswith('client://') and action != rpc.TARBALL:
                self._log.debug('skip staging for src %s', src)
                self._prof.prof('staging_in_skip', uid=uid, msg=did)
                continue

            if tgt.startswith('client://'):
                self._log.debug('skip staging for tgt %s', tgt)
                self._prof.prof('staging_in_skip', uid=uid, msg=did)
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
            assert(tgt.schema == 'file'), 'staging tgt must be file://'

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                assert(src.schema == 'file'), 'staging src expected as file://'

            # SAGA will take care of dir creation - but we do it manually
            # for local ops (copy, link, move)
            if flags & rpc.CREATE_PARENTS and action != rpc.TRANSFER:
                tgtdir = os.path.dirname(tgt.path)
                if tgtdir != sandbox:
                    self._log.debug("mkdir %s", tgtdir)
                    rpu.rec_makedir(tgtdir)

            if action == rpc.COPY: 
                try:
                    shutil.copytree(src.path, tgt.path)
                except OSError as exc: 
                    if exc.errno == errno.ENOTDIR:
                        shutil.copy(src.path, tgt.path)
                    else: 
                        raise
                        
            elif action == rpc.LINK:

                # Fix issue/1513 if link source is file and target is folder.
                # should support POSIX standard where link is created
                # with the same name as the source
                if os.path.isfile(src.path) and os.path.isdir(tgt.path):
                    os.symlink(src.path, 
                               '%s/%s' % (tgt.path, os.path.basename(src.path)))

                else: # default behavior
                    os.symlink(src.path, tgt.path)

            elif action == rpc.MOVE:
                shutil.move(src.path, tgt.path)

            elif action == rpc.TRANSFER:

                # NOTE:  TRANSFER directives don't arrive here right now.
                # FIXME: we only handle srm staging right now, and only for
                #        a specific target proxy. Other TRANSFER directives are
                #        left to umgr input staging.  We should use SAGA to
                #        attempt all staging ops which do not involve the client
                #        machine.
                if src.schema == 'srm':
                    # FIXME: cache saga handles
                    srm_dir = rs.filesystem.Directory('srm://proxy/?SFN=bogus')
                    srm_dir.copy(src, tgt)
                    srm_dir.close()

                else:
                    self._log.error('no transfer for %s -> %s', src, tgt)
                    self._prof.prof('staging_in_fail', uid=uid, msg=did)
                    raise NotImplementedError('unsupported transfer %s' % src)

            elif action == rpc.TARBALL:

                # If somethig was staged via the tarball method, the tarball is
                # extracted and then removed from the unit folder.  The target
                # path is expected to be an *absolute* path on the target system
                # - any relative paths specified by the application are expected
                # to get expanded on the client side.
                tarball = '%s/%s.tar' % (os.path.dirname(tgt.path), uid)
                self._log.debug('extract tarball for %s', tarball)
                tar = tarfile.open(tarball)
                tar.extractall(path='/')
                tar.close()

              # FIXME: make tarball removal dependent on debug settings
              # os.remove(os.path.dirname(tgt.path) + '/' + uid + '.tar')

            self._prof.prof('staging_in_stop', uid=uid, msg=did)

        # all staging is done -- pass on to the scheduler
        self.advance(unit, rps.AGENT_SCHEDULING_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------


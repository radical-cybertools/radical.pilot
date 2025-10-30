
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from collections import defaultdict

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIExec(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._mpt    : bool  = False
        self._rsh    : bool  = False
        self._use_rf : bool  = False
        self._use_hf : bool  = False
        self._can_os : bool  = False
        self._ccmrun : str   = ''
        self._dplace : str   = ''
        self._omplace: str   = ''
        self._command: str   = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        lm_info = {
            'env'    : env,
            'env_sh' : env_sh,
            'command': ru.which([
                'mpiexec',             # General case
                'mpiexec.mpich',       # Linux, MPICH
                'mpiexec.hydra',       # Linux, MPICH
                'mpiexec.openmpi',     # Linux, MPICH
                'mpiexec-mpich-mp',    # Mac OSX MacPorts
                'mpiexec-openmpi-mp',  # Mac OSX MacPorts
                'mpiexec_mpt',         # Cheyenne (NCAR)
            ]),
            'mpt'    : False,
            'rsh'    : False,
            'use_rf' : False,
            'use_hf' : False,
            'can_os' : False,
            'ccmrun' : '',
            'dplace' : '',
            'omplace': ''
        }

        if not lm_info['command']:
            raise ValueError('mpiexec not found - cannot start MPI tasks')

        if '_mpt' in self.name.lower():
            lm_info['mpt'] = True

        if '_rsh' in self.name.lower():
            lm_info['rsh'] = True

        # do we need ccmrun or dplace?
        if '_ccmrun' in self.name.lower():
            lm_info['ccmrun'] = ru.which('ccmrun')
            assert lm_info['ccmrun']

        if '_dplace' in self.name.lower():
            lm_info['dplace'] = ru.which('dplace')
            assert lm_info['dplace']

        # cheyenne always needs mpt and omplace
        if 'cheyenne' in ru.get_hostname():
            lm_info['omplace'] = 'omplace'
            lm_info['mpt']     = True

        # check that this implementation allows to use `rankfile` option
        lm_info['use_rf'] = self._check_available_lm_options(
            lm_info['command'], '-rf')

        # if we fail, then check if this implementation allows to use
        # `host names` option
        if not lm_info['use_rf']:
            lm_info['use_hf'] = self._check_available_lm_options(
                lm_info['command'], '-f')

        # check that this implementation allows to use `oversubscribe` option
        if self._rm_info.details.get('oversubscribe'):
            lm_info['can_os'] = self._check_available_lm_options(
                lm_info['command'], '--oversubscribe')
            if not lm_info['can_os']:
                self._log.warn('oversubscribe requested but not supported')

        mpi_version, mpi_flavor = self._get_mpi_info(lm_info['command'])
        lm_info['mpi_version']  = mpi_version
        lm_info['mpi_flavor']   = mpi_flavor

        return lm_info

    # --------------------------------------------------------------------------
    #
    def _check_available_lm_options(self, lm_cmd, option):
        cmd   = '%s --help | grep -e "%s\\>"' % (lm_cmd, option)
        check = bool(ru.sh_callout(cmd, shell=True)[0])
        self._log.debug('lm option %s: %s (%s)' % (option, check, cmd))

        if not check:
            # openmpi needs `all` to work here
            cmd   = '%s --help all | grep -e "%s\\>"' % (lm_cmd, option)
            check = bool(ru.sh_callout(cmd, shell=True)[0])
            self._log.debug('lm option %s: %s (%s)' % (option, check, cmd))

        if not check:
            # openmpi is different nowadays...
            check = bool(ru.sh_callout('%s --help mapping | grep -e "%s\\>"' %
                                      (lm_cmd, option), shell=True)[0])

        return check

    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        self._env         = lm_info['env']
        self._env_sh      = lm_info['env_sh']
        self._command     = lm_info['command']

        assert self._command

        self._mpt         = lm_info['mpt']
        self._rsh         = lm_info['rsh']
        self._use_rf      = lm_info['use_rf']
        self._use_hf      = lm_info['use_hf']
        self._can_os      = lm_info['can_os']
        self._dplace      = lm_info['dplace']
        self._ccmrun      = lm_info['ccmrun']

        self._mpi_version = lm_info['mpi_version']
        self._mpi_flavor  = lm_info['mpi_flavor']

        # ensure empty string on unset omplace
        if not lm_info['omplace']:
            self._omplace = ''
        else:
            self._omplace = 'omplace'


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if not task['description']['executable']:
            return False, 'no executable'

        return True, ''


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        lm_env_cmds = ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]

        # Cheyenne is the only machine that requires mpiexec_mpt.
        # We then have to set MPI_SHEPHERD=true
        if self._mpt:
            lm_env_cmds.append('export MPI_SHEPHERD=true')

        return lm_env_cmds


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _get_rank_file(slots, uid, sandbox):
        '''
        Rank file:
            rank 0=localhost slots=0,1,2,3
            rank 1=localhost slots=4,5,6,7
        '''
        rf_str  = ''
        rank_id = 0

        for slot in slots:
            rf_str += 'rank %d=%s ' % (rank_id, slot['node_name'])
            rf_str += 'slots=%s\n' % ','.join([str(c['index'])
                                               for c in slot['cores']])
            rank_id += 1

        rf_name = '%s/%s.rf' % (sandbox, uid)
        with ru.ru_open(rf_name, 'w') as fout:
            fout.write(rf_str)

        return rf_name


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _get_host_file(slots, uid, sandbox, mode=0):
        '''
        Create a hostfile for the given slots
        mode == 0:
            localhost
        mode == 1:
            localhost slots=2
        mode == 2:
            localhost:2
        '''

        host_slots = defaultdict(int)

        for slot in slots:
            host_slots[slot['node_name']] += 1

        if mode == 0:
            hf_str = '%s\n' % '\n'.join(list(host_slots.keys()))

        else:
            hf_str = ''
            if mode == 1: slots_ref = ' slots='
            else        : slots_ref = ':'

            for host_name, num_slots in host_slots.items():
                hf_str += '%s%s%d\n' % (host_name, slots_ref, num_slots)

        hf_name = '%s/%s.hf' % (sandbox, uid)
        with ru.ru_open(hf_name, 'w') as fout:
            fout.write(hf_str)

        return hf_name


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        uid   = task['uid']
        slots = task['slots']
        sbox  = task['task_sandbox_path']

        assert slots, 'task.slots not defined'

        host_slots = defaultdict(int)
        for slot in slots:
            host_slots[slot['node_name']] += 1

        cmd_options = '-np %d ' % sum(host_slots.values())

        if self._use_rf:
            rankfile     = self._get_rank_file(slots, uid, sbox)
            hosts        = set([slot['node_name'] for slot in slots])
          # cmd_options += '-H %s -rf %s ' % (','.join(hosts), rankfile)
            cmd_options += '-rf %s ' % rankfile

        elif self._mpi_flavor == self.MPI_FLAVOR_PALS:
            hostfile     = self._get_host_file(slots, uid, sbox)

            tmp = list()
            for slot in slots:
                cores = slot['cores']
                if len(cores) > 1:
                    tmp.append('%s-%s' % (cores[0]['index'], cores[-1]['index']))
                else:
                    tmp.append(str(cores[0]['index']))
            core_ids = ':'.join(tmp)

          # # FIXME: make this readable please
          # core_ids     = ':'.join([
          #     str(cores[0]) + ('-%s' % cores[-1] if len(cores) > 1 else '')
          #     for core_map in [slot['cores'] for slot in slots]
          #     for cores    in core_map])
            cmd_options += '--ppn %d '           % max(host_slots.values()) + \
                           '--cpu-bind list:%s ' % core_ids + \
                           '--hostfile %s '      % hostfile

            # NOTE: Option "--ppn" controls "node-depth" vs. "core-depth"
            #       process placement. If we submit "mpiexec" command with
            #       "--ppn" option, it will place processes within the same
            #       node first. If we do not provide "--ppn" option, it will
            #       place processes on the available nodes one by one and
            #       round-robin when each available node is populated.

            # if over-subscription is allowed,
            # then the following approach is applicable too:
            #    cores_per_rank = len(slots[0]['cores'])
            #    cmd_options   += '--depth=%d --cpu-bind depth ' % cores_per_rank

        elif self._use_hf:
            hostfile = self._get_host_file(slots, uid, sbox, mode=2)
            cmd_options += '-f %s ' % hostfile
        else:
            hostfile     = self._get_host_file(slots, uid, sbox, mode=1)
            cmd_options += '--hostfile %s ' % hostfile


        # some mpi versions don't like oversubscribe and rankfile together
        # (anvil OpenMPI 4.0.6)
        if not self._use_rf and not self._use_hf:
            if self._rm_info.details.get('oversubscribe') and self._can_os:
                cmd_options += '--oversubscribe '

        if self._omplace:
            cmd_options += '%s ' % self._omplace

        cmd = '%s %s %s' % (self._command, cmd_options, exec_path)
        return cmd.strip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: we know the MPI flavor, so make this less guesswork

        ret  = 'test -z "$MPI_RANK"     || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK"    || export RP_RANK=$PMIX_RANK\n'

        if self._mpt:
            ret += 'test -z "$MPT_MPI_RANK" || export RP_RANK=$MPT_MPI_RANK\n'

        if self._mpi_flavor == self.MPI_FLAVOR_HYDRA:
            ret += 'test -z "$PMI_ID"       || export RP_RANK=$PMI_ID\n'
            ret += 'test -z "$PMI_RANK"     || export RP_RANK=$PMI_RANK\n'
        elif self._mpi_flavor == self.MPI_FLAVOR_PALS:
            ret += 'test -z "$PALS_RANKID"  || export RP_RANK=$PALS_RANKID\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def get_env_blacklist(self):
        '''
        extend the base blacklist by SLURM specific environment variables
        '''

        ret = super().get_env_blacklist()

        ret.extend([
                    'OMPI_*',
                    'PMIX_*',
                   ])

        return ret


# ------------------------------------------------------------------------------


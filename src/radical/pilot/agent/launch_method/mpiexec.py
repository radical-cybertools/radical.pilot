
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
        self._ccmrun : str   = ''
        self._dplace : str   = ''
        self._omplace: str   = ''
        self._command: str   = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

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

        mpi_version, mpi_flavor = self._get_mpi_info(lm_info['command'])
        lm_info['mpi_version']  = mpi_version
        lm_info['mpi_flavor']   = mpi_flavor

        return lm_info

    # --------------------------------------------------------------------------
    #
    def _check_available_lm_options(self, lm_cmd, option):
        check = bool(ru.sh_callout('%s --help | grep -e "%s\\>"' %
                                  (lm_cmd, option), shell=True)[0])

        return check

    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env         = lm_info['env']
        self._env_sh      = lm_info['env_sh']
        self._command     = lm_info['command']

        assert self._command

        self._mpt         = lm_info['mpt']
        self._rsh         = lm_info['rsh']
        self._use_rf      = lm_info['use_rf']
        self._use_hf      = lm_info['use_hf']
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

        for rank in slots['ranks']:
            for core_map in rank['core_map']:
                rf_str += ('rank %d=%s ' % (rank_id, rank['node_name']) +
                           'slots=%s\n' % ','.join([str(c) for c in core_map]))
                rank_id += 1

        rf_name = '%s/%s.rf' % (sandbox, uid)
        with ru.ru_open(rf_name, 'w') as fout:
            fout.write(rf_str)

        return rf_name


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _get_host_file(slots, uid, sandbox, simple=True, mode=0):
        '''
        Host file (simple=True):
            localhost
        Host file (simple=False, mode=0):
            localhost slots=2
        Host file (simple=False, mode=1):
            localhost:2
        '''
        host_slots = defaultdict(int)
        for rank in slots['ranks']:
            host_slots[rank['node_name']] += len(rank['core_map'])

        if simple:
            hf_str = '%s\n' % '\n'.join(list(host_slots.keys()))
        else:
            hf_str    = ''
            slots_ref = ':' if mode else ' slots='
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

        assert slots.get('ranks'), 'task.slots.ranks not defined'

        host_slots = defaultdict(int)
        for rank in slots['ranks']:
            host_slots[rank['node_name']] += len(rank['core_map'])

        cmd_options = '-np %d ' % sum(host_slots.values())

        if self._use_rf:
            rankfile     = self._get_rank_file(slots, uid, sbox)
            hosts        = set([r['node_name'] for r in slots['ranks']])
            cmd_options += '-H %s -rf %s' % (','.join(hosts), rankfile)

        elif self._mpi_flavor == self.MPI_FLAVOR_PALS:
            hostfile     = self._get_host_file(slots, uid, sbox)
            core_ids     = ':'.join([
                str(cores[0]) + ('-%s' % cores[-1] if len(cores) > 1 else '')
                for core_map in [rank['core_map'] for rank in slots['ranks']]
                for cores in core_map])
            cmd_options += '--ppn %d '           % max(host_slots.values()) + \
                           '--cpu-bind list:%s ' % core_ids + \
                           '--hostfile %s'       % hostfile

            # NOTE: Option "--ppn" controls "node-depth" vs. "core-depth"
            #       process placement. If we submit "mpiexec" command with
            #       "--ppn" option, it will place processes within the same
            #       node first. If we do not provide "--ppn" option, it will
            #       place processes on the available nodes one by one and
            #       round-robin when each available node is populated.

            # if over-subscription is allowed,
            # then the following approach is applicable too:
            #    cores_per_rank = len(slots['ranks'][0]['core_map'][0])
            #    cmd_options   += '--depth=%d --cpu-bind depth' % cores_per_rank

        elif self._use_hf:
            hostfile = self._get_host_file(slots, uid, sbox, simple=False,
                                           mode=1)
            cmd_options += '-f %s' % hostfile
        else:
            hostfile     = self._get_host_file(slots, uid, sbox, simple=False)
            cmd_options += '--hostfile %s' % hostfile

        if self._omplace:
            cmd_options += ' %s' % self._omplace

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


# ------------------------------------------------------------------------------


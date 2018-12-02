#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru

t_num     = 1024 * 16
t_gen     = 4
t_procs   = 2
t_threads = 1
t_cores   = t_procs * t_threads
t_time    = 1
a_cores   = 0  # 16 * 4
p_cores   = t_num * t_cores / t_gen  + a_cores
p_queue   = 'debug'

# p_queue   = 'batch'
# p_cores   = 262144 + 46


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    session = rp.Session()
    try:

        report.header('submit pilots')
        pd_init = {
                #  'resource'      : 'exp.titan_orte',
                   'resource'      : 'exp.local',
                   'runtime'       : 60,
                   'exit_on_error' : True,
                 # 'project'       : 'BIP149',
                 # 'queue'         : p_queue,
                   'access_schema' : 'local',
                   'cores'         : p_cores
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

      # report.header('stage data')
      # pilot.stage_in({'source': 'client:///examples/misc/gromacs/',
      #                 'target': 'pilot:///',
      #                 'action': rp.TRANSFER})
      # pilot.stage_in({'source': 'client:///app_stats.dat',
      #                 'target': 'pilot:///',
      #                 'action': rp.TRANSFER})
      # report.ok('>>ok\n')

        report.header('submit units')
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

      # cudis = list()
      # for f in ['dynamic2.mdp', 'em_results.gro', 'eq_results.gro',
      #           'eq_results.log', 'equilibrium.tpr', 'FF.itp', 'FNF.itp',
      #           'grompp.log', 'Martini.top', 'martini_v2.2.itp', 'mdout.mdp',
      #           'WF.itp']:
      #     cudis.append({'source': 'pilot:///gromacs/%s' % f, 
      #                   'target': 'unit:///%s' % f,
      #                   'action': rp.LINK})
      #
      # share = '/lustre/atlas//world-shared/csc230'
      # path  = '%s/openmpi/applications/gromacs-2018.2/install/bin' % share
      # gmx   = '%s/gmx_mpi' % path
      #
      # args  = "mdrun -o traj.trr -e ener.edr -s topol.tpr -g mdlog.log -c outgro -cpo state.cpt -ntomp $RP_THREADS"
      # args  = "mdrun -s equilibrium.tpr -v -deffnm eq_results -ntomp $RP_THREADS"
        
        report.info('create %d unit description(s)\n\t' % t_num)
        cuds = list()
        for i in range(0, t_num):

            cud = rp.ComputeUnitDescription()

          # cud.executable       = '%s/wl_shape_02.sh' %  pwd
          # cud.executable       = '/usr/bin/time -f $fmt %s %s' % gmx
          # cud.executable       = gmx
          # cud.executable       = '/bin/echo %s' % gmx
            cud.executable       = '/bin/sleep'

          # cud.arguments        = args.split()
            cud.arguments        = [t_time]

          # cud.tags             = tags
            cud.gpu_processes    = 0
            cud.cpu_processes    = t_procs
            cud.cpu_threads      = t_threads

            cud.cpu_process_type = rp.MPI
            cud.cpu_thread_type  = rp.OpenMP
          # cud.input_staging    = cudis
          # cud.timeout          = 300
          # cud.pre_exec         = ["export fmt=\"CPU:%P \""]
          # cud.post_exec        = ['xargs=/usr/bin/xargs', 
          #                         'grep=/usr/bin/grep',
          #                         'cut=/usr/bin/cut', 
          #                         'sed=/usr/bin/sed',
          #                         'bc=/usr/bin/bc',
          #                         'echo=/bin/echo', 
          #                         'cat=/bin/cat', 
          #                         'u=$RP_UNIT_ID', 
          #
          #                         # runtime per RP profiles
          #                       # 'start=$($grep cu_exec_start $u.prof | $cut -f 1 -d ",")',
          #                       # 'stop=$( $grep cu_exec_stop  $u.prof | $cut -f 1 -d ",")',
          #                       # 'val=$($echo "($stop - $start)" | $bc)',
          #                       # '$echo $val > app_stats.dat',
          #
          #                         # avg CPU utilization (via `time -f CPU:%P\n`)
          #                       # 'vals=$($grep -e "^CPU:" STDERR | $cut -f 2 -d ":" | $sed -e "s/%/ /g")',
          #                       # '$echo "vals: $vals"',
          #                       # 'sum="0";n=0; for val in $vals; do sum="$sum + $val"; n=$((n+1)); done',
          #                       # '$echo "sum:  $sum"',
          #                       # 'avg=$($echo  "($sum)/$n/$RP_THREADS" | $bc)',
          #                       # '$echo "avg:  $avg"',
          #                       # '$echo "$avg" > app_stats.dat',
          #
          #                         # gromacs performance (ns/day)
          #                       # "val=$($grep Performance mdlog.log | $xargs $echo | $cut -f 2 -d " ")",
          #                       # '$echo $val > app_stats.dat',
          #
          #                        ]
            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        umgr.submit_units(cuds)
        report.header('gather results')
        umgr.wait_units()


    finally:
        report.header('finalize')
        session.close(download=False)

    report.header()


# ------------------------------------------------------------------------------


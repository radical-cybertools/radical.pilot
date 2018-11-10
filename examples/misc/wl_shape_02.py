#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    resource = str(sys.argv[1])
    n        = int(sys.argv[2])

    session = rp.Session()
    try:

        report.header('submit pilots')
        pd_init = {'resource'      : resource,
                   'runtime'       : 60,
                   'exit_on_error' : True,
                 # 'project'       : 'BIP149',
                 # 'queue'         : 'batch',
                   'access_schema' : 'local',
                   'cores'         : 16 + 16
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('stage data')
        pwd   = os.path.dirname(__file__)
        canon = 'radical.canon/gromacs/large/rawdata'
        pilot.stage_in({'source': 'client://%s/%s/' % (pwd, canon),
                        'target': 'pilot:///',
                        'action': rp.TRANSFER})
        pilot.stage_in({'source': 'client:///app_stats.dat',
                        'target': 'pilot:///',
                        'action': rp.TRANSFER})

        report.ok('>>ok\n')

        report.header('submit units')

        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        tags = {'app-stats'  : 'this_app',
                'constraint' : 'p * t <= 16', 
                'optimizer'  : 'max'}

        cudis = list()
        for f in ['dynamic2_new.mdp', 'FF.itp', 'FNF.itp',
                  'Martini.top', 'WF.itp', 'em_results.gro',
                  'eq_results.gro', 'eq_results.log', 'martini_v2.2.itp']:
            cudis.append({'source': 'pilot:///rawdata/%s' % f, 
                          'target': 'unit:///%s' % f,
                          'action': rp.LINK})

        share = '/lustre/atlas//world-shared/csc230'
        path  = '%s/openmpi/applications/gromacs-2018.2/install/bin' % share
        gmx   = '%s/gmx_mpi' % path


        pre_1 = '/usr/bin/sed -e "s/###ITER###/$iter/g" dynamic2_new.mdp ' \
                    + '> dynamic2.mdp'
        pre_2 = gmx + ' grompp $GROMPP_OPTS $NDXFILE_OPTS' \
                    + ' -f dynamic2.mdp' \
                    + ' -c em_results.gro' \
                    + ' -o equilibrium.tpr' \
                    + ' -p Martini.top'
        args  = 'mdrun $MDRUN_OPTS -s equilibrium.tpr -v -deffnm eq_results'

        report.info('create %d unit description(s)\n\t' % n)
        cuds = list()
        for i in range(0, n):

            cud = rp.ComputeUnitDescription()
          # cud.executable       = '%s/wl_shape_02.sh' %  pwd
          # cud.executable       = '/usr/bin/time -f $fmt %s %s' % gmx
            cud.executable       = gmx
            cud.arguments        = args.split()
            cud.tags             = tags
            cud.gpu_processes    = 0
          # cud.cpu_processes    = '1,2,4,8,16,32'
          # cud.cpu_threads      = '1-8'
            cud.cpu_processes    = '1,2,4'
            cud.cpu_threads      = '1-4'
            cud.cpu_process_type = rp.MPI  
            cud.cpu_thread_type  = rp.OpenMP
            cud.input_staging    = cudis
            cud.timeout          = 300
            cud.pre_exec         = [pre_1,
                                    pre_2,
                                    'export fmt="CPU:%P "']
            cud.post_exec        = ['xargs=/usr/bin/xargs', 
                                    'grep=/usr/bin/grep',
                                    'cut=/usr/bin/cut', 
                                    'sed=/usr/bin/sed',
                                    'bc=/usr/bin/bc',
                                    'echo=/bin/echo', 
                                    'cat=/bin/cat', 
                                    'u=$RP_UNIT_ID', 

                                    # runtime per RP profiles
                                  # 'start=$($grep cu_exec_start $u.prof | $cut -f 1 -d ",")',
                                  # 'stop=$( $grep cu_exec_stop  $u.prof | $cut -f 1 -d ",")',
                                  # 'val=$($echo "($stop - $start)" | $bc)',
                                  # '$echo $val > app_stats.dat',

                                    # avg CPU utilization (via `time -f CPU:%P\n`)
                                    'vals=$($grep -e "^CPU:" STDERR | $cut -f 2 -d ":" | $sed -e "s/%/ /g")',
                                    '$echo "vals: $vals"',
                                    'sum="0";n=0; for val in $vals; do sum="$sum + $val"; n=$((n+1)); done',
                                    '$echo "sum:  $sum"',
                                    'avg=$($echo  "($sum)/$n/$RP_THREADS" | $bc)',
                                    '$echo "avg:  $avg"',
                                    '$echo "$avg" > app_stats.dat',

                                    # gromacs performance (ns/day)
                                  # "val=$($grep Performance mdlog.log | $xargs $echo | $cut -f 2 -d " ")",
                                  # '$echo $val > app_stats.dat',

                                   ]
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


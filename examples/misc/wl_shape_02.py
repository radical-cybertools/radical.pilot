#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    if   len(sys.argv)  > 2:
        report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2:
        resource = sys.argv[1]
    else:
        resource = 'local.localhost'

    session = rp.Session()
    try:

        report.info('read config')
        pwd    = os.path.dirname(os.path.abspath(__file__))
        config = ru.read_json('%s/../config.json' % pwd)
        report.ok('>>ok\n')

        report.header('submit pilots')
        pd_init = {'resource'      : resource,
                   'runtime'       : 15,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'project'       :     config[resource]['project'],
                   'queue'         :     config[resource]['queue'],
                   'access_schema' :     config[resource]['schema'],
                   'cores'         : 8
                  }
        pdesc = rp.PilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('stage data')
        pilot.stage_in({'source': 'client:///gromacs/',
                        'target': 'pilot:///',
                        'action': rp.TRANSFER})
        report.ok('>>ok\n')

        report.header('submit tasks')

        tmgr = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)

        args = 'mdrun -o traj.trr -e ener.edr -s topol.tpr -g mdlog.log ' \
             + '-c outgro -cpo state.cpt'

        tags = {'app-stats'  : 'this_app',
                'constraint' : 'p * t <= 32'}

        cudis = list()
        for f in ['grompp.mdp', 'mdout.mdp', 'start.gro',
                  'topol.top',  'topol.tpr']:
            cudis.append({'source': 'pilot:///gromacs/%s' % f,
                          'target': 'task:///%s' % f,
                          'action': rp.LINK})

        n = 2
        n = 2 * 1024  # number of tasks to run
        report.info('create %d task description(s)\n\t' % n)
        tds = list()
        for i in range(0, n):

            # create a new Task description, and fill it.
            # Here we don't use dict initialization.
            td = rp.TaskDescription()
            td.executable       = 'date'
          # td.arguments        = '1'.split()
          # td.executable       = 'gmx'
          # td.arguments        = args.split()
            td.tags             = tags
            td.gpu_processes    = 0
            td.cpu_processes    = 2  # '1-32'
            td.cpu_threads      = 2  # '1-16'
            td.cpu_process_type = rp.MPI
            td.cpu_thread_type  = rp.OpenMP
      #     td.input_staging    = cudis

            tds.append(td)
            report.progress()
        report.ok('>>ok\n')

        tmgr.submit_tasks(tds)
        report.header('gather results')
        tmgr.wait_tasks()


    finally:
        report.header('finalize')
        session.close(download=True)

    report.header()


# ------------------------------------------------------------------------------


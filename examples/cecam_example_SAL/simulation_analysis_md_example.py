#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot
import radical.utils as ru
import os


dh = ru.DebugHelper ()

RUNTIME         =   10
CORES           =   16
SIM_INSTANCES   =   16
ANA_INSTANCES   =   1           # DO NOT CHANGE !
CYCLES          =   2
SCHED       = radical.pilot.SCHEDULER_DIRECT_SUBMISSION

MY_STAGING_AREA = 'staging:///'

resources = {
        'local.localhost' : {
            'project'  : None,
            'queue'    : None,
            'schema'   : None
            },

        'epsrc.archer' : {
            'project'  : 'e290',
            'queue'    : 'short',
            'schema'   : None
            },

        'xsede.stampede' : {
            'project'  : 'TG-MCB090174',
            'queue'    : 'development',
            'schema'   : None
            },

        }

remote_env = {
    

    'amber': {

        'local.localhost': [],      #For the user to fill in order to run on localhost

        'epsrc.archer': [
                            "module load packages-archer",
                            "module load amber"
                        ],

        'xsede.stampede': [
                            "module load TACC", 
                            "module load amber/12.0"
                        ],

    },

    'coco': {

        'local.localhost': [],      #For the user to fill in order to run on localhost

        'epsrc.archer': [

                           "module load python-compute/2.7.6",
                            "module load pc-numpy/1.8.0-libsci",
                            "module load pc-scipy/0.13.3-libsci",
                            "module load pc-coco/0.18",
                            "module load pc-netcdf4-python/1.1.0",
                            "module load amber"
                        ],

            
        'xsede.stampede': [
                            "module load TACC",
                            "module load intel/13.0.2.146",
                            "module load python/2.7.9",
                            "module load netcdf/4.3.2",
                            "module load hdf5/1.8.13",
                            "export PYTHONPATH=/opt/apps/intel13/mvapich2_1_9/python/2.7.9/lib/python2.7/site-packages:/work/02998/ardi/coco-0.19_installation/lib/python2.7/site-packages:$PYTHONPATH",
                            "export PATH=/work/02998/ardi/coco-0.19_installation/bin:$PATH",
                            "module load amber"
                        ],

    },
}

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if pilot:

        print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
            pilot.uid, state)

        if state == radical.pilot.states.FAILED:
            print "#######################"
            print "##       ERROR       ##"
            print "#######################"
            print "Pilot {0} has FAILED. Can't recover.".format(pilot.uid)
            print "Pilot log:- "
            for log in pilot.log:
                print log.as_dict()
            print u'Pilot STDOUT : {0}'.format(pilot.stdout)
            print u'Pilot STDERR : {0}'.format(pilot.stderr)
            sys.exit(1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    
    if unit:
        print "[Callback]: ComputeUnit '{0}' state changed to {1}.".format(
            unit.uid, state)

        if state == radical.pilot.states.FAILED:
            print "#######################"
            print "##       ERROR       ##"
            print "#######################"
            print "ComputeUnit {0} has FAILED. Can't recover.".format(unit.uid)
            print "ComputeUnit log:- "
            for log in unit.log:
                print log.as_dict()
            print u"STDERR : {0}".format(unit.stderr)
            print u"STDOUT : {0}".format(unit.stdout)
            sys.exit(1)

        elif state == radical.pilot.states.CANCELED:
            print "#######################"
            print "##       ERROR       ##"
            print "#######################"
            print "ComputeUnit {0} was canceled prematurely because the pilot was terminated. Can't recover.".format(unit.uid)
            sys.exit(1)

    else:
        return


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size: %s." % wait_queue_size


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        resource = sys.argv[1]
    else:
        resource = 'local.localhost'

    print 'running on %s' % resource

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = radical.pilot.Session()
    # Add an ssh identity to the session.
    cred = radical.pilot.Context('ssh')
    cred.user_id = 'vivek91'
    session.add_context(cred)

    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        pmgr = radical.pilot.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource      = resource
        pdesc.cores         = CORES
        pdesc.project       = resources[resource]['project']
        pdesc.queue         = resources[resource]['queue']
        pdesc.runtime       = RUNTIME
        pdesc.cleanup       = False
        pdesc.access_schema = resources[resource]['schema']


        pilot = pmgr.submit_pilots(pdesc)


        #------------------------------------------------------------------------------------
        # Data staging with pilot
        pilot_stagein_files = [
                                'mdshort.in',
                                'min.in',
                                'penta.top',
                                'penta.crd',
                                'postexec.py'
                            ]

        md_file, min_file, top_file, crd_file, postexec_file = pilot_stagein_files

        sd_pilot_list = list()
        for item in pilot_stagein_files:
            sd_pilot = {
                        'source': 'file://{0}/{1}'.format(os.getcwd(),item),
                        'target': '{0}{1}'.format(MY_STAGING_AREA,item),
                        'action': radical.pilot.TRANSFER
                        }
            sd_pilot_list.append(sd_pilot)

        sd_pilot = {
                        'source': 'file://{0}/{1}'.format(os.getcwd(),crd_file),
                        'target': '{0}iter0/{1}'.format(MY_STAGING_AREA,crd_file),
                        'action': radical.pilot.TRANSFER
                    }
        sd_pilot_list.append(sd_pilot)

        pilot.stage_in(sd_pilot_list)

        #------------------------------------------------------------------------------------

        umgr = radical.pilot.UnitManager(session=session, scheduler=SCHED)
        umgr.register_callback(unit_state_cb,      radical.pilot.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, radical.pilot.WAIT_QUEUE_SIZE)
        umgr.add_pilots(pilot)

        cycle=0
        
        for cycle in range(0,CYCLES):

            print 'Cycle %s'%(cycle+1)
            print '======================'

            print 'Simulation Step'
            print '----------------------'
            # Minimization Step
            cudesc_list_A = list()
            for i in range(0, SIM_INSTANCES):

                #==================================================================
                # CU Definition for Amber - Stage 1

                cud = radical.pilot.ComputeUnitDescription()
                cud.executable     = 'pmemd'
                cud.pre_exec       = remote_env['amber'][resource]
                cud.arguments      = [
                                        '-O',
                                        '-i',min_file,
                                        '-o','min%s.out'%cycle,
                                        '-inf','min%s.inf'%cycle,
                                        '-r','md%s.crd'%cycle,
                                        '-p',top_file,
                                        '-c',crd_file,
                                        '-ref','min%s.crd'%cycle
                                    ]
                cud.cores          = 1

                #==================================================================
                # Data Staging for the CU

                #------------------------------------------------------------------
                # Coordinate file staging

                if(cycle==0):

                    crd_stage = {
                                'source': MY_STAGING_AREA + 'iter0/' + crd_file,
                                'target': 'min0.crd',
                                'action': radical.pilot.LINK
                                }
                else:

                    crd_stage = {
                                'source': MY_STAGING_AREA + 'iter{0}/min{0}{1}.crd'.format(cycle,i),
                                'target': 'min{0}.crd'.format(cycle),
                                'action': radical.pilot.LINK
                                }

                #------------------------------------------------------------------

                #------------------------------------------------------------------
                # Configure the staging directive for shared input file.
                top_stage = {
                            'source': MY_STAGING_AREA + top_file,
                            'target': top_file,
                            'action': radical.pilot.LINK
                            }
                minin_stage = {
                            'source': MY_STAGING_AREA + min_file,
                            'target': min_file,
                            'action': radical.pilot.LINK
                            }

                init_crd_stage = {
                            'source': MY_STAGING_AREA + crd_file,
                            'target': crd_file,
                            'action': radical.pilot.LINK
                            }
                #------------------------------------------------------------------

                #------------------------------------------------------------------
                # Stage OUT the output to the staging area

                md_stage_out = {
                            'source': 'md{0}.crd'.format(cycle),
                            'target': MY_STAGING_AREA + 'iter{0}/md{0}{1}.crd'.format(cycle,i),
                            'action': radical.pilot.LINK
                            }
                #----------------------------------------------------------

                cud.input_staging = [crd_stage,top_stage,minin_stage,init_crd_stage]
                cud.output_staging = [md_stage_out]

                cudesc_list_A.append(cud)


            # MD step
            cudesc_list_B = []
            for i in range(0,SIM_INSTANCES):

                #==================================================================
                # CU Definition for Amber - Stage 2

                cud = radical.pilot.ComputeUnitDescription()
                cud.executable     = 'pmemd'
                cud.pre_exec       = remote_env['amber'][resource]
                cud.arguments      = [
                                    '-O',
                                    '-i', md_file,
                                    '-o', 'md%s.out'%cycle,
                                    '-inf', 'md%s.inf'%cycle,
                                    '-x', 'md%s.ncdf'%cycle,
                                    '-r', 'md%s.rst'%cycle,
                                    '-p', top_file,
                                    '-c', 'md%s.crd'%cycle
                                    ]
                cud.cores          = 1

                #==================================================================
                # Data Staging

                #------------------------------------------------------------------
                # Link to output from first-stage of Amber
                md_stage_in = {
                                'source': MY_STAGING_AREA + 'iter{0}/md{0}{1}.crd'.format(cycle,i),
                                'target': 'md{0}.crd'.format(cycle),
                                'action': radical.pilot.LINK
                                }

                # Link to shared data from staging area
                mdin_stage = {
                                'source': MY_STAGING_AREA + md_file,
                                'target': md_file,
                                'action': radical.pilot.LINK
                            }
                top_stage = {
                                'source': MY_STAGING_AREA + top_file,
                                'target': top_file,
                                'action': radical.pilot.LINK
                            }

                cud.input_staging = [md_stage_in,mdin_stage,top_stage]
                #------------------------------------------------------------------


                #------------------------------------------------------------------
                # Link output data to staging area
                ncdf_stage_out = {
                                'source': 'md{0}.ncdf'.format(cycle),
                                'target': MY_STAGING_AREA + 'iter{0}/md_{0}_{1}.ncdf'.format(cycle, i),
                                'action': radical.pilot.COPY
                                }
                cud.output_staging = [ncdf_stage_out]
                #------------------------------------------------------------------

                cudesc_list_B.append(cud) 


            #Submit all Amber CUs in Stage 1
            cu_list_A = umgr.submit_units(cudesc_list_A)

            cu_list_B = []
            cu_list_A_copy = cu_list_A[:]

            while cu_list_A:
                for cu_a in cu_list_A:
                    idx = cu_list_A_copy.index(cu_a)
                    cu_a.wait ()
                    cu_list_B.append(umgr.submit_units(cudesc_list_B[idx]))
                    cu_list_A.remove(cu_a)

            umgr.wait_units()    


            #==================================================================
            # CU Definition for CoCo

            print 'Analysis Step'
            print '----------------------'

            cud = radical.pilot.ComputeUnitDescription()
            cud.cores = SIM_INSTANCES  #pyCoCo should use as many as cores as the number of frontpoints
            cud.executable = 'pyCoCo'
            cud.pre_exec = remote_env['coco'][resource]
            cud.arguments = [
                                '--grid','5',
                                '--dims','3',
                                '--frontpoints',SIM_INSTANCES,
                                '--topfile',top_file,
                                '--mdfile','*.ncdf',
                                '--output','pdbs',
                                '--logfile','coco.log',
                            ]
            cud.post_exec = ['python postexec.py %s %s' % (SIM_INSTANCES,cycle)]
            cud.mpi = False


            #==================================================================
            # Data Staging for the CU

            #------------------------------------------------------------------
            # postexec and topology file staging

            postexec_stage = {
                            'source': MY_STAGING_AREA + 'postexec.py',
                            'target': 'postexec.py',
                            'action': radical.pilot.LINK
                        }

            top_stage = {
                            'source': MY_STAGING_AREA + top_file,
                            'target': top_file,
                            'action': radical.pilot.LINK
                        }
            cud.input_staging = [postexec_stage,top_stage]
            #------------------------------------------------------------------


            #------------------------------------------------------------------
            # stage in the ncdf files from all previous iterations
            for iter in range(0,cycle+1):
                for inst in range(0,SIM_INSTANCES):
                    dir = {
                            'source': MY_STAGING_AREA + 'iter{0}/md_{0}_{1}.ncdf'.format(iter,inst),
                            'target': 'md_{0}_{1}.ncdf'.format(iter,inst),
                            'action': radical.pilot.COPY
                            }
                    cud.input_staging.append(dir)
            #------------------------------------------------------------------

            cud.output_staging = []
            #------------------------------------------------------------------
            # stage out the crd files to staging area
            for inst in range(0,SIM_INSTANCES):
                dir = {
                        'source': 'min{0}{1}.crd'.format(cycle,inst),
                        'target': MY_STAGING_AREA + 'iter{2}/min{0}{1}.crd'.format(cycle,inst,cycle+1),
                        'action': radical.pilot.LINK
                        }
                cud.output_staging.append(dir)
            #------------------------------------------------------------------

            #------------------------------------------------------------------
            # stage out the crd files to localhost
            for inst in range(0,SIM_INSTANCES):
                dir = {
                        'source': 'min{0}{1}.crd'.format(cycle,inst),
                        'target': 'backup/iter{1}/min{0}.crd'.format(inst,cycle+1),
                        }
                cud.output_staging.append(dir)
            #------------------------------------------------------------------

            unit = umgr.submit_units(cud)

            unit.wait()



    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------


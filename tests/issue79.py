import radical.pilot as rp


PILOT_DESCR = {'project':     None, 
               'resource':    'sierra.futuregrid.org', 
               'queue':       'interactive', 
               'sandbox':     '/N/u/merzky//troy_agents/', 
               'cleanup':     None, 
               'cores':       4, 
               'runtime':     600}
UNIT_DESCR  = {'environment': None,
               'input_data':  None,
               'executable':  '/N/u/marksant/bin/mdrun',
               'name':        None,
               'output_data': None,
               'cores':       None,
               'working_directory_priv': '/N/u/merzky//troy_tutorial/troy_tutorial_01_2/',
               'arguments':   ['topol.tpr']}


session = sp.Session           ()
sp_um    = sp.UnitManager      (session      = session, 
                                scheduler    = 'direct_submission')
sp_pm    = sp.PilotManager     (session      = session)
sp_pilot_descr = sp.ComputePilotDescription ()
sp_pilot_descr.resource = PILOT_DESCR['resource']
sp_pilot_descr.cores    = PILOT_DESCR['cores']
sp_pilot_descr.runtime  = PILOT_DESCR['runtime']
sp_pilot_descr.queue    = PILOT_DESCR['queue']
sp_pilot_descr.sandbox  = PILOT_DESCR['sandbox']

sp_pilot = sp_pm.submit_pilots (sp_pilot_descr)

sp_um.add_pilots (sp_pilot)
sp_cu_descr = sp.ComputeUnitDescription ()
for key in UNIT_DESCR :
    sp_cu_descr[key] = UNIT_DESCR[key]


sp_cu    = sp_um.submit_units  (sp_cu_descr)

print "STATE: %s" % sp_cu.state


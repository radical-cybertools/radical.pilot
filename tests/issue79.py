
import radical.pilot as rp


# ##############################################################################
# #79: Cannot get stderr for failed CU
# ##############################################################################


session = rp.Session      ()
umgr    = rp.UnitManager  (session      = session, 
                           scheduler    = rp.SCHED_DIRECT_SUBMISSION)
pmgr    = rp.PilotManager (session      = session)

pilot_descr = rp.ComputePilotDescription ()
pilot_descr.resource = 'local.localhost'
pilot_descr.cores    = 1
pilot_descr.runtime  = 10

pilot = pmgr.submit_pilots (pilot_descr)
umgr.add_pilots (pilot)

unit_descr = rp.ComputeUnitDescription()
unit_descr.executable = '/road/to/nowhere/does_not_exist'

unit = umgr.submit_units (unit_descr)

unit.wait ()

assert ('/road/to/nowhere/does_not_exist: No such file or directory' in unit.stderr)
assert (unit.state == rp.FAILED)

pmgr.cancel_pilots()       
pmgr.wait_pilots()       
session.close ()


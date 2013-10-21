
import sinon
import sys

print 0

print sinon.version

print 1

pd       = sinon.ComputePilotDescription ()
pd.resource = 'local'
pd.slots    = 10

print 2

um       = sinon.UnitManager (scheduler='round_robin')

print 3

ud       = sinon.ComputeUnitDescription ({'executable'    :'/usr/bin/touch', 
                                          'arguments'     : ['/tmp/sinon_bj_touch'],
                                          "number_of_processes" : 1,            
                                          "spmd_variation":"single",
                                          "output"        : "/tmp/bjstdout.txt",
                                          "error"         : "/tmp/bjstderr.txt"})
u1       = um.submit_unit (ud)
uid1     = u1.uid

print 4

pm       = sinon.PilotManager ()
p1       = pm.submit_pilot (pd)
p2       = pm.submit_pilot (pd)

print 5
print p1
print p2

p1.wait (state=sinon.RUNNING, timeout=10.0)

um.add_pilot (p1)
um.add_pilot (p2)
u2       = um.submit_unit (ud)
u3       = um.submit_unit (ud)
u4       = um.submit_unit (ud)
u5       = um.submit_unit (ud)
u6       = um.submit_unit (ud)

print str(u1)

print 6

u1.wait ()
u2.wait ()
u3.wait ()
u4.wait ()
u5.wait ()
u6.wait ()

print 7

u3 = sinon.ComputeUnit (uid=uid1)
u3.wait ()

print 8


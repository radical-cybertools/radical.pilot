#!/usr/bin/env python


import radical.pilot as rp

print " >>> test__issue_114_part_3"

session = rp.Session()

try:

    pm = rp.PilotManager(session=session)
    
    cpd = rp.ComputePilotDescription()
    cpd.resource = "local.localhost"
    cpd.cores    = 1
    cpd.runtime  = 1
    cpd.sandbox  = "/tmp/radical.pilot.sandbox.unittests"
    cpd.cleanup  = True
    
    pilot = pm.submit_pilots(pilot_descriptions=cpd)
    
    um = rp.UnitManager(
        session   = session,
        scheduler = rp.SCHEDULER_DIRECT_SUBMISSION
    )
    um.add_pilots(pilot)
    
    state = pm.wait_pilots(state=[rp.ACTIVE, 
                                  rp.DONE, 
                                  rp.FAILED], 
                                  timeout=20*60)
    
    assert state       == [rp.ACTIVE], 'state      : %s' % state    
    assert pilot.state ==  rp.ACTIVE , 'pilot state: %s' % pilot.state 
    
    state = pm.wait_pilots(timeout=3*60)
    
    print "pilot %s: %s / %s" % (pilot.uid, pilot.state, state)
    for entry in pilot.state_history :
        print "      %s : %s" % (entry.timestamp, entry.state)
    for log in pilot.log :
        print "      log : %s" % log
    
    assert state       == [rp.DONE], 'state      : %s' % state        
    assert pilot.state ==  rp.DONE , 'pilot state: %s' % pilot.state  
    
    print " <<< test__issue_114_part_3"

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    session.close()


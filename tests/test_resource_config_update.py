#!/usr/bin/env python


import sys
import pprint
import radical.pilot as rp

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
# Create a new session. A session is the 'root' object for all other
# RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
# well as security contexts.
session = rp.Session()

try:
    
    RESOURCE = 'epsrc.archer'
    
    # get a pre-installed resource configuration
    cfg = session.get_resource_config(RESOURCE)
    pprint.pprint (cfg)
    
    # create a new config based on the old one, and set a different launch method
    new_cfg = rp.ResourceConfig(RESOURCE, cfg)
    new_cfg.default_queue = 'royal_treatment'
    
    # now add the entry back.  As we did not change the config name, this will
    # replace the original configuration.  A completely new configuration would
    # need a unique label.
    session.add_resource_config(new_cfg)
    pprint.pprint (session.get_resource_config(RESOURCE))


except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()


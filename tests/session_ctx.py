#!/usr/bin/env python


import radical.pilot as rp

s1 = None
s2 = None

try:
  s1 = rp.Session()
  print "Session 1: %s (%d)" % (s1.uid, len(s1.list_contexts()))
  
  # Add an ssh identity to the session.
  c1 = rp.Context('ssh')
  c1.user_id = "tg802352"
  
  print 'context 1: %s' % c1
  
  s1.add_context(c1)
  
  # Add an ssh identity to the session.
  c2 = rp.Context('ssh')
  c2.user_id = "abcedesds"
  
  print 'context 2: %s' % c2
  
  s1.add_context(c2)
  
  for c in s1.list_contexts():
      print c
  
  
  s2 = rp.Session(uid=s1.uid)
  print "Session 2: %s (%d)" % (s2.uid, len(s2.list_contexts()))
  
  
  for c in s2.list_contexts():
      print c
  
  assert (len(s1.list_contexts()) == len(s2.list_contexts()))

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    if s1: s1.close() 
    if s2: s2.close()


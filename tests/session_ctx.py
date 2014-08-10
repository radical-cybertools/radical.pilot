import sagapilot


session = sagapilot.Session()
print "Session 1: %s " % session.uid

# Add an ssh identity to the session.
c1 = sagapilot.Context('ssh')
c1.user_id = "tg802352"

print c1

session.add_context(c1)

# Add an ssh identity to the session.
c2 = sagapilot.Context('ssh')
c2.user_id = "abcedesds"

print c2

session.add_context(c2)

for c in session.list_contexts():
    print c


session2 = sagapilot.Session(session_uid=session.uid)
print "Session 2: %s " % session2.uid

for c in session2.list_contexts():
    # contexts are *not* serialized as part of the session information, but need
    # to be added again!
    print c


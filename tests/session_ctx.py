import sagapilot


DBURL  = "mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017"

session = sagapilot.Session(database_url=DBURL)
print "Session: {0} ".format(session)

# Add an ssh identity to the session.
cred1 = sagapilot.SSHCredential()
cred1.user_id = "tg802352"

print cred1

session.add_credential(cred1)

# Add an ssh identity to the session.
cred2 = sagapilot.SSHCredential()
cred2.user_id = "abcedesds"

print cred2

session.add_credential(cred2)

for c in session.list_credentials():
    print c


session2 = sagapilot.Session(database_url=DBURL, session_uid=session.uid)
print "Session: {0} ".format(session2)

for c in session2.list_credentials():
    print c

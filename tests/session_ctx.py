import sagapilot


# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("SAGAPILOT_DBURL")
if DBURL is None:
    print "ERROR: SAGAPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

session = sagapilot.Session(database_url=DBURL)
print "Session: {0} ".format(session)

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


session2 = sagapilot.Session(database_url=DBURL, session_uid=session.uid)
print "Session: {0} ".format(session2)

for c in session2.list_contexts():
    # contexts are *not* serialized as part of the session information, but need
    # to be added again!
    print c


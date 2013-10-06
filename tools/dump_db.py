#!/usr/bin/env python

import sinon
import saga

print "-----------------------------------------"
_, root_dir = sinon.initialize ()

# base_url points to the session dir.  We want to go up for two levels (session
# dir, user dir).
root_dir = saga.advert.Directory (str(root_dir.url)+ '/../..')
print "sinon root: %s" % root_dir.url

entries = root_dir.list (flags=saga.advert.RECURSIVE)

if  entries :
    entries.sort ()

print "-----------------------------------------\n"
for entry in entries :
    
    print entry

    ad_url      = root_dir.url
    ad_url.path = entry
    ad          = root_dir.open (str(ad_url))
    data        = ad.as_dict ()

    for key in data.keys () :
        print "    %-10s : %s" % (key, data[key])

print "\n-----------------------------------------\n"


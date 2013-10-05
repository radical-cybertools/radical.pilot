#!/usr/bin/env python

import sinon
import saga

_, base_url = sinon.initialize ()

# base_url points to the session dir.  We want to go up for two levels (session
# dir, user dir).
root_dir = saga.advert.Directory (str(base_url)+ '/../..')
print "sinon root: %s" % root_dir.url

entries = root_dir.list (flags=saga.advert.RECURSIVE)

if  entries :
    entries.sort ()

for entry in entries :
    
    print entry

    ad_url      = base_url
    ad_url.path = entry
    ad          = root_dir.open (str(ad_url))
    data        = ad.as_dict ()

    for key in data.keys () :
        print "  %-10s : %s" % (key, data[key])


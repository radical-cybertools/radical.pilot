#!/usr/bin/env python3

import radical.utils as ru

cfg = ru.Config("radical.pilot.resource", name="*")

print('''
.. csv-table:: Launch Methods
   :header: "Site", "Resource", "Launch Methods"
   :widths: 15, 25, 30

   ''')

for site in cfg:
    for r in cfg[site]:
        try:
            lms = cfg[site][r]['launch_methods'].keys() - ['order']
            print('    %-15s. %-25s, %s' % (site, r, ' '.join(lms)))
        except:
            print('    %-15s, %-25s, ' % (site, r))

print()
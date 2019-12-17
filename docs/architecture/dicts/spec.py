#!/usr/bin/env python3

# ------------------------------------------------------------------------------
# given a layout specification, benchmark time to
#
#   - create of 10^6
#   - fill 10^6
#   - deep-copy 10^6
#   - change one attribute in 10^6
#
# dicts or similar structures
# ------------------------------------------------------------------------------


import copy
import time

import radical.utils as ru

from spec_rs         import CUD
from spec_typed_dict import TDD
from spec_config     import Cfg


import spec_attribs as a


# ------------------------------------------------------------------------------
#
checks = [dict, TDD, Cfg, CUD]
checks = [dict, TDD, Cfg]


for check in checks:

    n = 1024 * 1024
    l = list()
    t = list()
    r = ru.Reporter('radical.test')
    r.progress_tgt(n)

    t0 = time.time()

    # -----------------------------------------------
    # create n entities
    for i in range(n):
        e = check()
        l.append(e)
        r.progress()
    r.progress_done()

    t1 = time.time()
    t.append(['create', t1 - t0, 'sec'])

    # -----------------------------------------------
    # fill n entities
    for i, e in enumerate(l):
        e[a.NAME] = 'name.%09d' % i

        for att in a.attribs_s:
            e[att] = 'foo'
        for att in a.attribs_dict_ss:
            e[att] = {'foo': 'bar', 'buz': 'baz'}
        for att in a.attribs_dict_aa:
            e[att] = {'foo': True,  'buz': 2}
        for att in a.attribs_list_s:
            e[att] = ['foo', 'bar', 'buz', 'baz']
        for att in a.attribs_any:
            e[att] = ['foo', {'bar': ['buz', 2, True]}]
        for att in a.attribs_list_a:
            e[att] = ['foo', {'bar': ['buz', 2, True]}]
        for att in a.attribs_int:
            e[att] = 3

    t2 = time.time()
    t.append(['fill', t2 - t1, 'sec'])

    # -----------------------------------------------
    # change one attribute in n entities
    for i, e in enumerate(l):
        e[a.NAME] = 'name.%09d' % i

    t3 = time.time()
    t.append(['change', t3 - t2, 'sec'])

    # -----------------------------------------------
    # deep-copy n entities
    c = list()
    for e in l:
        c.append(copy.deepcopy(e))

    t4 = time.time()
    t.append(['copy', t4 - t3, 'sec'])

    t.append(['size', ru.get_size(l) / (1024 * 1024), 'MB'])

    for n,x,u in t:
        print('      %-10s: %11.3f %s' % (n, x, u))


# ------------------------------------------------------------------------------


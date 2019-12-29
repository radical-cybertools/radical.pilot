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

# import resource

import radical.utils as ru

from spec_rs         import CUD
from spec_typed_dict import TDD  # pip install mypy
from spec_pydantic   import PYD  # pip install pydantic
from spec_good       import GOD  # pip install good
from spec_schema     import SCH  # pip install schema
from spec_rudict     import RUD
from spec_rumunch    import RUM
from spec_config     import CFG


import spec_attribs as a


# ------------------------------------------------------------------------------
#
checks = [CUD, GOD, SCH, RUM, dict, TDD, CFG, RUD, PYD]
checks = [               RUM, dict, TDD, CFG, RUD, PYD]


for check in checks:

    n = 1024 * 1024
    l = list()
    t = list()
    r = ru.Reporter('radical.test')
    r.progress_tgt(n * 5, label=check.__name__)

    t0 = time.time()

    # -----------------------------------------------
    # create n entities
    for i in range(n):
        e = check()
        l.append(e)
        r.progress()

    t1 = time.time()
    t.append(['create', t1 - t0, 'sec'])

    # -----------------------------------------------
    # fill n entities
    r.ok('\b|')
    for i, e in enumerate(l):
        e[a.NAME] = 'name.%09d' % i

        for att in a.attribs_s:
            e[att] = 'foo'
        for att in a.attribs_b:
            e[att] = False
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
        r.progress()

    t2 = time.time()
    t.append(['fill', t2 - t1, 'sec'])

    # -----------------------------------------------
    # change one attribute in n entities
    r.ok('\b|')
    for i, e in enumerate(l):
        e[a.NAME] = 'name.%09d' % i
        r.progress()

    t3 = time.time()
    t.append(['change', t3 - t2, 'sec'])

    # -----------------------------------------------
    # deep-copy n entities
    c = list()
    r.ok('\b|')
    for e in l:
        c.append(copy.deepcopy(e))
        r.progress()

    t4 = time.time()
    t.append(['copy', t4 - t3, 'sec'])

    # -----------------------------------------------
    # check type/val errors
    i = 0
    r.ok('\b|')
    for e in l:
        try:
            e[a.CPU_PROCESSES] = 'foo'
            e.validate()
        except AttributeError:
            pass
        except Exception as E:
            i += 1
          # print(E)
          # raise
        r.progress()

    t5 = time.time()
    t.append(['check', t5 - t4, 'sec [%d]' % i])
    t.append(['total', t5 - t0, 'sec'])

    t.append(['size', ru.get_size(l) / (1024 * 1024), 'MB'])
  # t.append(['size', resource.getrusage(1)[2] / (1024), 'MB'])

    print()
    for n,x,u in t:
        print('      %-10s: %11.3f %s' % (n, x, u))


# ------------------------------------------------------------------------------


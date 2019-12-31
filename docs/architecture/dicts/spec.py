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

from spec_rs         import RSA
from spec_rp         import CUD
from spec_typed_dict import TDD  # pip install mypy
from spec_pydantic   import PYD  # pip install pydantic
from spec_good       import GOD  # pip install good
from spec_schema     import SCH  # pip install schema
from spec_rudict     import RUD
from spec_rumunch    import RUM
from spec_config     import CFG

import spec_attribs as a

class DIC(dict):
    pass

# ------------------------------------------------------------------------------
#
checks = [RSA, GOD, SCH, RUM, DIC, TDD, CFG, CUD, RUD, PYD]
checks = [               RUM, DIC, TDD, CFG, CUD, RUD, PYD]
checks = [               RUM,                CUD          ]

data   = list()

for check in checks:

    results = [check.__name__]

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
    results.append(t1 - t0)

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
    results.append(t2 - t1)

    # -----------------------------------------------
    # change one attribute in n entities
    r.ok('\b|')
    for i, e in enumerate(l):
        e[a.NAME] = 'name.%09d' % i
        r.progress()

    t3 = time.time()
    results.append(t3 - t2)

    # -----------------------------------------------
    # deep-copy n entities
    c = list()
    r.ok('\b|')
    for e in l:
        c.append(copy.deepcopy(e))
        r.progress()

    t4 = time.time()
    results.append(t4 - t3)

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
    results.append(t5 - t4)
    results.append(i)
    results.append(t5 - t0)
    results.append(ru.get_size(l) / (1024 * 1024))
  # results.append(resource.getrusage(1)[2] / (1024))

    data.append(results)
    print()


print('+------+--------+--------+--------+--------+--------+--------+--------+--------+')
print('| name | create |   fill | change |   copy |  check |  found |  total |   size |')
print('|      |  [sec] |  [sec] |  [sec] |  [sec] |  [sec] |    [n] |  [sec] |   [MB] |')
print('+------+--------+--------+--------+--------+--------+--------+--------+--------+')

data.sort(key=lambda x: x[7])
for results in data:
    print('| %-4s | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6d | %6.2f | %6d |'
            % tuple(results))

print('+------+--------+--------+--------+--------+--------+--------+--------+--------+')

# ------------------------------------------------------------------------------


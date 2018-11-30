

# pylint: disable=protected-access, unused-argument

import os
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def setUp(test_type, test_name):

    pwd = os.path.dirname(__file__)
    ret = list()

    tests = ru.read_json('%s/results_%s.json' % (pwd, test_name))
    uids = tests.keys()

    for uid in uids:
        fin = '%s/../../test_cases/%s.json' % (pwd, uid)
        tc = ru.read_json(fin)
        unit = tc['unit']
        setup = tc['setup'].get(test_type, {})
        test = ru.dict_merge(unit, setup, ru.PRESERVE)
        result = tests[uid]

        ret.append([test, result])

    return ret

# ------------------------------------------------------------------------------
#


def tearDown():

    pass


# ------------------------------------------------------------------------------

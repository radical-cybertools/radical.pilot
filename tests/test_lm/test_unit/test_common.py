
# pylint: disable=protected-access, unused-argument

import os
import glob

import radical.utils as ru


# ------------------------------------------------------------------------------
# 
def setUp(test_type, test_name):

    pwd = os.path.dirname(__file__)
    ret = list()
    for fin in glob.glob('%s/../../test_cases/unit.*.json' % pwd):

        tc            = ru.read_json(fin)
        unit          = tc['unit'   ]
        setup         = tc['setup'  ].get(test_type, {})
        result        = tc['results'].get(test_type, {}).get(test_name)
        resource_file = tc['results'].get('resource_file', {}).get(test_name)
        test          = ru.dict_merge(unit, setup, ru.PRESERVE)

        if result:
            if resource_file:
                ret.append([test, result, resource_file])
            else:
                ret.append([test, result])

    return ret


# ------------------------------------------------------------------------------
#
def tearDown():

    pass

# ------------------------------------------------------------------------------

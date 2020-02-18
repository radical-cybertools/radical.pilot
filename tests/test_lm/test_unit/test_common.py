
# pylint: disable=protected-access, unused-argument

import glob

import radical.utils as ru


# ------------------------------------------------------------------------------
# 
def setUp(test_type, test_name):

    ret = list()
    for fin in glob.glob('tests/test_cases/unit.*.json'):

        tc                = ru.read_json(fin)
        unit              = tc['unit'   ]
        setup             = tc['setup'  ].get(test_type, {})
        result            = tc['results'].get(test_type, {}).get(test_name)
        resource_file     = tc['results'].get('resource_file', {}).get(test_name)
        resource_filename = tc['results'].get('resource_filename', {}).get(test_name)
        test              = ru.dict_merge(unit, setup, ru.PRESERVE)

        if result:
            if resource_file and resource_filename:
                ret.append([test, result, resource_file, resource_filename])
            else:
                ret.append([test, result])

    return ret


# ------------------------------------------------------------------------------
#
def tearDown():

    pass

# ------------------------------------------------------------------------------

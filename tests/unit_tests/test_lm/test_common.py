
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import glob

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def setUp(test_type, test_name):

    ret = list()
    pwd = os.path.dirname(__file__)
    for fin in glob.glob('%s/../test_cases/task.*.json' % pwd):

        tc                = ru.read_json(fin)
        task              = tc['task'   ]
        setup             = tc['setup'  ].get(test_type, {})
        results           = tc['results']
        result            = results.get(test_type, {}).get(test_name)
        resource_file     = results.get('resource_file', {}).get(test_name)
        resource_filename = results.get('resource_filename', {}).get(test_name)
        test              = ru.dict_merge(task, setup, ru.PRESERVE)

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
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

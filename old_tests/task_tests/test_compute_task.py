#!/usr/bin/env python

import pytest

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
def test_cu_sandbox():

    session = None
    p_sbox  = 'foo://bar.buz/fiz/'
    p_dict  = {'pilot_sandbox' : p_sbox}

    sboxes  = [[None,                   '%s/task.000000/'   % p_sbox],
               ['test_1/',              '%s/test_1/'        % p_sbox],
               ['test_1/test_2/',       '%s/test_1/test_2/' % p_sbox],
               ['/tmp/test_3/',         None],
               ['/tmp/test_3/test_5/',  None],
              ]

    try:
        session = rp.Session()
        tmgr    = rp.TaskManager(session=session)
    
        for sbox_in, sbox_out in sboxes:

            td = rp.TaskDescription()
            td.executable = 'true'  # required attribute
            td.sandbox    = sbox_in

            if not sbox_out:
                with pytest.raises(ValueError):
                    task = tmgr.submit_tasks(td)

                continue

            task   = tmgr.submit_tasks(td)
            u_dict = task.as_dict()
            check  = session._get_task_sandbox(u_dict, p_dict)

            while check.endswith('//'):
                check = check[:-1]

            print '%s == %s' % (check, sbox_out)
            assert(check == sbox_out), '%s != %s' % (check, sbox_out)

    finally:

        if session:
            session.close(download=False)



# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_cu_sandbox()


# ------------------------------------------------------------------------------


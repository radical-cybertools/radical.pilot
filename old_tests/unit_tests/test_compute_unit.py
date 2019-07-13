#!/usr/bin/env python

import pytest

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
def test_cu_sandbox():

    session = None
    p_sbox  = 'foo://bar.buz/fiz/'
    p_dict  = {'pilot_sandbox' : p_sbox}

    sboxes  = [[None,                   '%s/unit.000000/'   % p_sbox],
               ['test_1/',              '%s/test_1/'        % p_sbox],
               ['test_1/test_2/',       '%s/test_1/test_2/' % p_sbox],
               ['/tmp/test_3/',         None],
               ['/tmp/test_3/test_5/',  None],
              ]

    try:
        session = rp.Session()
        umgr    = rp.UnitManager(session=session)
    
        for sbox_in, sbox_out in sboxes:

            cud = rp.ComputeUnitDescription()
            cud.executable = 'true'  # required attribute
            cud.sandbox    = sbox_in

            if not sbox_out:
                with pytest.raises(ValueError):
                    unit = umgr.submit_units(cud)

                continue

            unit   = umgr.submit_units(cud)
            u_dict = unit.as_dict()
            check  = session._get_unit_sandbox(u_dict, p_dict)

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


#!/bin/sh


echo '-------------------------------------------------------------------------'
test "$RP_TEST"        = 'TASK' && echo "RP_TEST       : ok" || echo "RP_TEST       : $RP_TEST"
test "$RP_TEST_VIRGIN" = 'TASK' && echo "RP_TEST_VIRGIN: ok" || echo "RP_TEST_VIRGIN: $RP_TEST_VIRGIN"

test   -z "$RP_TEST_BOOT"   && echo "RP_TEST_BOOT  : ok" || echo "RP_TEST_BOOT  : $RP_TEST_BOOT"
test   -z "$RP_TEST_AGENT"  && echo "RP_TEST_AGENT : ok" || echo "RP_TEST_AGENT : $RP_TEST_AGENT"
test   -z "$RP_TEST_EXEC"   && echo "RP_TEST_EXEC  : ok" || echo "RP_TEST_EXEC  : $RP_TEST_EXEC"
test   -z "$RP_TEST_LAUNCH" && echo "RP_TEST_LAUNCH: ok" || echo "RP_TEST_LAUNCH: $RP_TEST_LAUNCH"
test   -z "$RP_TEST_LM_1"   && echo "RP_TEST_LM_1  : ok" || echo "RP_TEST_LM_1  : $RP_TEST_LM_1"
test   -z "$RP_TEST_LM_2"   && echo "RP_TEST_LM_2  : ok" || echo "RP_TEST_LM_2  : $RP_TEST_LM_2"
test   -z "$RP_TEST_PRE"    && echo "RP_TEST_PRE   : ok" || echo "RP_TEST_PRE   : $RP_TEST_PRE"
test   -z "$RP_TEST_RANK0"  && echo "RP_TEST_RANK0 : ok" || echo "RP_TEST_RANK0 : $RP_TEST_RANK0"
test ! -z "$RP_TEST_TASK"   && echo "RP_TEST_TASK  : ok" || echo "RP_TEST_TASK  : $RP_TEST_PRE"

echo "hello world: $OMPI_COMM_WORLD_RANK $RP_TEST_VIRGIN $RP_TEST_PRE $RP_TEST_TASK"

env | sort > ./env.check

echo '-------------------------------------------------------------------------'


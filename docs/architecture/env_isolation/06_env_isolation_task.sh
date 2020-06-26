#!/bin/sh

echo '--- rank $PMIX_RANK------------------------------------------------------'
printf "RP_TEST       : %-10s : %s\n" "$RP_TEST"            "RANK0 / PRE"
printf "RP_TEST       : %-10s : %s\n" "$RP_TEST"            "RANK0 / PRE"
printf "RP_TEST_VIRGIN: %-10s : %s\n" "$RP_TEST_VIRGIN"     "True"
printf "RP_TEST_BOOT  : %-10s : %s\n" "$RP_TEST_BOOT"       "-"
printf "RP_TEST_AGENT : %-10s : %s\n" "$RP_TEST_AGENT"      "-"
printf "RP_TEST_EXEC  : %-10s : %s\n" "$RP_TEST_EXEC"       "-"
printf "RP_TEST_LAUNCH: %-10s : %s\n" "$RP_TEST_LAUNCH"     "-"
printf "RP_TEST_LM_1  : %-10s : %s\n" "$RP_TEST_LM_1"       "-"
printf "RP_TEST_LM_2  : %-10s : %s\n" "$RP_TEST_LM_2"       "-"
printf "RP_TEST_PRE   : %-10s : %s\n" "$RP_TEST_PRE"        "True"
printf "RP_TEST_RANK0 : %-10s : %s\n" "$RP_TEST_RANK0"      "True / -"
printf "HELLO WORLD   : $PMIX_RANK $RP_TEST_VIRGIN $RP_TEST_PRE\n"

if test "$PMIX_RANK" = 1
then
    env | sort > ./env.check.env
fi

echo '-------------------------------------------------------------------------'


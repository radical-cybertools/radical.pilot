#!/bin/sh

printf "HELLO WORLD $PMIX_RANK $RP_TEST_VIRGIN $RP_TEST_PRE\n"
printf "+-%-15s-+-%-10s-+-%-30s-|\n" "---------------" "----------" "------------------------------"
printf "+ %-15s | %-10s | %-30s |\n" "RANK $PMIX_RANK" "" ""
printf "| %-15s | %-10s | %-30s |\n" "VAR NAME       " "VALUE"               "EXPECTED VALUE"
printf "+-%-15s-+-%-10s-+-%-30s-|\n" "---------------" "----------" "------------------------------"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST        " "$RP_TEST"            "RANK0 / PRE"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_VIRGIN"  "$RP_TEST_VIRGIN"     "True"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_BOOT"    "$RP_TEST_BOOT"       "-"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_AGENT"   "$RP_TEST_AGENT"      "-"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_EXEC"    "$RP_TEST_EXEC"       "-"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_LAUNCH"  "$RP_TEST_LAUNCH"     "-"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_LM_1"    "$RP_TEST_LM_1"       "-"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_LM_2"    "$RP_TEST_LM_2"       "-"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_PRE"     "$RP_TEST_PRE"        "True"
printf "| %-15s | %-10s | %-30s |\n" "RP_TEST_RANK0"   "$RP_TEST_RANK0"      "True / -"
printf "+-%-15s-+-%-10s-+-%-30s-|\n" "---------------" "----------" "------------------------------"

if test "$PMIX_RANK" = 1
then
    env | sort > ./env.check.env
fi


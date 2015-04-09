#!/bin/sh

# JENKINS_VERBOSE 
#     TRUE : print logs and stdio while running the tests
#     else : print logs and stdio only on failure, after running the test
#
# JENKINS_EXIT_ON_FAIL:
#     TRUE : exit immediately when a test fails
#     else : run all tests, independent of success/failure (exit code reflects
#            failures though)I



failed=0

export SUCCESS_MARKER="JENKINS TEST SUCCESS"

export SAGA_VERBOSE=DEBUG
export RADICAL_VERBOSE=DEBUG
export RADICAL_UTILS_VERBOSE=DEBUG
export RADICAL_PILOT_VERBOSE=DEBUG


for s in integration mpi
do
    break

    tests=`cat jenkins.cfg | sed -e 's/#.*//g' | grep -v '^ *$'  | grep "$s" | cut -f 1 -d :`
    for t in $tests
    do
        echo "# -----------------------------------------------------"
        echo "# TEST: $s $t"
        echo "# "

        log_tgt="./rp.test_$s_$t.log"

        if test "$JENKINS_VERBOSE" = "TRUE"
        then
            progress='print'
        else
            progress='printf "."'
        fi

        ( set -e ; "./test_$s.py" "$t" ; echo "$SUCCESS_MARKER") 2>&1 \
        | tee "$log_tgt" | awk "{$progress}"

        if grep "$SUCCESS_MARKER" "$log_tgt"
        then
            echo
            echo "# "
            echo "# SUCCESS $s $t"
            echo "# -----------------------------------------------------"
        else
            echo
            echo "# "
            echo "# FAILED $s $t"
            echo "# -----------------------------------------------------"

            if ! test "$JENKINS_VERBOSE" = "TRUE"
            then
                cat "$log_tgt"
                echo "# -----------------------------------------------------"
            fi

            if test "$JENKINS_EXIT_ON_FAIL" = "TRUE"
            then
                exit 1
            fi
            failed=1
        fi
    done
done

issues=`cat jenkins_issues.cfg | sed -e 's/#.*//g' | grep -v '^ *$'`
for i in $issues
do
    echo "# -----------------------------------------------------"
    echo "# TEST ISSUE: $i"
    echo "# "

    log_tgt="./rp.test_issue_$i.log"

    if test "$JENKINS_VERBOSE" = "TRUE"
    then
        progress='print'
    else
        progress='printf "."'
    fi

    ( set -e ; "./$i" ; echo "$SUCCESS_MARKER") 2>&1 \
    | tee "$log_tgt" | awk "{$progress}"

    if grep "$SUCCESS_MARKER" "$log_tgt"
    then
        echo
        echo "# "
        echo "# SUCCESS $i"
        echo "# -----------------------------------------------------"
    else
        echo
        echo "# "
        echo "# FAILED $i"
        echo "# -----------------------------------------------------"

        if ! test "$JENKINS_VERBOSE" = "TRUE"
        then
            cat "$log_tgt"
            echo "# -----------------------------------------------------"
        fi

        if test "$JENKINS_EXIT_ON_FAIL" = "TRUE"
        then
            exit 1
        fi
        failed=1
    fi
done

exit $failed


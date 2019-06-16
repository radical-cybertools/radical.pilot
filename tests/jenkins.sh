#!/bin/sh

# JENKINS_VERBOSE 
#     TRUE : print logs and stdio while running the tests
#     else : print logs and stdio only on failure, after running the test
#
# JENKINS_EXIT_ON_FAIL:
#     TRUE : exit immediately when a test fails
#     else : run all tests, independent of success/failure (exit code reflects
#            failures though)

# make sure we exit cleanly
\trap shutdown QUIT TERM EXIT

export FAILED=0

export TEST_OK="JENKINS TEST SUCCESS"

export SAGA_LOG_LVL=DEBUG
export RADICAL_LOG_LVL=DEBUG
export RADICAL_UTILS_LOG_LVL=DEBUG
export RADICAL_PILOT_LOG_LVL=DEBUG

export FILE_TGT="../report"
export LINK_TGT="http://ci.radical-project.org/job/radical.pilot.devel/branch/devel/ws/radical.pilot/report/"

export HTML_TARGET="$FILE_TGT/test_results.html"
export HTML_SUCCESS="<font color=\"\#66AA66\">SUCCESS</font>"
export HTML_FAILURE="<font color=\"\#AA6666\">FAILED</font>"

# ------------------------------------------------------------------------------
#
html_start()
{
    (
        echo "<html>"
        echo " <body>"
        echo "  <table>"
        echo "   <tr>"
        echo "    <td> <b> Test    </b> </td> "
        echo "    <td> <b> Result  </b> </td> "
        echo "    <td> <b> Logfile </b> </td> "
        echo "    <td> <b> Stats   </b> </td> "
        echo "    <td> <b> Plot    </b> </td> "
        echo "   </tr>"
    ) > $HTML_TARGET
}


# ------------------------------------------------------------------------------
#
html_entry()
{
    name=$1
    result=$2

    (
        echo "   <tr>"
        echo "    <td> $name    </td> "
        echo "    <td> $result  </td> "
        if test -f "$FILE_TGT/$name.log"
        then
            echo "    <td> <a href=\"$LINK_TGT/$name.log\">log</a> </td> "
        else
            echo "    <td> - </td> "
        fi
        if test -f "$FILE_TGT/$name.txt"
        then
            echo "    <td> <a href=\"$LINK_TGT/$name.txt\">stat</a> </td> "
        else
            echo "    <td> - </td> "
        fi
        if test -f "$FILE_TGT/$name.png"
        then
            echo "    <td> <a href=\"$LINK_TGT/$name.png\">plot</a> </td> "
        else
            echo "    <td> - </td> "
        fi
        echo "   </tr>"
    ) >> $HTML_TARGET
}


# ------------------------------------------------------------------------------
#
html_stop()
{
    (
        echo "  </table>"
        echo " </body>"
        echo "</html>"
    ) >> $HTML_TARGET
}


# ------------------------------------------------------------------------------
#
run_test() {

    name="$1";  shift
    cmd="$*"

    echo "# ------------------------------------------------------------------------------"
    echo "# TEST $name: $cmd"
    echo "# "

    log="$FILE_TGT/$name.log"

    if ! test -z "$JENKINS_VERBOSE"
    then
        progress='print'
    else
        progress='printf "."'
    fi

    (set -e ; $cmd ; printf "\n$TEST_OK\n") 2>&1 | tee "$log" | awk "1==NR%80{print \"\"}{$progress}"
    echo

    SID=`grep 'SESSION ID' $log | head -n 1 | cut -f 2 -d ':' | tr -d ' '`
    if ! test -z "$SID"
    then
        radicalpilot-stats         -m  stat,plot -s $SID -t png >  $FILE_TGT/$name.txt 2>&1
        radicalpilot-close-session -m  purge     -s $SID        >> $FILE_TGT/$name.txt 2>&1
        test -f $SID.png && mv $SID.png $FILE_TGT/$name.png
    fi


    if grep -q "$TEST_OK" "$log"
    then
        html_entry "$name" "$HTML_SUCCESS"
        echo "# "
        echo "# SUCCESS $name"
        echo "# ------------------------------------------------------------------------------"
    else
        html_entry "$name" "$HTML_FAILURE"
        echo "# "
        echo "# FAILED $name"
        echo "# ------------------------------------------------------------------------------"

        FAILED=1
    fi


    if test "$JENKINS_EXIT_ON_FAIL" = "TRUE" && test "$FAILED" -eq 0
    then
        shutdown
    fi
}


# ------------------------------------------------------------------------------
#
startup()
{
    html_start
}


# ------------------------------------------------------------------------------
#
CLOSED=false
shutdown()
{
    if ! $CLOSED
    then
        html_stop
        exit $FAILED
        CLOSED=true
    fi
}


# ------------------------------------------------------------------------------
#
startup

for s in integration mpi
do
    tests=`cat jenkins.cfg | sed -e 's/#.*//g' | grep -v '^ *$'  | grep "$s" | cut -f 1 -d :`
    for t in $tests
    do
        run_test "test_${s}_$t" "./test_$s.py $t"
    done
done

issues=`cat jenkins_issues.cfg | sed -e 's/#.*//g' | grep -v '^ *$'`
for i in $issues
do
    run_test "issue_$i" "./$i"
done

shutdown
#
# ------------------------------------------------------------------------------


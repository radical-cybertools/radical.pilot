
tests=`cat jenkins.cfg | sed -e 's/#.*//g' | grep -v '^ *$'  | grep integration | cut -f 1 -d :`

for t in $tests
do
    echo "# -----------------------------------------------------"
    echo "# integration test for $t"
    echo "# "
    log_tgt="./rp.test_mpi.$t.log"
    out_tgt="./rp.test_mpi.$t.out"
    export RADICAL_LOG_TARGETS="stdout,$log_tgt"
    export RADICAL_UTILS_LOG_TARGETS="stdout,$log_tgt"
    export SAGA_LOG_TARGETS="stdout,$log_tgt"
    export RADICAL_PILOT_LOG_TARGETS="stdout,$log_tgt"
    ./test_integration.py "$t" 2>&1 | tee "$out_tgt" | awk '{ printf "." }'
    if test "$?" = 0
    then
        echo
        echo "# "
        echo "# SUCCESS"
        echo "# -----------------------------------------------------"
        if test "$JENKINS_VERBOSE" = "TRUE"
        then
            cat "$log_tgt"
            echo "# -----------------------------------------------------"
            cat "$out_tgt"
            echo "# -----------------------------------------------------"
        fi
    else
        echo
        echo "# "
        echo "# FAILED"
        echo "# -----------------------------------------------------"
        cat "$log_tgt"
        echo "# -----------------------------------------------------"
        cat "$out_tgt"
        echo "# -----------------------------------------------------"
    fi
done


tests=`cat jenkins.cfg | sed -e 's/#.*//g' | grep -v '^ *$'  | grep mpi | cut -f 1 -d :`
for t in $tests
do
    echo "# -----------------------------------------------------"
    echo "# mpi test for $t"
    echo "# "
    log_tgt="./rp.test_mpi.$t.log"
    out_tgt="./rp.test_mpi.$t.out"
    export RADICAL_LOG_TARGETS="stdout,$log_tgt"
    export RADICAL_UTILS_LOG_TARGETS="stdout,$log_tgt"
    export SAGA_LOG_TARGETS="stdout,$log_tgt"
    export RADICAL_PILOT_LOG_TARGETS="stdout,$log_tgt"
    ./test_mpi.py "$t" 2>&1 | tee "$out_tgt" | awk '{ printf "." }'
    if test "$?" = 0
    then
        echo
        echo "# "
        echo "# SUCCESS"
        echo "# -----------------------------------------------------"
    else
        echo
        echo "# "
        echo "# FAILED"
        echo "# -----------------------------------------------------"
        cat "$log_tgt"
        echo "# -----------------------------------------------------"
        cat "$out_tgt"
        echo "# -----------------------------------------------------"
    fi
done



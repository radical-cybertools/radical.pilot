#!/bin/sh

check="$1"; shift
args="$*"

test "$check" = "flake8" && check="flake8 --statistics --format=pylint"

test -z "$args" && args='src'

case "$args" in

    diff)
        old=$( git log -n 1 | grep Merge | cut -f 2 -d ' ')
        new=$( git log -n 1 | grep Merge | cut -f 3 -d ' ')
        args=$(git diff --name-only --diff-filter=b $old...$new | grep -e '\.py$')
        ;;

    *)
        args=$(find $args -name \*.py)
        ;;
esac

err=0
for f in $args
do
    # skip archived files and __init__.py
    f=$(echo $f | grep -v -e 'archive/' -e '__init__.py')
    test -z "$f" && continue

    # run check and grep for relevant output
    res=`$check $f \
              | grep -v -e '^[-*]' -e ' rated at ' -e '^$' \
              | sed -e "s|^$f:|  |" \
              | column -t -s ':'`

    # dump output if there is any (indicating an error)
    if ! test -z "$res"
    then
        echo $f
        echo "$res"
        echo
        err=1
    fi
done

# fail if any check run failed
exit $err



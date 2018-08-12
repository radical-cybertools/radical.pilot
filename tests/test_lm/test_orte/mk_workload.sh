#!/bin/bash

base=`pwd`
nfile=$1

if test -z "$nfile"
then
    nfile=$base/nodes
fi

tmp=$base/unit.tmp
ntmp=$base/nodes.tmp
nmax=0
cpn=


n_w=4
n_u=128


rm -rf $base/workload/
rm -rf $ntmp

nids=''
for spec in $(cat $nfile)
do
    nid=$(   echo $spec | cut -f 1 -d :)
    ncores=$(echo $spec | cut -f 2 -d :)

    # we expect an uniform cluster
    if test -z "$cpn"
    then
        cpn=$ncores
    else
        if ! test "$cpn" = "$ncores"
        then
            echo "inconsistent cpn ($cpn - $spec)"
            exit
        fi
    fi

    c=0
    while test $c -lt $ncores
    do
        echo "$nid" >> $ntmp
        c=$((c+1))
        nmax=$((nmax+1))
    done
done

w=0
while test $w -lt $n_w
do
    wid=$(printf "wl.%04d" $w)
    echo -n "$wid: "
    w=$((w+1))
    mkdir -p "$base/workload/$wid"

    nidx=0

    u=0
    while test $u -lt $n_u
    do
        uid=$(printf "unit.%06d" $u)
      # echo -n "$uid "
        u=$((u+1))

        t=$((RANDOM % 10))               # random sleep between 1 an 10
        c=$(((RANDOM % (cpn * 2)) + 1))  # random number of cores, up to 2 nodes
        r=0

        n=''
        for node in $(head -n $((nidx + c)) $ntmp | tail -n $c)
        do
            n="$n,$node"
        done
        nidx=$((nidx+c))
        x=$nidx

        if test $nidx -gt $nmax
        then
            echo -n '-'
            continue
        fi

        echo -n '.'

        n=$(echo $n | sed -e 's/^,//g')

        cat "$tmp" \
            | sed -e "s/#T#/$t/g" \
            | sed -e "s/#C#/$c/g" \
            | sed -e "s/#N#/$n/g" \
            | sed -e "s/#R#/$r/g" \
            | sed -e "s/#X#/$x/g" \
            > "$base/workload/$wid/$uid"

    done
    echo
done



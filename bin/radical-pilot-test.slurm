#!/bin/sh

#SBATCH --nodes=1
#SBATCH --account   "###ACCOUNT###"
#SBATCH --partition "###PARTITION###"
#SBATCH --time 00:05:00

###VE_ACTIVATE###

runtime=30
noise=5

start=$(radical-gtod | cut -f 1 -d '.')
srun /bin/sleep $runtime &
srun /bin/sleep $runtime &
wait
stop=$(radical-gtod | cut -f 1 -d '.')

duration=$((stop - start))

if test $duration -gt $((2 * runtime - noise))
then
    echo "duration too long, no concurrency"
    exit 1
else
    echo "duration consistent with concurrent execution"
    exit 0
fi


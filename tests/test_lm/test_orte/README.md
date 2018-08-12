
OpenMPI's 'Open RunTime Environment' (ORTE) is unit launch method which is used
on many of RP's target systens.  RP relies, however, on some ORTE features which
are not yet released in OpenMPI, and as such need to be considered unstable.
And indeed, we frequently encounter IRTE instabilities, in particular at scale.

This test suite is supposed to stress test ORTE in that context, to confirm the
viability of an ORTE deployment.  

`test_orte.sh` with start an ORTE DVM (distributed virtual machines) on all
available nodes, and will then run a large number of tasks on that DVM.  The
script expects an OMPI installation to be available in `$PATH` - you can create
one with `../../../bin/radical-pilot-deploy-ompi $(pwd)`, and then add the
resulting installation under `./ompi/installed/.../bin` to `$PATH`.

The test workloads to be executed are under `workloads/`, they will run one
after the other.  All units in each workload subdir are executed concurrently.
A unit file is structured like this:

```
    command : /bin/false
    slots   : node_1:4,node_2:2
    retval  : 0
```

The `slots` field will let orterun to place the respective command processes
into the respective nodes and number of cores - in the example above, it will
place 4 processes on `node_1`, and 2 processes on `node_2`.  It is up to the
executor of the test script to ensure that a sufficient number of nodes is
available.  The test will replace the node name placeholders with actual node
names.

The retval is the expected return value of the unit, and will be checked for.


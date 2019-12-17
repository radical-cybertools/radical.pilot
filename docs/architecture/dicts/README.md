
Description Performance in RP
-----------------------------

This code contains a simple performance comparison between different potential
implementations of `Description` types used in RP.  The current implementation
is based on SAGA attributes which is very slow: it seems not viable to scale to
millions of tasks.  We compare the following approaches:

  - `dict` : plain Python dictionary
  - `tdict`: typed dictionaries (backported to 3.6 via mypy)
  - `rucfg`: ru.Config instances (using plain `dict`s as backend)
  - `saga` : current implementation

The following performance metrics are considered:

  - `create`: create `n` description instances
  - `fill`  : fill all attributes in those instances (correctly typed)
  - `change`: change on attribute in those instances
  - `copy`  : deep-copy all those instances
  - `size`  : memory allocated by all instances

When including the current implementation, we run with `n = 8 * 1024` and obtain
the following results:

```
dict  ##########################################################################
      create    :       0.006 sec
      fill      :       0.070 sec
      change    :       0.003 sec
      copy      :       0.469 sec
      size      :      61.785 MB
tdict ##########################################################################
      create    :       0.008 sec
      fill      :       0.090 sec
      change    :       0.003 sec
      copy      :       0.471 sec
      size      :      61.785 MB
rucfg ##########################################################################
      create    :       0.070 sec
      fill      :       0.058 sec
      change    :       0.004 sec
      copy      :       0.870 sec
      size      :      62.253 MB
saga  ##########################################################################
      create    :       4.207 sec
      fill      :       2.522 sec
      change    :       0.083 sec
      copy      :       7.903 sec
      size      :     580.605 MB
```

`copy` dominated all experiments, but it is difficult to distinguish the first
three in more detail.  SAGA attributes behave slow, as expected.  They also
consume the ~10-fold amount of memory.

When excluding SAGA attributes we can run with more entities (`n = 1024 * 1024`)
and obtain:

```
dict  #######################################################################
      create    :       0.710 sec
      fill      :      11.406 sec
      change    :       0.347 sec
      copy      :      60.963 sec
      size      :    7908.295 MB
tdict #######################################################################
      create    :       0.943 sec
      fill      :      11.327 sec
      change    :       0.361 sec
      copy      :      57.499 sec
      size      :    7908.295 MB
rucfg #######################################################################
      create    :       7.759 sec
      fill      :      11.050 sec
      change    :       0.375 sec
      copy      :     105.571 sec
      size      :    7968.295 MB
```

The similarities between all three implementations becomes more apparent: they
are likely all dominated by the underlying `dict` implementation.  The
additional recursive object hierarchy of the `ru.Config` implementation is
adding some overhead on the deep copy test - but not as much as to make it
concerning (deep copy is a rare operation in RP).

Conclusion:
-----------

Any replacement of SAGA attributes is a good replacement (with respect to
performance).  I would suggest to use typed dictionaries under our `ru.Config`
implementation to make typed descriptions and config files somewhat uniform and
to add the benefits of type safety to our configuration system.



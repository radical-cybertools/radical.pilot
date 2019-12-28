
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
  -
  - `CUD`  : `ComputeUnitDescription` (current SAGA based implementation)
  - `GOD`  : pypi `good` module (schema validator
  - `SCH`  : pypi `schema` module (schema validator)
  - `DICT` : plain python dictionary
  - `TDD`  : typed dictionary (via mypy)
  - `CFG`  : `radical.utils.Config` (munched dictionary)
  - `RUD`  : `radicals.utils.DictMixin` dict implementation
  - `PYD`  : `pydantic` module (type anotator)

The following performance metrics are considered:

  - `create`: create `n` description instances
  - `fill`  : fill all attributes in those instances (correctly typed)
  - `change`: change on attribute in those instances
  - `copy`  : deep-copy all those instances
  - `check` : trigger a type error on value setting on each entity
  - `size`  : memory allocated by all instances

When runnint with `n = 8 * 1024`, we obtain the following results:

```
CUD   : #############|#############|##############|#############|###############
      create    :       4.258 sec
      fill      :       2.318 sec
      change    :       0.085 sec
      copy      :       7.044 sec
      check     :       0.690 sec [8192]
      total     :      14.395 sec
      size      :     568.847 MB
GOD   : #############|#############|##############|#############|###############
      create    :       0.008 sec
      fill      :       0.214 sec
      change    :       0.008 sec
      copy      :       0.479 sec
      check     :      31.506 sec [8192]
      total     :      32.215 sec
      size      :     258.449 MB
SCH   : #############|#############|##############|#############|###############
      create    :       0.033 sec
      fill      :       0.052 sec
      change    :       0.010 sec
      copy      :       1.654 sec
      check     :       7.422 sec [8192]
      total     :       9.170 sec
      size      :      57.597 MB
dict  : #############|#############|##############|#############|###############
      create    :       0.006 sec
      fill      :       0.116 sec
      change    :       0.007 sec
      copy      :       0.496 sec
      check     :       0.010 sec [0]
      total     :       0.635 sec
      size      :      57.472 MB
TDD   : #############|#############|##############|#############|###############
      create    :       0.007 sec
      fill      :       0.068 sec
      change    :       0.007 sec
      copy      :       0.464 sec
      check     :       0.010 sec [0]
      total     :       0.557 sec
      size      :      57.472 MB
CFG   : #############|#############|##############|#############|###############
      create    :       0.062 sec
      fill      :       0.075 sec
      change    :       0.008 sec
      copy      :       0.787 sec
      check     :       0.008 sec [0]
      total     :       0.940 sec
      size      :      57.941 MB
RUD   : #############|#############|##############|#############|###############
      create    :       0.033 sec
      fill      :       0.098 sec
      change    :       0.009 sec
      copy      :       0.483 sec
      check     :       0.011 sec [0]
      total     :       0.634 sec
      size      :      59.332 MB
PYD   : #############|#############|##############|#############|###############
      create    :       0.217 sec
      fill      :       0.226 sec
      change    :       0.014 sec
      copy      :       0.644 sec
      check     :       0.014 sec [0]
      total     :       1.115 sec
      size      :      58.035 MB
```

Based on these data, two classes of implementations can be established: those
which do proper runtime type checking (and detect all of errors in the `check`
entry), and those which don't (only static type checking during a linter run
supported).  For the type checking ones, the type check itself usually dominates
(for SAGA, the type check is done during construction / filling).  `copy`
dominates all implementations which do not perform runtime type checking.

Note that while the current implementation is average on performance (for proper
type checking), it consumes the most memory.  `GOD` is slowest and also consumes
a rather large amount of memory.

When excluding runtime type checkers, we can run with more entities and obtain
for `n = 1024 * 1024`:

```
dict  : #############|#############|##############|#############|###############
      create    :       0.658 sec
      fill      :      10.553 sec
      change    :       0.780 sec
      copy      :      53.451 sec
      check     :       0.998 sec [0]
      total     :      66.439 sec
      size      :    7356.295 MB
TDD   : #############|#############|##############|#############|###############
      create    :       0.805 sec
      fill      :       9.876 sec
      change    :       0.853 sec
      copy      :      53.415 sec
      check     :       0.985 sec [0]
      total     :      65.934 sec
      size      :    7356.295 MB
CFG   : #############|#############|##############|#############|###############
      create    :       8.941 sec
      fill      :       8.279 sec
      change    :       0.775 sec
      copy      :     102.456 sec
      check     :       0.727 sec [0]
      total     :     121.178 sec
      size      :    7416.295 MB
RUD   : #############|#############|##############|#############|###############
      create    :       1.050 sec
      fill      :      11.606 sec
      change    :       1.076 sec
      copy      :      67.801 sec
      check     :       1.147 sec [0]
      total     :      82.680 sec
      size      :    7594.295 MB
PYD   : #############|#############|##############|#############|###############
      create    :      25.558 sec
      fill      :      29.719 sec
      change    :       1.645 sec
      copy      :      85.967 sec
      check     :       1.794 sec [0]
      total     :     144.683 sec
      size      :    7428.295 MB
```

The similarities between these implementations becomes more apparent: they
are likely all dominated by the underlying `dict` implementation.  The
additional recursive object hierarchy of the `ru.Config` implementation is
adding some overhead on the deep copy test - but not as much as to make it
concerning (deep copy is a rare operation in RP).

Conclusion:
-----------

Almost any replacement of SAGA attributes is a good replacement (with respect to
performance and memory consumption) - but proper type checking is costly, no
matter what we chose (we could still look into implementing our own limited and
optimized type checker).  I would suggest to use plain dictionaries under our
`ru.Config` implementation, and to *optionally* add type checking via `schema`
(the fastest runtime checker).  This would improve performance and memory
consumption, preserve a uniform API to dict-like data, and add the benefits of
type safety to the RCT configuration system.



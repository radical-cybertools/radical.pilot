
Description Performance in RP
-----------------------------

This code contains a simple performance comparison between different potential
implementations of `Description` types used in RP.  The current implementation
is based on SAGA attributes which is very slow: it seems not viable to scale to
millions of tasks.  We compare the following approaches:

  - `PYDICT`  : python dictionary
  - `MYDICT`  : typed python dictionary (via mypy)
  - `GOOD`    : `good` module (schema validator)
  - `SCHEMA`  : `schema` module (schema validator)
  - `PYDANTIC`: `pydantic` module (type annotator)
  - `NEW_CUD` : `radical.pilot.TaskDescription` (new implementation)
  - `RU_CFG`  : `radical.utils.Config` (munched dictionary)
  - `RU_DICT` : `radical.utils.DictMixin` (RU dict implementation)
  - `RU_MUNCH`: `radical.utils.Munch` (munched dict with typed schema)
  - `OLD_CUD` : `radical.saga.attributes` (current SAGA based implementation)

The following performance metrics are considered:

  - `create`  : create `n` description instances
  - `fill`    : fill all attributes in those instances (correctly typed)
  - `change`  : change one attribute in those instances
  - `copy`    : deep-copy all those instances
  - `check`   : trigger a type error on value setting on each entity
  - `size`    : memory allocated by all instances

When running with `n = 1024 * 8`, we obtain the following results (sorted by
`total`):

```
OLD_CUD : #############|#############|##############|#############|#############
GOOD    : #############|#############|##############|#############|#############
SCHEMA  : #############|#############|##############|#############|#############
RU_MUNCH: #############|#############|##############|#############|#############
PYDICT  : #############|#############|##############|#############|#############
MYDICT  : #############|#############|##############|#############|#############
RU_CFG  : #############|#############|##############|#############|#############
NEW_CUD : #############|#############|##############|#############|#############
RU_DICT : #############|#############|##############|#############|#############
PYDANTIC: #############|#############|##############|#############|#############
+----------+--------+--------+--------+--------+-------+-------+--------+------+
| name     | create |   fill | change |   copy | check | found |  total | size |
|          |  [sec] |  [sec] |  [sec] |  [sec] | [sec] |   [n] |  [sec] | [MB] |
+----------+--------+--------+--------+--------+-------+-------+--------+------+
| RU_MUNCH |   0.05 |   0.13 |   0.01 |   0.18 |  0.02 |  8192 |  0.38  |   59 |
| NEW_CUD  |   0.09 |   0.10 |   0.01 |   0.18 |  0.01 |  8192 |  0.40  |   59 |
| PYDICT   |   0.01 |   0.08 |   0.01 |   0.50 |  0.01 |     0 |  0.61  |   57 |
| MYDICT   |   0.01 |   0.08 |   0.01 |   0.54 |  0.01 |     0 |  0.65  |   57 |
| RU_DICT  |   0.01 |   0.12 |   0.01 |   0.65 |  0.01 |     0 |  0.80  |   59 |
| RU_CFG   |   0.06 |   0.09 |   0.01 |   0.87 |  0.01 |     0 |  1.04  |   57 |
| PYDANTIC |   0.23 |   0.22 |   0.02 |   0.83 |  0.01 |     0 |  1.31  |   58 |
| SCHEMA   |   0.05 |   0.06 |   0.01 |   1.88 |  8.66 |  8192 |  10.66 |   57 |
| OLD_CUD  |   4.44 |   2.62 |   0.09 |   8.41 |  0.91 |  8192 |  16.47 |  568 |
| GOOD     |   0.01 |   0.07 |   0.01 |   0.53 | 37.30 |  8192 |  37.92 |  258 |
+----------+--------+--------+--------+--------+-------+-------+--------+------+
```

Based on these data, two classes of implementations can be distinguished: those
which do proper runtime type checking (and find all 8192 errors in the `check`
column), and those which don't (only static type checking during a linter run
supported).  For the type checking ones, the type check itself usually
dominates (for SAGA, the type check is done during construction / filling).
`copy` dominates all implementations which do not perform runtime type
checking.

Note that while the current implementation is average on performance (for
proper type checking), it consumes the most memory.  `GOOD` is slowest and also
consumes a rather large amount of memory.

When excluding slow runtime type checkers (`SCHEMA`, `OLD_CUD`, `GOOD`), we can
run with more entities and obtain for `n = 1024 * 1024`:

```
RU_MUNCH: #############|#############|##############|#############|#############
PYDICT  : #############|#############|##############|#############|#############
MYDICT  : #############|#############|##############|#############|#############
RU_CFG  : #############|#############|##############|#############|#############
NEW_CUD : #############|#############|##############|#############|#############
RU_DICT : #############|#############|##############|#############|#############
PYDANTIC: #############|#############|##############|#############|#############
+----------+--------+-------+--------+-------+-------+---------+--------+------+
| name     | create |  fill | change |  copy | check |   found |  total | size |
|          |  [sec] | [sec] |  [sec] | [sec] | [sec] |     [n] |  [sec] | [MB] |
+----------+--------+-------+--------+-------+-------+---------+--------+------+
| RU_MUNCH |   1.68 | 13.64 |   0.93 | 18.10 |  1.59 | 1048576 |  35.94 | 7594 |
| NEW_CUD  |   8.99 | 13.21 |   1.00 | 18.52 |  1.68 | 1048576 |  43.30 | 7594 |
| MYDICT   |   0.75 |  8.57 |   0.81 | 54.44 |  1.04 |       0 |  65.62 | 7356 |
| PYDICT   |   0.67 | 14.90 |   0.88 | 57.08 |  1.07 |       0 |  74.59 | 7372 |
| RU_DICT  |   3.55 | 14.97 |   0.96 | 62.44 |  1.19 |       0 |  83.11 | 7594 |
| RU_CFG   |   8.73 |  8.54 |   0.84 | 99.61 |  0.75 |       0 | 118.46 | 7416 |
| PYDANTIC |  26.00 | 28.64 |   1.67 | 81.55 |  1.53 |       0 | 139.39 | 7428 |
+----------+--------+-------+--------+-------+-------+---------+--------+------+
```

The similarities between these implementations becomes more apparent: they
are likely all dominated by the underlying `dict` implementation.
      
      
Conclusion:
-----------

Almost any replacement of SAGA attributes is a good replacement (with respect
to performance and memory consumption).  All available proper type checking
modules either provide only static type checks or are very costly, no matter
what we chose.  We thus implement our own limited and optimized type checker
(RU_MUNCH, schema-based).  (the fastest runtime checker).  This improves
performance and memory consumption, preserve a uniform API to dict-like data,
and can potentially add the benefits of type safety to the RCT configuration
system.


RU now implements a type checking `Description` class (`NEW_CUD`).  The
schema based type checking is limited and very forgiving (types are converted
if possible).  That implementation is the fastest type checking one by a factor
of 10 while preserving performance and memory consumption close to the lean
DictMixin implementations (`RU_CFG`, `RU_DICT`).  The penalty of `NEW_CUD` vs.
`RU_MUNCH` comes from the application of default values for the RP description
types which requires a dictionary merge.

The `NEW_CUD` / `RU_MUNCH` implementations
  - have proper (if limited) type checking
  - have fastest deep_copy by a large margin (even compared to `PYDICT`)
  - have very small overheads compared to native Python dicts (`PYDICT`).



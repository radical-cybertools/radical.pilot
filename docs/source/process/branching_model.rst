.. _branching_model:

Branching Model
===============

RADICAL-Pilot (RP) uses `git-flow
<http://nvie.com/posts/a-successful-git-branching-model/>`__ as branching model,
with some simplifications. We follow `semantic version numbering
<http://semver.org/>`__.

-  Release candidates and releases are tagged in the ``master`` branch (we do
   not use dedicated release branches at this point).

-  A release is prepared by:

   -  Tagging a release candidate on ``devel`` (e.g. ``v1.23RC4``);
   -  testing that RC;
   -  problems are fixed in ``devel``, toward a new release candidate;
   -  once the RC is found stable, ``devel`` is merged to master, the release is
      tagged on master (e.g. ``v1.23``) and shipped to PyPI.

-  Urgent hotfix releases:

   -  Branch from master to ``hotfix/problem_name``;
   -  fix the problem in that branch;
   -  test that branch;
   -  merge back to master and prepare release candidate for hotfix release.

-  Normal bug fixes:

   -  Branch of ``devel``, naming convention: ``fix/issue_1234`` (reference
      GitHub issue);
   -  fix in that branch, and test;
   -  create pull request toward ``devel``;
   -  code review, then merge.

-  Major development activities go into feature branches:

   -  Branch ``devel`` into ``feature/feature_name``;
   -  work on that feature branch;
   -  on completion, merge ``devel`` into the feature branch (that should be
      done frequently if possible, to avoid large deviation (== pain) of the
      branches);
   -  test the feature branch;
   -  create a pull request for merging the feature branch into ``devel`` (that
      should be a fast-forward now);
   -  merging of feature branches into ``devel`` should be discussed with the
      group *before* they are performed, and only after code review.

-  Documentation changes are handled like fix or feature branches, depending on
   size and impact, similar to code changes.

Branch Naming
-------------

-  ``devel``, ``master``: *never* commit directly to those;
-  ``feature/abc``: development of new features;
-  ``fix/abc_123``: referring to ticket 123;
-  ``hotfix/abc_123``: referring to ticket 123, to be released right after merge
   to master;
-  ``experiment/sc16``: experiments toward a specific publication etc. Cannot be
   merged, they will be converted to tags after experiments conclude;
-  ``project/xyz``: branch for a dedicated group of people, usually contains
   unreleased features/fixes, and is not expected to be merged back;
-  ``tmp/abc``: temporary branch, will be deleted soon;
-  ``test/abc``: test some integration, like a merge of two feature branches.

For the latter: assume you want to test how ``feature/a`` works in combination
with ``feature/b``, then:

-  ``git checkout feature/a``;
-  ``git checkout -b test/a_b``;
-  ``git merge feature/b``;
-  do tests.

Branching Policies
------------------

All branches are ideally short living. To support this, only a limited number of
branches should be open at any point in time. Like, only ``N`` branches for
fixes and ``M << N`` branches for features should be open for each developer -
other features / issues have to wait.

Some additional rules
---------------------

-  Commits, in particular for bug fixes, should be self-contained so make it
   easy to use ``git cherry-pick``, so that bug fixes can quickly be transferred
   to other branches (such as hotfixes).
-  Do not use ``git rebase``, unless you *really* know what you are doing.
-  You may not want to use the tools available for ``git-flow`` -- those have
   given us inconsistent results in the past, partially because they used
   rebase.

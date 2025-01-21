.. _release_procedure:

Release Procedure
=================

**Release Manager:** Andre Merzky

Preconditions for release
-------------------------

1. If release is a milestone release: no open tickets for milestone;
2. all tests on ``devel`` `pass <https://github.com/radical-cybertools/radical.pilot/actions>`__.

Preparing a regular Release
---------------------------

.. code:: shell

    git prep
    git release
    git bump minor

`git prep` is a git script for the following steps:

.. code:: shell

    git co master              --> checkout master
    git pa                     --> pull all branches form remote
    git gone -a                --> remove all stale branches
    git merge devel            --> merge the release candidate
    git change >> CHANGES.md   --> draft changelog from new commits in master
    gvim -o VERSION CHANGES.md --> set new version if needed, make CHANGELOG human readable

After that that last manual intervention, the actual release itself with `git release` runs:

.. code:: shell

    git ci -am 'version bump'       --> commit VERSION and CHANGELOG changes
    git tag \"v$(cat VERSION)\"     --> tag the release
    git push                        --> push master to origin
    git push --tags                 --> push the release tag
    make upload                     --> push to pypi
    git co devel                    --> checkout devel
    git merge master                --> merge the release into devel
    git bump minor                  --> bump minor version
    git ci -am 'devel version bump' --> commit minor version bump on devel
    git dsync -a                    --> sync all branches to be in sync with devel

That last step is hard to automate as it involves resolving conflicts in all branches. But it is also important as it saved us over the last years from branches running out of sync with devel. `git-dsync` is a custom script which, essentially, does the following (pseudo-code)

.. code:: shell

    git checkout devel
    git pull
    for each branch in $(git branch -a)
    do
        git checkout branch
        git pull
        git merge devel
        git push
    done


Preparing a hotfix release
--------------------------

1. Create branch from latest master: e.g. ``git checkout master; git pull; git
   checkout -b hotfix/issue_123``;
2. update version ``echo "0.1.2" > VERSION``;
3. make modifications to branch: either by ``$EDITOR`` or ``git cherry-pick
   abcsuperdupercommit890`` (The latter is preferred);
4. update release notes: ``$EDITOR CHANGES.md``;
5. commit and push: ``git commit -a; git push``;
6. create `pull-request <https://github.com/radical-cybertools/radical.pilot/pulls>`__ of hotfix
    branch to master;
7. wait on and/or nudge other developer to review and test;
8. if not approved, ``GOTO 3``.

Perform a Release
-----------------

1. If approved, move to master branch and pull in merged content: ``git checkout
   master``, then ``git pull``;
2. create tag: ``git tag -a v0.1.2 -m "release v0.1.2.3"``;
3. push tag to github: ``git push --tags``;
4. release on PyPI: ``python setup.py sdist; twine upload --skip-existing
   dist/radical.xyz-0.1.2.tar.gz``;
5. verify PyPI version on ``https://pypi.python.org/pypi/radical.xyz``;
6. ``GOTO "Post Release"``.

Post Release
------------

1. Merge master into devel branch: ``git checkout devel; git merge master; git
   push``;
2. merge ``devel`` into all open development branches: ``for b in $branches; do
   git checkout $b; git merge devel; done``.

Testing twine and PyPI release
------------------------------

1. Register at `PyPI <https://test.pypi.org/>`__;
2. create the test release: ``python setup.py sdist``;
3. Upload your test release to ``test.pypi``: ``twine upload -r testpypi
   --skip-existing dist/radical.xyz-0.1.2.tar.gz``;
4. Check/test your release. More information at `Using test PyPI
   <https://packaging.python.org/guides/using-testpypi/>`__.

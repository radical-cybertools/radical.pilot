.. _release_procedure:

Release Procedure
=================

-  **Release Manager:** Andre Merzky

Preconditions for release
-------------------------

1. If release is a milestone release: no open tickets for milestone;
2. all tests on ``devel`` `pass <https://github.com/radical-cybertools/radical.pilot/actions>`__.

Preparing a regular Release
---------------------------

1.  Pull ``devel``: ``git checkout devel; git pull``;
2.  create branch from latest master: e.g. ``git checkout master; git pull; git
    checkout -b release/0.1.2``;
3.  update version: ``echo "0.1.2" > VERSION``;
4.  make modifications to branch: usually by merging devel ``git merge devel``
    (make sure to pull ``devel`` before);
5.  update version dependencies to radical stack in setup.py;
6.  update release notes: ``$EDITOR CHANGES.md``;
7.  commit and push: ``git commit -a; git push`` (make sure there are no
    unwanted files in the repo);
8.  create `pull-request
    <https://github.com/radical-cybertools/radical.pilot/pulls>`__ of release
    branch **to master**;
9.  wait on and/or nudge other developer to review and test;
10. if not approved, ``GOTO Perform a Release``.

Preparing a hotfix release
-------------------------

1. Create branch from latest master: e.g. ``git checkout master; git pull; git
   checkout -b hotfix/issue_123``;
2. update version ``echo "0.1.2" > VERSION``;
3. make modifications to branch: either by ``$EDITOR`` or ``git cherry-pick
   abcsuperdupercommit890`` (The latter is preferred);
4. update release notes: ``$EDITOR CHANGES.md``;
5. commit and push: ``git commit -a; git push``;
6. create `pull-request
    <https://github.com/radical-cybertools/radical.pilot/pulls>`__ of hotfix
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
   git checkout $b; git merge master; done``.

Testing twine and PyPI release
------------------------------

1. Register at `PyPI <https://test.pypi.org/>`__;
2. create the test release: ``python setup.py sdist``;
3. Upload your test release to ``test.pypi``: ``twine upload -r testpypi
   --skip-existing dist/radical.xyz-0.1.2.tar.gz``;
4. Check/test your release. More information at `Using test PyPI
   <https://packaging.python.org/guides/using-testpypi/>`__.

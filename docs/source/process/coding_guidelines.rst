.. _coding_guidelines:

Coding Guidelines
=================

We use `PEP.8 <https://www.python.org/dev/peps/pep-0008/>`_ as base of our
coding guidelines, with some additions and changes. Those changes are mostly due
to the personal preferences of the current developers of RADICAL-Pilot (RP).
Such preferences can be discussed, revised and agreed upon during RP development
meetings. Deviations from PEP.8 pertains to:

 * Enabling visual alignment of code;
 * preferring ``'`` over ``"`` (but use ``"`` when that avoids escapes);
 * preferring ``%``-based string expansion over ``string.format``;
 * allowing short names for loop variables.

In all other regards, we expect PEP.8 compliance.

Example:

.. code:: python

    #!/usr/bin/env python3

    import sys
    import time
    import pprint

    import radical.utils as ru
    import radical.saga  as rs
    import radical.pilot as rp


    # ------------------------------------------------------------------------------
    # constants
    # (we use delimiting lines like above to separate code sections, classes, etc.)
    #
    STATE_ONE   = 'one'
    STATE_TWO   = 'two'
    STATE_THREE = 'three'


    # ------------------------------------------------------------------------------
    #
    # this class is nothing special, but its mine.
    #
    class MyClass(object):
        '''
        This method demonstrates what deviations from PEP.8 are accepted in RP.
        '''

        # --------------------------------------------------------------------------
        #
        def __init__(self):

            self._state   = STATE_ONE
            self._created = time.time()
            self._worker  = ru.Thread()
            self._config  = {'title'         : 'hello darkness',
                             'subtitle'      : 'my old friend',
                             'super_category': 'example'}


        # --------------------------------------------------------------------------
        #
        def method(self, arg_1='default_1', arg_2='default_2',
                         arg_3='default_3', arg_4='default_4'):
            '''
            method documentation
            '''

            if arg_1 == 'default_1' and \
               arg_2 != 'default_2':
                print 'this is unexpectedly indented (%s: %s)' % (arg_1, arg_2)

            # we allow one letter names for temporary loop variables
            for i in range(4):
                print "It's a simply i [%d]" % i

            # strangely indendet clauses
            if   arg3 == arg4: print 'haleluja'               # because why not
            elif arg2 == arg4: print 'praise the space'       # loooots of space
            else             : print 'clickediclickediclick'  # clickedispace!

            pprint.some_formatting_method(mode='radical', align='fancy',
                                          enforce=False,  encourage=True)


    # ------------------------------------------------------------------------------

This is our ``flake8`` configuration, which should transfer to other pep8 linters:

::

    [flake8]
    exclude          = .git,__pycache__,docs,build,dist,setup.py
    max-complexity   = 10
    max-line-length  = 80
    per-file-ignores =
         # imported but unused, star-import, unmatched F*
         __init__.py: F401, F403, X100
    ignore           =
         # module level import location
         E402,
         # space before/after operator
         E221, E222,E251,
         # multiple spaces after/after keyword
         E271, E272,
         # line too long
         E501,
         # whitespace before/after ...
         E203, E202, E231, E241, E211, E116, E127,
         # same indent
         E129,
         # comment indent not mod(4)
         E114,
         # cont line indentation
         E128, E126, E124, E125, E131,
         # blank lines
         W391, E301, E303,
         # multiple statements on one line
         E701, E704,
         # space before bracket
         C0326,
         # trailing whitespace
         W291,
         # Complex methods
         C901,
         # Do not use bare 'except'
         E722,
         # allow line breaks after binary operators
         W504,
         # allow lambda assignment (used for partial callbacks)
         E731


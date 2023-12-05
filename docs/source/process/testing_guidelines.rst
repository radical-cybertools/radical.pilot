.. _testing_guidelines:

Testing Guidelines
==================

When coding unit tests, the following guidelines are in effect. This is based on
Python's ``unittest`` module (`docs
<https://docs.python.org/3/library/unittest.html>`__), information gathered by
`Python Guide docs <https://docs.python-guide.org/writing/tests/>`__, `Pytest
docs <https://docs.pytest.org/en/latest/unittest.html>`__, `online examples
<https://www.freecodecamp.org/news/an-introduction-to-testing-in-python/>`__ and
the personal preferences of the development team. Such preferences can be
discussed, revised and agreed upon during RP development meetings.

Tests organization
------------------

Under the ``/tests`` folder, for each module there is a folder named ``test_*``,
where ``*`` is the component under test. There are folders for unit and
integration tests. A unit test for a specific class contains in the filename the
name of the tested class after ``test_``. For example:

.. code:: text

   tests/
   │
   ├── test_scheduler/
   │   └── __init__.py
   |   |
   |   └── test_unit/
   |       └── __init__.py
   |       |
   |       └── test_base.py

Each unit test has a single class. This class is named after the component it
tests, for example ``TestContinuous`` for testing the continuous scheduler of
RP. In addition, the following guidelines are in place:

-  Unit tests should import the ``TestCase`` class from ``unittest``,
   for example:

   .. code:: python

      from unittest import TestCase

-  Unit tests should inherit from ``TestCase``:

   .. code:: python

      class SimpleTestClass(TestCase):

-  A single class method should be tested by each test class method. All others
   should be mocked like:

   .. code:: python

          # --------------------------------------------------------------------------
          #
          @mock.patch.object(Continuous, '__init__', return_value=None)
          @mock.patch.object(Continuous, '_configure', return_value=None)
          def test_find_resources(mocked_init, mocked_configure):

-  Each unit test can define a ``setUp`` method and utilize it to get its test
   cases from the ``test_cases`` folder. This method can be generalized between
   tests of the same component.

-  Each unit test is responsible to tear down the test and remove any logs,
   profiles or any other file created because of the test by defining a
   ``tearDown`` method.

-  Each unit test defines a ``main`` as follows:

   .. code:: python

      if __name__ == '__main__':

          tc = SimpleTestClass()
          tc.test_find_resources()

-  Unit tests methods should use assertion methods and not ``assert`` as defined
   in `TestCase documentation
   <https://docs.python.org/3/library/unittest.html#unittest.TestCase>`__.
   Accordingly, test cases organization is proposed to be as `following
   <https://github.com/radical-cybertools/radical.pilot/wiki/RP-testing:-proposal-on-structure-reorganization>`__.
   That proposal will be evolved, and the current guidelines will be extended
   accordingly.

Executing tests
---------------

We run unit tests using ``pytest`` from the repo's root folder, for example:

.. code:: bash

   pytest -vvv tests/

Testing methods with no ``return``
----------------------------------

Each method in RP's components does three things: returns a value, changes the
state of an object or executes a callback. Two of these cases do not run a
return or return always ``True``. When the method changes an object the new
values should be checked. When the method calls a callback the callback should
be mocked.

Object change
~~~~~~~~~~~~~

Some methods change the state of either an input object or the state of their
self. For example:

.. code:: python

   ...
   def foo(self, unit, value):
       unit['new_entry'] = some_value

In this case the test should assert for the new value, for example:

.. code:: python

   def test_foo(self):
       component.foo(unit, value)
       self.assertTrue(unit['new_entry'], value)

Similarly, when a method changes the state of a component that should be
asserted. For example:

.. code:: python

   def configure(self):
       self._attribute = something

The test should look like:

.. code:: python

   def test_configure(self):
       component.configure()

       self.assertTrue(component._attribute, something)

Mocking callbacks
~~~~~~~~~~~~~~~~~

RP uses callbacks to move information around components. As a result, several
methods are not returning specific values or objects. This in turn makes it
difficult to create a unit test for such methods.

The following code shows an example of how such methods can be mocked so that a
unit test can receive the necessary information

.. code:: python

       # --------------------------------------------------------------------------
       #
       @mock.patch.object(Default, '__init__',   return_value=None)
       @mock.patch('radical.utils.raise_on')
       def test_work(self, mocked_init, mocked_raise_on):

           global_things = []
           global_state = []

           # ------------------------------------------------------------------------------
           #
           def _advance_side_effect(things, state, publish, push):
               nonlocal global_things
               nonlocal global_state
               global_things.append(things)
               global_state.append(state)

           # ------------------------------------------------------------------------------
           #
           def _handle_unit_side_effect(unit, actionables):
               _advance_side_effect(unit, actionables, False, False)


           tests = self.setUp()
           component = Default(cfg=None, session=None)
           component._handle_unit = mock.MagicMock(side_effect=_handle_unit_side_effect)
           component.advance = mock.MagicMock(side_effect=_advance_side_effect)
           component._log = ru.Logger('dummy')

           for test in tests:
               global_things = []
               global_state = []
               component._work([test[0]])
               self.assertEqual(global_things, test[1][0])
               self.assertEqual(global_state, test[1][1])

The method under test (MUT) checks if a unit has staging input directives and is
pushed either to ``_handle_units`` or ``advance``. Finally, ``_handle_units``
call advance. It is important to ``mock`` both calls. For that reason there are
two local functions defined ``_advance_side_effect`` and
``_handle_unit_side_effect``. These functions are used as `side_effects
<https://docs.python.org/3/library/unittest.mock.html#unittest.mock.Mock.side_effect>`__
of `MagicMock
<https://docs.python.org/3/library/unittest.mock.html#unittest.mock.MagicMock>`__.
When these methods are called by the MUT, the code in our code will be executed.

We also want to be able to capture the input of the side effect. This is done by
``global_things`` and ``global_state`` variables. It is important that these two
variables are changed from the mock functions and keep the new values. This is
done by defining them as `nonlocal
<https://stackoverflow.com/questions/1261875/python-nonlocal-statement>`__.

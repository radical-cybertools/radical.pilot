# pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: radical.pilot.compute_unit_description
   :platform: Unix
   :synopsis: Implementation of the DataUnitDescription class.
"""

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes as attributes

# ------------------------------------------------------------------------------
# Attribute description keys
NAME                   = 'name'
SIZE                   = 'size'
FILES                  = 'files'
CLEANUP                = 'cleanup'
LIFETIME               = 'lifetime'
SELECTION              = 'selection'

# ------------------------------------------------------------------------------
#
class DataUnitDescription(attributes.Attributes):
    """
    A DataUnitDescription object describes the requirements and
    properties of a :class:`radical.pilot.DataUnit` and is passed as a parameter to
    :meth:`radical.pilot.UnitManager.submit_units` to instantiate a new
    DataUnit.

    .. note:: A DataUnitDescription **MUST** define at least :data:`files`.

    **Example**::

        # TODO

    .. data:: name

       (`Attribute`) The non-unique name label (`string`) [`optional`].

    .. data:: size

       (`Attribute`) The number of bytes estimated to be required. (`int`) [`optional`].

    .. data:: files

       (`Attribute`) a set of files associated with this data unit. (`list of
       strings`) [`mandatory`].

    .. data:: lifetime

       (`Attribute`) the time duration (in minutes) for which the data
       unit is expected to be available (`int`) [`optional`].

    .. data:: cleanup

       If cleanup is set to True, the pilot will delete the entire unit sandbox
       upon termination. (`bool`) [`optional`]

    """
    def __init__(self, from_dict=None):

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        # action description
        self._attributes_register(NAME,      None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SIZE,      None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(FILES,     None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(CLEANUP,   None, attributes.BOOL,   attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LIFETIME,  None, attributes.INT,    attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SELECTION, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

        # explicitly set attrib defaults so they get listed and included via as_dict()
        self.set_attribute (NAME,           None)
        self.set_attribute (SIZE,           0)
        self.set_attribute (FILES,          None)
        self.set_attribute (CLEANUP,        False)
        self.set_attribute (LIFETIME,       0)
        self.set_attribute (SELECTION,      None)

        # apply initialization dict
        if from_dict:
            self.from_dict(from_dict)


    #------------------------------------------------------------------------------
    #
    def __deepcopy__ (self, memo):

        other = DataUnitDescription()

        for key in self.list_attributes():
            other.set_attribute(key, self.get_attribute(key))

        return other

# ---------------------------------------------------------------------------------

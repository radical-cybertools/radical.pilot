# pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: radical.pilot.compute_unit_description
   :platform: Unix
   :synopsis: Implementation of the DataUnitDescription class.
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes as attributes

# ------------------------------------------------------------------------------
# Attribute description keys
NAME                   = 'name'
SIZE                   = 'size'
FILE_URLS              = 'file_urls'
CLEANUP                = 'cleanup'
LIFETIME               = 'lifetime'

# ------------------------------------------------------------------------------
#
class DataUnitDescription(attributes.Attributes) :
    """
    A DataUnitDescription object describes the requirements and 
    properties of a :class:`radical.pilot.DataUnit` and is passed as a parameter to
    :meth:`radical.pilot.UnitManager.submit_units` to instantiate a new 
    DataUnit.

    .. note:: A DataUnitDescription **MUST** define at least :data:`file_urls`.

    **Example**::

        # TODO 

    .. data:: name 

       (`Attribute`) The non-unique name label (`string`) [`optional`].

    .. data:: size 

       (`Attribute`) The number of bytes estimated to be required. (`int`) [`optional`].

    .. data:: file_urls

       (`Attribute`) a set of files associated with this data unit. (`list of
       strings`) [`mandatory`].

    .. data:: lifetime 

       (`Attribute`) the time duration (in minutes) for which the data
       unit is expected to be available (`int`) [`optional`].

    .. data:: cleanup

       If cleanup is set to True, the pilot will delete the entire unit sandbox
       upon termination. (`bool`) [`optional`]

    """
    def __init__(self):

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        # action description
        self._attributes_register(NAME     , None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SIZE     , None, attributes.INT   , attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(FILE_URLS, None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(CLEANUP  , None, attributes.BOOL  , attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LIFETIME , None, attributes.INT   , attributes.SCALAR, attributes.WRITEABLE)

        # explicitly set attrib defaults so they get listed and included via as_dict()
        self.set_attribute (NAME     , None)
        self.set_attribute (SIZE     , None)
        self.set_attribute (FILE_URLS, None)
        self.set_attribute (CLEANUP  , None)
        self.set_attribute (LIFETIME , False)


# ---------------------------------------------------------------------------------


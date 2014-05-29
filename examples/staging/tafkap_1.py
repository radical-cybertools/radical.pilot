import saga.attributes as attributes

# TODO
# - split, merge, etc.
# - signature descriptions
# - callbacks



# ------------------------------------------------------------------------------
#
class StorageResourceDescription(attributes.Attributes):
    """Class to describe and instantiate SRs

    Class members:
        - resource_url     # The URL of the endpoint for this resource
        - capacity         # Minimum size in bytes

    """
    def __init__(self, url=None, size=None):
        """
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register('resource_url', url, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register('capacity', size, attributes.INT, attributes.SCALAR, attributes.WRITEABLE)



# ------------------------------------------------------------------------------
#
class StorageResource(object):
    """Storage Resource (TSR)

    == StagingArea ?!?!

    This is a handle to location on a storage resource that will be put under the control of RP.

    Class members
    -------------
        - resource_url     # The URL of the endpoint for this resource
        - capacity         # Storage capacity
        - available        # Available storage space

    State Model
    -----------
    * Available
    The SR is ready.

    * Busy
    Data is currently being accessed or stored on this SR.

    * Canceled
    Cancellation of this SR has been requested.
    It can no longer be used for new actions.


    """
    def __init__(self, url):
        """
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def insert(self, desc):
        """ Make the content of the DUD available at the SR.

        Keyword argument(s)::

            name(type): description.

        Return::

            Returns a DU that references the contents of the file(s).
            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def allocate(self, du):
        """Associate and allocate space for an "empty" DU on this SR to be used as the output of a CU.

        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def remove(self, dus):
        """ Remove the content of the DU from this SR.
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def list(self):
        """List the DUs and their state on this SR
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def cancel(self):
        """Cancel this SR
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """

# ------------------------------------------------------------------------------
#
class DataUnitDescription(attributes.Attributes):
    """DataUnitDescription(DUD) is a description class that can be used to instantiate DUs.
    DUs are input and output of CUs.

    Class members
    -------------
        - name            # A non-unique label.
        - file_urls {     # Dict of logical and physical file names, e.g.:
            'file_name1.txt': [
                'sftp://.../file_name1.txt',
                'srm://grid/vo/random42.txt'
            ],
            'file_name2.txt': ['file://.../file_name2.txt']
        }
        - lifetime        # Needs to stay available for at least ...
        - cleanup         # Can be removed when cancelled
        - size            # Estimated size of DU (in bytes)

    """
    def __init__(self, name=None, url=None, lifetime=None, cleanup=True, size=None):
        """
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register('name', name, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register('file_urls', url, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register('lifetime', lifetime, attributes.INT, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register('cleanup', cleanup, attributes.BOOL, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register('size', size, attributes.INT, attributes.SCALAR, attributes.WRITEABLE)


# ------------------------------------------------------------------------------
#
class DataUnit(object):
    """DataUnit (DU)

       - A DU is a logical container for a set of files.
       - Access to a DU is atomic: it starts "empty" and is "filled" by either an "import" or by the output of a CU.
       - The total content of the DU can be stored on one or more StorageResource's

    Class Members
    -------------

    State Model
    -----------
    * New
    DU is created to be used as an output TFC but not yet assigned to a CU.

    * Pending
    Files are not yet available, but are scheduled for transfer, or transfer is in
    progress.

    * Available
    All files are available on at least one SR.

    * Canceled
    The data in this DU is scheduled for removal and cannot be used anymore.

    * Failed
    The data could not be transferred, and will not become available in the future either.

    """
    def __init__(self, urls=None):
        """
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def list_content(self):
        """ List the content of this DU.

        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None

         """
        pass

    def list_srs(self):
        """ List the SRs and the state of where this DU is present.
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def wait(self, state='AVAILABLE'):
        """Wait for the DU to reach the specified state.
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def export(self, name, output_url, tsr=None):
        """Export the specified file of a DU to the specified directory (with the original name) or file.
           Optionally, if the DU exist on multiple SRs, the source SR can be specified,
           otherwise the system will decide.
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def export_all(self, output_url, tsr=None):
        """Export the content of a DU to the specified directory preserving file names.
           Optionally, if the DU exist on multiple SRs, the source SR can be specified,
           otherwise the system will decide.
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def remove(self):
        """Remove all copies of this DU from all SRs.
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

    def cancel(self):
        """Stops all services from dealing with this DU. Does not remove the data.
        Keyword argument(s)::

            name(type): description.

        Return::

            name(type): description.
            |
            None

        Raises::

            name: reason.
            |
            None
        """
        pass

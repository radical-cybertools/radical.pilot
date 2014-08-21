class TAFKAP_StorageResource(object):
    """ TAFKAP Storage Resource (TSR)

    State Model
    -----------
    * Unknown
    No state information could be obtained for this TFC.

    * Canceled
    Cancellation of this TSR has been requested.
    It can no longer be used for new actions.

    * Available
    The TSR is ready.


    """
    def __init__(self, url):
        pass

    def create_tfc(self, urls):
        """ Make the content of the URL(s) available at the TSR.

            Returns a TFC that references the contents of the file.
        """
        pass

    def add_tfc(self, tfcs):
        """ Make the content of the TFC(s) available at this TSR.

        """
        pass

    def remove_tfc(self, tfcs):
        """ Remove the content of the TFC from this TSR.
        """
        pass

    def list_tfcs(self):
        """List the tfcs and their state on this TSR
        """
        pass

    def export_tfc(self, url):
        """Export the content of a TFC to the specified location
        """
        pass

    def get_url(self):
        """Report the url of the TSR.
        """



class TAFKAP_FileContainer(object):
    """TAFKAP File Container (TFC)

    State Model
    -----------

    * Unknown
    No state information could be obtained for this TFC.

    * Pending
    Files are not yet available, but are scheduled for transfer, or transfer is in
    progress.

    * Available
    All files are available on at least one TSR.

    * Canceled
    The data in this TFC is scheduled for removal and cannot be used anymore.

    * Failed
    The data could not be transferred, and will not become available in the future either.

    """
    def __init__(self, urls=None):
        pass

    def list_content(self):
        """ List the content of this TFC
        """
        pass

    def list_tsrs(self):
        """ List the TSRs and the state of where this TFC is present.
        """
        pass

    def wait(self, state='AVAILABLE'):
        """Wait for the TFC to reach the specified state.
        """
        pass

    def export(self, name, output_url, tsr=None):
        """Export the specified file of a TFC to the specified directory (with the original name) or file.
           Optionally, if the TFC exist on multiple TSRs, the source TSR can be specified,
           otherwise the system will decide.
        """
        pass

    def export_all(self, output_url, tsr=None):
        """Export the content of a TFC to the specified directory preserving file names.
           Optionally, if the TFC exist on multiple TSRs, the source TSR can be specified,
           otherwise the system will decide.
        """
        pass


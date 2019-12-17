
import radical.saga.attributes as rsa

from spec_attribs import *


# ------------------------------------------------------------------------------
#
class CUD(rsa.Attributes):

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        # initialize attributes
        rsa.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        # action description
        self._attributes_register(KERNEL,           None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(NAME,             None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(EXECUTABLE,       None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(ARGUMENTS,        None, rsa.STRING, rsa.VECTOR, rsa.WRITEABLE)
        self._attributes_register(ENVIRONMENT,      None, rsa.STRING, rsa.DICT,   rsa.WRITEABLE)
        self._attributes_register(SANDBOX,          None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(PRE_EXEC,         None, rsa.STRING, rsa.VECTOR, rsa.WRITEABLE)
        self._attributes_register(POST_EXEC,        None, rsa.STRING, rsa.VECTOR, rsa.WRITEABLE)
        self._attributes_register(RESTARTABLE,      None, rsa.BOOL,   rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(TAGS,             None, rsa.ANY,    rsa.DICT,   rsa.WRITEABLE)
        self._attributes_register(METADATA,         None, rsa.ANY,    rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(CLEANUP,          None, rsa.BOOL,   rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(PILOT,            None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)


        # I/O
        self._attributes_register(STDOUT,           None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(STDERR,           None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(INPUT_STAGING,    None, rsa.ANY,    rsa.VECTOR, rsa.WRITEABLE)
        self._attributes_register(OUTPUT_STAGING,   None, rsa.ANY,    rsa.VECTOR, rsa.WRITEABLE)

        # resource requirements
        self._attributes_register(CPU_PROCESSES,    None, rsa.INT,    rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(CPU_PROCESS_TYPE, None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(CPU_THREADS,      None, rsa.INT,    rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(CPU_THREAD_TYPE,  None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(GPU_PROCESSES,    None, rsa.INT,    rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(GPU_PROCESS_TYPE, None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(GPU_THREADS,      None, rsa.INT,    rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(GPU_THREAD_TYPE,  None, rsa.STRING, rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(LFS_PER_PROCESS,  None, rsa.INT,    rsa.SCALAR, rsa.WRITEABLE)
        self._attributes_register(MEM_PER_PROCESS,  None, rsa.INT,    rsa.SCALAR, rsa.WRITEABLE)

        # explicitly set attrib defaults so they get listed and included via as_dict()
        self.set_attribute (KERNEL,           None)
        self.set_attribute (NAME,             None)
        self.set_attribute (EXECUTABLE,       None)
        self.set_attribute (SANDBOX,          None)
        self.set_attribute (ARGUMENTS,      list())
        self.set_attribute (ENVIRONMENT,    dict())
        self.set_attribute (PRE_EXEC,       list())
        self.set_attribute (POST_EXEC,      list())
        self.set_attribute (STDOUT,           None)
        self.set_attribute (STDERR,           None)
        self.set_attribute (INPUT_STAGING,  list())
        self.set_attribute (OUTPUT_STAGING, list())

        self.set_attribute (CPU_PROCESSES,       1)
        self.set_attribute (CPU_PROCESS_TYPE,   '')
        self.set_attribute (CPU_THREADS,         1)
        self.set_attribute (CPU_THREAD_TYPE,    '')
        self.set_attribute (GPU_PROCESSES,       0)
        self.set_attribute (GPU_PROCESS_TYPE,   '')
        self.set_attribute (GPU_THREADS,         1)
        self.set_attribute (GPU_THREAD_TYPE,    '')
        self.set_attribute (GPU_THREAD_TYPE,    '')
        self.set_attribute (LFS_PER_PROCESS,     0)
        self.set_attribute (MEM_PER_PROCESS,     0)

        self.set_attribute (RESTARTABLE,     False)
        self.set_attribute (TAGS,           dict())
        self.set_attribute (METADATA,         None)
        self.set_attribute (CLEANUP,         False)
        self.set_attribute (PILOT,              '')

        self._attributes_register_deprecated(CORES, CPU_PROCESSES)
        self._attributes_register_deprecated(MPI,   CPU_PROCESS_TYPE)

        # apply initialization dict
        if from_dict:
            self.from_dict(from_dict)


    # --------------------------------------------------------------------------
    #
    def __deepcopy__ (self, memo):

        other = CUD()

        for key in self.list_attributes ():
            other.set_attribute(key, self.get_attribute (key))

        return other


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def verify(self):
        '''
        Verify that the description is syntactically and semantically correct.
        This method encapsulates checks beyond the SAGA attribute level checks.
        '''

        # replace 'None' values for strng types with '', etc
        if self.get(KERNEL          ) is None: self[KERNEL          ] = ''
        if self.get(NAME            ) is None: self[NAME            ] = ''
        if self.get(EXECUTABLE      ) is None: self[EXECUTABLE      ] = ''
        if self.get(ARGUMENTS       ) is None: self[ARGUMENTS       ] = list()
        if self.get(ENVIRONMENT     ) is None: self[ENVIRONMENT     ] = dict()
        if self.get(PRE_EXEC        ) is None: self[PRE_EXEC        ] = list()
        if self.get(POST_EXEC       ) is None: self[POST_EXEC       ] = list()
        if self.get(PILOT           ) is None: self[PILOT           ] = ''
        if self.get(STDOUT          ) is None: self[STDOUT          ] = ''
        if self.get(STDERR          ) is None: self[STDERR          ] = ''
        if self.get(CPU_PROCESS_TYPE) is None: self[CPU_PROCESS_TYPE] = ''
        if self.get(CPU_THREAD_TYPE ) is None: self[CPU_THREAD_TYPE ] = ''
        if self.get(GPU_PROCESS_TYPE) is None: self[GPU_PROCESS_TYPE] = ''
        if self.get(GPU_THREAD_TYPE ) is None: self[GPU_THREAD_TYPE ] = ''
        if self.get(CPU_PROCESSES   ) is None: self[CPU_PROCESSES   ] = 0
        if self.get(CPU_THREADS     ) is None: self[CPU_THREADS     ] = 0
        if self.get(GPU_PROCESSES   ) is None: self[GPU_PROCESSES   ] = 0
        if self.get(GPU_THREADS     ) is None: self[GPU_THREADS     ] = 0
        if self.get(MEM_PER_PROCESS)  is None: self[MEM_PER_PROCESS ] = 0

        if  not self.get('executable') and \
            not self.get('kernel')     :
            raise ValueError("CU description needs 'executable' or 'kernel'")



# ------------------------------------------------------------------------------


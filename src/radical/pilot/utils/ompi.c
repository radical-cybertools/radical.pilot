
#include <Python.h>
#include <stdio.h>

// -----------------------------------------------------------------------------
//
static char submit_docs[] = "submit( ): submit a task\n";
static char ompi_docs[]   = "ompit task submission\n";


// -----------------------------------------------------------------------------
//
// take a DMV URL and a task description, submit the task to the DMV, and return
// a handle
// TODO: register for state notifications
static PyObject* submit(PyObject* self) {
    return Py_BuildValue("s", "task submitted");
}


// -----------------------------------------------------------------------------
//
static PyMethodDef ompi_funcs[] = {
    {"submit", (PyCFunction)submit, METH_NOARGS, submit_docs},
    {NULL}
};


// -----------------------------------------------------------------------------
//
void initompi(void) {
    Py_InitModule3("ompi", ompi_funcs, ompi_docs);
}


// -----------------------------------------------------------------------------


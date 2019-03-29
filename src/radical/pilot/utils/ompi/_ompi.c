
#include "Python.h"
#include  <stdio.h>

static PyObject *SpamError;

PyMODINIT_FUNC
initspam(void)
{
    PyObject *m;

    m = Py_InitModule("spam", SpamMethods);
    if (m == NULL)
        return;

    SpamError = PyErr_NewException("spam.error", NULL, NULL);
    Py_INCREF(SpamError);
    PyModule_AddObject(m, "error", SpamError);
}


static PyObject *
spam_system(PyObject *self, PyObject *args)
{
    const char *command;
    int sts;

    if (!PyArg_ParseTuple(args, "s", &command))
        return NULL;
    sts = system(command);
    return Py_BuildValue("i", sts);
}



static PyObject *
spam_system(PyObject *self, PyObject *args)
{
    const char *command;
    int sts;

    if (!PyArg_ParseTuple(args, "s", &command))
        return NULL;
    sts = system(command);
    if (sts < 0) {
        PyErr_SetString(SpamError, "System command failed");
        return NULL;
    }
    return PyLong_FromLong(sts);
}


static PyMethodDef SpamMethods[] = {
    ...
    {"system",  spam_system, METH_VARARGS,
     "Execute a shell command."},
    ...
    {NULL, NULL, 0, NULL}        /* Sentinel */
};


PyMODINIT_FUNC
initspam(void)
{
    (void) Py_InitModule("spam", SpamMethods);
}





// int submit(double x0, double y0, int max_iterations) {
// 
//     fprintf(stdout, "hello ompi");
// 
// 	return 42;
// }
// 
// 
// PyDoc_STRVAR(ompi__doc__, "ompi interface for task submission");
// PyDoc_STRVAR(submit__doc__, "submit a task to ompi");
// 
// static PyObject *
// py_submit(PyObject *self, PyObject *args)
// {
// 	double x=0, y=0;
//     int iterations, max_iterations=1000;
// 
// 	if (!PyArg_ParseTuple(args, "dd|i:iterate_point", &x, &y, &max_iterations))
//         return NULL;
// 
// 	iterations = submit(x, y, max_iterations);
// 	
// 	return PyInt_FromLong((long) iterations);
// }
// 
// static PyMethodDef ompi_methods[] = {
// 	{"submit",  py_submit, METH_VARARGS, submit__doc__},
// 	{NULL, NULL}      /* sentinel */
// };
// 
// PyMODINIT_FUNC
// init_ompi(void)
// {
//     fprintf(stdout, "init 1\n");
// 	/* There have been several InitModule functions over time */
//     return Py_InitModule3("radical.pilot.utils.ompi", ompi_methods, ompi__doc__);
// }
// 
// 
// // -----------------------------------------------------------------------------
// 

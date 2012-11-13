#include "dismage.h"
#include "constants.h"
#include "server.h"


static PyMethodDef dismage_methods[] = {
    {"listen", (PyCFunction)server_listen, METH_VARARGS | METH_KEYWORDS, "listen"},
    {"run", (PyCFunction)server_run, METH_VARARGS | METH_KEYWORDS, "run"},
    {"set_wait_callback", (PyCFunction)server_set_wait_callback, METH_VARARGS, "set_wait_callback"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

#ifdef PY3
#define INITERROR return NULL

static struct PyModuleDef dismage_module_def = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME,
    NULL,
    -1,
    dismage_methods,
};

PyObject *
PyInit_dismage(void)
#else
#define INITERROR return

PyMODINIT_FUNC
init_dismage(void)
#endif
{
    PyObject *m;
#ifdef PY3
    m = PyModule_Create(&dismage_module_def);
#else
    m = Py_InitModule3(MODULE_NAME, dismage_methods, "");
#endif
    if (m == NULL){
        INITERROR;
    }

    if (init_server() < 0){
        INITERROR;
    }

    add_drizzle_constants(m);
#ifdef PY3
    return m;
#endif
}


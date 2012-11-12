#include "result.h"

PyObject* create_start_result(PyObject *clientObj)
{
    ResultObject *result;

    result = PyObject_NEW(ResultObject, &ResultObjectType);

    if (result == NULL) {
       return NULL;
    }
    
    GDEBUG("alloc ResultObject:%p", result);

    result->columns = PyList_New(0);
    if (result->columns == NULL) {
        PyObject_DEL(result);
        return NULL;
    }

    return (PyObject*)result;
}


static void
ResultObject_dealloc(ResultObject *self)
{
    GDEBUG("dealloc ResultObject:%p", self);

    PyObject_DEL(self);
    
}

static PyObject *
ResultObject_call(PyObject *obj, PyObject *args, PyObject *kw)
{

    Py_RETURN_NONE;
}





static PyMethodDef ResultObject_methods[] = {
    {NULL, NULL} /* sentinel */
};

PyTypeObject ResultObjectType = {
#ifdef PY3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                    /* ob_size */
#endif
    MODULE_NAME ".Result",             /*tp_name*/
    sizeof(ResultObject), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)ResultObject_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    ResultObject_call,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE, /* tp_flags */
    "",                 /* tp_doc */
    0, /* tp_traverse */
    0,         /* tp_clear */
    0,                   /* tp_richcompare */
    0,                   /* tp_weaklistoffset */
    0,                   /* tp_iter */
    0,                       /* tp_iternext */
    ResultObject_methods,        /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                      /* tp_init */
    0,                         /* tp_alloc */
    0,                           /* tp_new */
    0,                           /* tp_new */
};

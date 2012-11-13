#include "util.h"

PyObject*
str_to_bytes(PyObject *value)
{
    PyObject *result = NULL;

#ifdef PY3
    if (!PyUnicode_Check(value)) {
        PyErr_Format(PyExc_TypeError, "expected unicode object, value "
                     "of type %.200s found", value->ob_type->tp_name);
        return NULL;
    }

    result = PyUnicode_AsLatin1String(value);

    if (!result) {
        PyErr_SetString(PyExc_ValueError, "unicode object contains non "
                        "latin-1 characters");
        return NULL;
    }
#else
    if (!PyBytes_Check(value)) {
        PyErr_Format(PyExc_TypeError, "expected byte string object, "
                     "value of type %.200s found", value->ob_type->tp_name);
        return NULL;
    }

    Py_INCREF(value);
    result = value;
#endif

    return result;
}

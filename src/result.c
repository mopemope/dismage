#include "result.h"
#include "server.h"
#include "client.h"

PyObject* create_start_result(PyObject *clientObj)
{
    ResultObject *result;

    result = PyObject_NEW(ResultObject, &ResultObjectType);

    if (result == NULL) {
       return NULL;
    }
    
    GDEBUG("alloc ResultObject:%p", result);

    result->columns = NULL;
    result->rows = NULL;
    

    return (PyObject*)result;
}


static void
ResultObject_dealloc(ResultObject *self)
{
    GDEBUG("dealloc ResultObject:%p", self);

    PyObject_DEL(self);
    
}

static PyObject *
ResultObject_call(PyObject *o, PyObject *args, PyObject *kwargs)
{
    
    PyObject *catalog = NULL;
    PyObject *db = NULL;
    PyObject *table = NULL;
    PyObject *columns = NULL;
    ResultObject *self = (ResultObject*)o;

    static char *kwlist[] = { "catalog", "db", "table", "columns", NULL } ;
  
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|sssO:start_response",
           kwlist, &catalog, &db, &table, &columns)){
        return NULL;
    }

    if (columns == NULL) {
        //TODO Change Err Type
        PyErr_SetString(PyExc_ValueError, "columns was not supplied");
        return NULL;
    }

    if (!PyList_Check(columns)) {
        PyErr_SetString(PyExc_TypeError, "columns must be list");
        return NULL;
    }
    
    self->columns = columns;

    Py_RETURN_NONE;
}


static io_state
write_columns_count(client_t *client, int count)
{
    drizzle_return_t ret;

    drizzle_result_set_column_count(client->result, (uint16_t)count);

    ret = drizzle_result_write(client->con, client->result, false);
    if (ret == DRIZZLE_RETURN_IO_WAIT) {
        return STATE_IO_WAIT;
    } else if (ret == DRIZZLE_RETURN_OK) {
        return STATE_OK;
    } else {
        //ERROR
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle_con_drizzle(client->con)));
        drizzle_result_free(client->result);
        client->result = NULL;
        return STATE_ERROR;
    }
}

static io_state
write_column_info(ClientObject *clientObj, ResultObject *result)
{
    drizzle_column_st column;
    drizzle_return_t ret;
    PyObject *columns, *iterator = NULL, *item = NULL;
    PyObject *type, *size, *name;

    io_state state = STATE_ERROR;
    client_t *client = clientObj->client;
    
    if (drizzle_column_create(client->result, &column) == NULL) {
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle_con_drizzle(client->con)));
        goto error;
    }

    drizzle_column_set_catalog(&column, "sqlite");
    drizzle_column_set_db(&column, "sqlite_db");
    drizzle_column_set_table(&column, "sqlite_table");
    drizzle_column_set_orig_table(&column, "sqlite_table");
    drizzle_column_set_charset(&column, 8);

    iterator = PyObject_GetIter(result->columns);
    if (PyErr_Occurred()){
        goto error; 
    }

    while ((item =  PyIter_Next(iterator))) {

        if (unlikely(!PyTuple_Check(item))) {
            PyErr_Format(PyExc_TypeError, "list of tuple values " "expected, value of type %.200s found",
                         item->ob_type->tp_name);
            goto error;
        }
        if (unlikely(PyTuple_GET_SIZE(item) != 2)) {
            PyErr_Format(PyExc_ValueError, "tuple of length 2 " "expected, length is %d",
                         (int)PyTuple_Size(item));
            goto error;
        }
        
        type = PyTuple_GET_ITEM(item, 0);
        size = PyTuple_GET_ITEM(item, 1);
        name = PyTuple_GET_ITEM(item, 2);
        


        drizzle_column_set_type(&column, DRIZZLE_COLUMN_TYPE_VARCHAR);
        drizzle_column_set_size(&column, 0);
        drizzle_column_set_name(&column, "");
        drizzle_column_set_orig_name(&column, "");

        while (1) {
            ret = drizzle_column_write(client->result, &column);
            if (ret == DRIZZLE_RETURN_IO_WAIT) {
                state = STATE_IO_WAIT;
            } else if (ret == DRIZZLE_RETURN_OK) {
                state = STATE_OK;
            } else {
                //ERROR
                RDEBUG("ret %d:%s", ret, drizzle_error(drizzle_con_drizzle(client->con)));
                goto error;
            }
            state = check_state(clientObj, state);
            BDEBUG("column write state:%d", state);
            if (state == STATE_OK) {
                Py_DECREF(item);
                break;
            } else if (state == STATE_ERROR) {
                goto error;
            }
        }
    }
    /* drizzle_column_free(&column); */

    drizzle_result_set_eof(client->result, true);

    while (1) {
        ret = drizzle_result_write(client->con, client->result, false);
        if (ret == DRIZZLE_RETURN_IO_WAIT) {
            state = STATE_IO_WAIT;
        } else if (ret == DRIZZLE_RETURN_OK) {
            state = STATE_OK;
        } else {
            //ERROR
            RDEBUG("ret %d:%s", ret, drizzle_error(drizzle_con_drizzle(client->con)));
            goto error;
        }
        state = check_state(clientObj, state);
        BDEBUG("column write state:%d", state);
        if (state == STATE_OK) {
            Py_DECREF(item);
            break;
        } else if (state == STATE_ERROR) {
            goto error;
        }
    }


error:
    Py_XDECREF(item);
    Py_XDECREF(iterator);
    drizzle_result_free(client->result);
    client->result = NULL;
    return STATE_ERROR;
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

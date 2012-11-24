#include "result.h"
#include "server.h"
#include "client.h"

PyObject* create_start_result()
{
    ResultObject *result;

    result = PyObject_NEW(ResultObject, &ResultObjectType);

    if (result == NULL) {
       return NULL;
    }
    
    GDEBUG("alloc ResultObject:%p", result);

    result->columns = NULL;
    /* result->rows = NULL; */
    

    return (PyObject*)result;
}


static void
ResultObject_dealloc(ResultObject *self)
{
    GDEBUG("dealloc ResultObject:%p", self);

    Py_CLEAR(self->columns);
    Py_XDECREF(self->catalog);
    Py_XDECREF(self->db);
    Py_XDECREF(self->table);
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
    
    Py_INCREF(columns);
    self->columns = columns;

    Py_XINCREF(catalog);
    self->catalog = catalog;

    Py_XINCREF(db);
    self->db = db;

    Py_XINCREF(table);
    self->table = table;

    Py_RETURN_NONE;
}


static io_state
send_result_data_internal(client_t *client, bool buffer)
{
    drizzle_return_t ret;

    ret = drizzle_result_write(client->con, client->result, buffer);
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
send_result_data(ClientObject *clientObj, bool buffer)
{
    io_state state = STATE_ERROR;
    client_t *client = clientObj->client;
    while (1) {
        state = send_result_data_internal(client, buffer);
        state = check_state(clientObj, state);
        BDEBUG("send_result_data state:%d", state);
        if (state == STATE_OK) {
            break;
        } else if (state == STATE_ERROR) {
            return state;
        }
    }
    return state;
}

static io_state
write_columns_count(ClientObject *clientObj, int count)
{
    client_t *client = clientObj->client;

    drizzle_result_set_column_count(client->result, (uint16_t)count);
    return send_result_data(clientObj, false);
}

static void
set_column_table_info(drizzle_column_st *column, ResultObject *start_result)
{
    char *catalog = NULL;
    char *db = NULL;
    char *table = NULL;
    
    // set catalog 
    if (start_result->catalog == NULL) {
        drizzle_column_set_catalog(column, "dismage_catalog");
    } else {
        catalog = PyBytes_AS_STRING(start_result->catalog);
        drizzle_column_set_catalog(column, catalog);
    }

    // set db
    if (start_result->db == NULL) {
        drizzle_column_set_db(column, "dismage_db");
    } else {
        db = PyBytes_AS_STRING(start_result->db);
        drizzle_column_set_db(column, db);
    }

    //set table 
    if (start_result->table== NULL) {
        drizzle_column_set_table(column, "dismage_table");
        drizzle_column_set_orig_table(column, "dismage_table");
    } else {
        table = PyBytes_AS_STRING(start_result->table);
        drizzle_column_set_table(column, table);
        drizzle_column_set_orig_table(column, table);
    }

    drizzle_column_set_charset(column, 8);
}

static io_state
set_columns_info(ClientObject *clientObj, drizzle_column_st *column, PyObject *item)
{
    PyObject *type, *size, *name;
    io_state state = STATE_ERROR;
    drizzle_column_type_t dtype = DRIZZLE_COLUMN_TYPE_VARCHAR;
    drizzle_return_t ret ;
    uint32_t dsize = 0;
    char *dname = NULL;
    client_t *client = clientObj->client;

    type = PyTuple_GET_ITEM(item, 0);
    size = PyTuple_GET_ITEM(item, 1);
    name = PyTuple_GET_ITEM(item, 2);
    
    // Check type
    if (type == NULL || !PyLong_Check(type)) {
        return STATE_ERROR;
    }

    // Check size
    if (size == NULL || !PyLong_Check(size)) {
        return STATE_ERROR;
    }
    
    //
    if (name == NULL || !PyBytes_Check(name)) {
        return STATE_ERROR;
    }
    
    dtype = (drizzle_column_type_t)PyLong_AsLong(type);
    dsize = (uint32_t)PyLong_AsLong(size);
    dname = PyBytes_AS_STRING(name);

    drizzle_column_set_type(column, dtype);
    drizzle_column_set_size(column, dsize);
    drizzle_column_set_name(column, dname);
    drizzle_column_set_orig_name(column, dname);

    while (1) {
        ret = drizzle_column_write(client->result, column);
        if (ret == DRIZZLE_RETURN_IO_WAIT) {
            state = STATE_IO_WAIT;
        } else if (ret == DRIZZLE_RETURN_OK) {
            state = STATE_OK;
        } else {
            //ERROR
            RDEBUG("ret %d:%s", ret, drizzle_error(drizzle_con_drizzle(client->con)));
            return state;
        }
        state = check_state(clientObj, state);
        BDEBUG("column write state:%d", state);
        if (state != STATE_IO_WAIT) {
            return state;
        }
    }
    return state;
}

static io_state
write_columns_info(ClientObject *clientObj)
{
    drizzle_column_st column;
    drizzle_return_t ret;
    PyObject *iterator = NULL, *item = NULL;

    io_state state = STATE_ERROR;
    client_t *client = clientObj->client;
    ResultObject *start_result = (ResultObject*)clientObj->start_result;
    
    if (drizzle_column_create(client->result, &column) == NULL) {
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle_con_drizzle(client->con)));
        goto error;
    }
    
    set_column_table_info(&column, start_result);

    iterator = PyObject_GetIter(start_result->columns);
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
        state = set_columns_info(clientObj, &column, item);
        Py_DECREF(item);
        if (state == STATE_ERROR) {
            item = NULL;
            goto error;
        }
        
    }
    /* drizzle_column_free(&column); */

    drizzle_result_set_eof(client->result, true);
    state = send_result_data(clientObj, false);
    Py_XDECREF(item);
    Py_XDECREF(iterator);
    
    return state;

error:
    Py_XDECREF(item);
    Py_XDECREF(iterator);
    drizzle_result_free(client->result);
    client->result = NULL;
    return STATE_ERROR;
}

static io_state
write_rows(ClientObject *clientObj, PyObject *resObj, uint32_t count)
{
    io_state state = STATE_ERROR;

    return state;
}

static PyObject*
return_simple_result(ClientObject *clientObj)
{
    io_state state = STATE_ERROR;

    state = send_result_data(clientObj, true);
    if (state == STATE_ERROR) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject*
return_row_result(ClientObject *clientObj, PyObject *resObj)
{
    io_state state = STATE_ERROR;
    PyObject *columns = NULL;
    int count = 0;

    ResultObject *start_result = (ResultObject*)clientObj->start_result;
    columns = start_result->columns;
    count = PyList_GET_SIZE(columns);

    state = write_columns_count(clientObj, count);
    DEBUG("write_columns_count clientObj:%p count:%d", clientObj, count);
    if (state == STATE_ERROR) {
        return NULL;
    }
    state = write_columns_info(clientObj);
    DEBUG("write_columns_info clientObj:%p resObj:%p", clientObj, resObj);
    if (state == STATE_ERROR) {
        return NULL;
    }

    // rows
    state = write_rows(clientObj, resObj, count);
    if (state == STATE_ERROR) {
        return NULL;
    }

    Py_RETURN_NONE;
}

PyObject* 
write_result(ClientObject *clientObj, PyObject *resObj)
{
    drizzle_result_st *result = NULL;
    
    client_t *client = clientObj->client;
    result = drizzle_result_create(client->con, NULL);
    if (result == NULL) {
        RDEBUG("ret %s", drizzle_error(drizzle_con_drizzle(client->con)));
        return NULL;
    }

    client->result = result;

    if (Py_None == resObj) {
        DEBUG("return None");
        return return_simple_result(clientObj);
    } else {
        DEBUG("return Rows");
        return return_row_result(clientObj, resObj);
    }
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

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
    result->catalog = NULL;
    result->db = NULL;
    result->table = NULL;
    /* result->rows = NULL; */
    

    return (PyObject*)result;
}


static void
ResultObject_dealloc(ResultObject *self)
{
    GDEBUG("dealloc ResultObject:%p", self);

    Py_XDECREF(self->columns);
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
check_return_type(ClientObject *clientObj, drizzle_return_t ret)
{
    io_state state = STATE_ERROR;
    client_t *client = clientObj->client;

    if (ret == DRIZZLE_RETURN_OK) {
        return STATE_OK;
    } else if (ret == DRIZZLE_RETURN_IO_WAIT) {
        state = STATE_IO_WAIT;
    } else {
        //ERROR
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle_con_drizzle(client->con)));
        PyErr_SetString(database_error, drizzle_error(drizzle_con_drizzle(client->con)));
        drizzle_result_free(client->result);
        client->result = NULL;
        return STATE_ERROR;
    }
    state = check_state(clientObj, state);
    return state;
}

static io_state
send_result_data_internal(ClientObject *clientObj, bool buffer)
{
    drizzle_return_t ret;

    client_t *client = clientObj->client;
    ret = drizzle_result_write(client->con, client->result, buffer);
    return check_return_type(clientObj, ret);
}

static io_state
send_result_data(ClientObject *clientObj, bool buffer)
{
    io_state state = STATE_ERROR;
    while (1) {
        state = send_result_data_internal(clientObj, buffer);
        if (state != STATE_IO_WAIT) {
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
    
    DEBUG("type:%p size:%p name:%p", type, size, name);

    // Check type
#ifdef PY3
    if (type == NULL || !PyLong_Check(type)) {
#else
    if (type == NULL || !PyInt_Check(type)) {
#endif
        RDEBUG("error type");
        PyErr_SetString(PyExc_TypeError, "must be int");
        return STATE_ERROR;
    }

    // Check size
#ifdef PY3
    if (size == NULL || !PyLong_Check(size)) {
#else
    if (size == NULL || !PyInt_Check(size)) {
#endif
        RDEBUG("error size");
        PyErr_SetString(PyExc_TypeError, "must be int");
        return STATE_ERROR;
    }
    
    //
    if (name == NULL || !PyBytes_Check(name)) {
        RDEBUG("error name");
        PyErr_SetString(PyExc_TypeError, "must be bytes");
        return STATE_ERROR;
    }
    
    dtype = (drizzle_column_type_t)PyLong_AsLong(type);
    dsize = (uint32_t)PyLong_AsLong(size);
    dname = PyBytes_AS_STRING(name);

    drizzle_column_set_type(column, dtype);
    drizzle_column_set_size(column, dsize);
    drizzle_column_set_name(column, dname);
    drizzle_column_set_orig_name(column, dname);
    
    DEBUG("column set type:%d, size:%d name:%s", (int)dtype, (int)dsize, dname);

    while (1) {
        ret = drizzle_column_write(client->result, column);
        state = check_return_type(clientObj, ret);
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
        PyErr_SetString(database_error, drizzle_error(drizzle_con_drizzle(client->con)));
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
        if (unlikely(PyTuple_GET_SIZE(item) != 3)) {
            PyErr_Format(PyExc_ValueError, "tuple of length 3 " "expected, length is %d",
                         (int)PyTuple_Size(item));
            goto error;
        }
        state = set_columns_info(clientObj, &column, item);
        Py_DECREF(item);
        if (state == STATE_ERROR) {
            RDEBUG("error");
            item = NULL;
            goto error;
        }
        
    }

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
write_row_internal(ClientObject *clientObj, PyObject *rowObj, uint32_t count)
{
    io_state state = STATE_ERROR;
    drizzle_return_t ret;
    PyObject *strObjs[count];
    Py_ssize_t sizes[count];
    char *fields[count];
    uint32_t x = 0;
    PyObject *strObj, *iterator = NULL, *item = NULL;
    client_t *client = clientObj->client;

    iterator = PyObject_GetIter(rowObj);
    if (PyErr_Occurred()){
        goto error; 
    }
  
    while ((item =  PyIter_Next(iterator))) {
        if (PyBytes_Check(item)) {
           Py_INCREF(item); 
           strObj = item;
        } else {
            strObj = PyObject_Bytes(item);
            if (strObj == NULL) {
                goto error;
            }
        }
        PyString_AsStringAndSize(strObj, (char**)&fields[x], &sizes[x]);
        DEBUG("field:%s size:%d", fields[x], (int)sizes[x]);

        Py_XDECREF(item);
        strObjs[x] = strObj;
        x++;
    }
    Py_DECREF(iterator);
    
    drizzle_result_calc_row_size(client->result, (drizzle_field_t *)fields, (size_t*)sizes);

    while (1) {
        ret = drizzle_row_write(client->result);
        state = check_return_type(clientObj, ret);
        BDEBUG("drizzle_row_write:%d", state);
        if (state == STATE_ERROR) {
             goto error;
        } else if(state == STATE_OK) {
            break;
        }
    }


    for (x = 0; x < count; x++) {
        while (1) {
            ret = drizzle_field_write(client->result, (drizzle_field_t)fields[x], (size_t)sizes[x], (size_t)sizes[x]);
            state = check_return_type(clientObj, ret);
            BDEBUG("drizzle_row_write:%d", state);
            if (state == STATE_ERROR) {
                 goto error;
            } else if(state == STATE_OK) {
                break;
            }
        }
    }

    for (x = 0; x < count; x++) {
        Py_XDECREF(strObjs[x]);
    }
    return STATE_OK;

error:
    Py_XDECREF(item);
    Py_XDECREF(iterator);
    for (x = 0; x < count; x++) {
        Py_XDECREF(strObjs[x]);
    }

    return STATE_ERROR;
}

static io_state
write_last_data(ClientObject *clientObj)
{
    drizzle_return_t ret;
    client_t *client = clientObj->client;
    io_state state = STATE_ERROR;

    drizzle_result_set_eof(client->result, true);
    ret = drizzle_result_write(client->con, client->result, true);
    state = check_return_type(clientObj, ret);
   
    if (client->result != NULL) {
        drizzle_result_free(client->result);
        client->result = NULL;
    }
    return state;
}

static io_state
write_rows(ClientObject *clientObj, PyObject *resObj, uint32_t count)
{
    io_state state = STATE_ERROR;
    PyObject *iterator = NULL, *item = NULL;

    iterator = PyObject_GetIter(resObj);
    if (PyErr_Occurred()){
        goto error; 
    }
    
    while ((item = PyIter_Next(iterator))) {

        if (unlikely(!PyTuple_Check(item))) {
            PyErr_Format(PyExc_TypeError, "list of tuple values " "expected, value of type %.200s found",
                         item->ob_type->tp_name);
            goto error;
        }
        
        if (unlikely(PyTuple_GET_SIZE(item) != count)) {
            PyErr_Format(PyExc_ValueError, "tuple of length %d " "expected, length is %d",
                         count, (int)PyTuple_Size(item));
            goto error;
        }
        
        state = write_row_internal(clientObj, item, count);
        if (state == STATE_ERROR) {
            goto error;
        }
        Py_DECREF(item);
    }

    Py_DECREF(iterator);

    return state;

error:
    Py_XDECREF(item);
    Py_XDECREF(iterator);

    return STATE_ERROR;
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
    DEBUG("write_columns_count clientObj:%p count:%d state:%d", clientObj, count, (int)state);
    if (state == STATE_ERROR) {
        return NULL;
    }

    state = write_columns_info(clientObj);
    DEBUG("write_columns_info clientObj:%p resObj:%p state:%d", clientObj, resObj, (int)state);
    if (state == STATE_ERROR) {
        return NULL;
    }
    
    // rows
    state = write_rows(clientObj, resObj, count);
    DEBUG("write_row clientObj:%p resObj:%p", clientObj, resObj);
    if (state == STATE_ERROR) {
        return NULL;
    }

    state = write_last_data(clientObj);
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
        PyErr_SetString(database_error, drizzle_error(drizzle_con_drizzle(client->con)));
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

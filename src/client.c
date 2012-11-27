#include "client.h"


static client_t*
new_client_t(drizzle_con_st *con)
{
    client_t *client;
    int fd = 0;

    fd = drizzle_con_fd(con);
    if (fd == -1) {
        return NULL;
    }
    client = PyMem_Malloc(sizeof(client_t));

    if (client == NULL) {
        return NULL;
    }

    memset(client, 0x0, sizeof(client_t));

    client->con = con;
    client->client_fd = fd;
    client->result = NULL;
    
    GDEBUG("alloc client_t:%p", client);
    return client;
}

static void
free_client_t(client_t *client)
{
    GDEBUG("dealloc client_t:%p", client);
    if (client->result) {
        drizzle_result_free(client->result);
        client->result = NULL;
    }
    drizzle_con_close(client->con);
    drizzle_con_free(client->con);
    client->con = NULL;
    PyMem_Free(client);
}

PyObject*
ClientObject_new(drizzle_con_st *con)
{
    ClientObject *clientObj = NULL;
    client_t *client = NULL;

    clientObj = (ClientObject*)PyObject_GC_New(ClientObject, &ClientObjectType);
    if (clientObj == NULL) {
       return NULL;
    }
    
    
    client = new_client_t(con);
    if (client == NULL) {
        Py_DECREF(clientObj);
        return NULL;
    }

    clientObj->greenlet = NULL;
    clientObj->start_result = NULL;

    clientObj->client = client;
    PyObject_GC_Track(clientObj);
    GDEBUG("alloc ClientObject%p", clientObj);
    return (PyObject*)clientObj;
}

int
CheckClientObject(PyObject *obj)
{
    if (obj->ob_type != &ClientObjectType) {
        return 0;
    }
    return 1;
}

static int
ClientObject_clear(ClientObject *self)
{
    GDEBUG("self:%p", self);
    Py_XDECREF(self->start_result);
    Py_CLEAR(self->greenlet);
    return 0;
}

static int
ClientObject_traverse(ClientObject *self, visitproc visit, void *arg)
{
    GDEBUG("self:%p", self);
    Py_VISIT(self->greenlet);
    return 0;
}
static void
ClientObject_dealloc(ClientObject *self)
{
    GDEBUG("dealloc ClientObject %p", self);
    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_SAFE_BEGIN(self);
    ClientObject_clear(self);
    if (self->client) {
        free_client_t(self->client);
    }
    PyObject_GC_Del(self);
    Py_TRASHCAN_SAFE_END(self);
}

static PyMethodDef ClientObject_methods[] = {
    {NULL, NULL} /* sentinel */
};

PyTypeObject ClientObjectType = {
#ifdef PY3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                    /* ob_size */
#endif
    MODULE_NAME ".Client",             /*tp_name*/
    sizeof(ClientObject), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)ClientObject_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE, /* tp_flags */
    "",                 /* tp_doc */
    (traverseproc)ClientObject_traverse, /* tp_traverse */
    (inquiry)ClientObject_clear,         /* tp_clear */
    0,                   /* tp_richcompare */
    0,                   /* tp_weaklistoffset */
    0,                   /* tp_iter */
    0,                       /* tp_iternext */
    ClientObject_methods,        /* tp_methods */
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
    PyObject_GC_Del,                           /* tp_new */
};

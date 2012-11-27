#ifndef SERVER_H
#define SERVER_H

#include "dismage.h"
#include "client.h"

int init_server(void);

io_state check_state(ClientObject *clientObj, io_state state);

PyObject* server_set_wait_callback(PyObject *obj, PyObject *args);

PyObject* server_listen(PyObject *obj, PyObject *args, PyObject *kwargs);

PyObject* server_run(PyObject *obj, PyObject *args, PyObject *kwargs);

PyObject* server_io_trampoline(PyObject *self, PyObject *args, PyObject *kwargs);

PyObject* server_cancel_wait(PyObject *self, PyObject *args);

#endif

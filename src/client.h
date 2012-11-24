#ifndef CLIENT_H
#define CLIENT_H

#include "dismage.h"


typedef struct _client {
    drizzle_con_st *con;
    int client_fd;
    drizzle_result_st *result;
} client_t;

typedef struct {
    PyObject_HEAD
    client_t *client;
    PyObject *greenlet;
    PyObject *start_result;
} ClientObject;

extern PyTypeObject ClientObjectType;


PyObject* ClientObject_new(drizzle_con_st *con);

#endif

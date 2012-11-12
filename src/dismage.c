#include "dismage.h"
#include "picoev.h"
#include "greensupport.h"
#include "client.h"
#include "result.h"

#define ACCEPT_TIMEOUT_SECS 0
#define TIMEOUT_SECS 300

drizzle_st *drizzle;

static PyObject *external_io_wait = NULL;
static drizzle_con_st *listen_conn = NULL;
static int listen_sock = 0;
static char *unix_sock_name = NULL;

static uint32_t activecnt = 0;

static PyObject *app = NULL;
static PyObject *watchdog = NULL;
static PyObject *hub_switch_value = NULL;

static volatile sig_atomic_t loop_done = 0;
static volatile sig_atomic_t catch_signal = 0;

static picoev_loop* main_loop = NULL; //main loop

static int backlog = 1024 * 4; // backlog size
static int max_fd = 1024 * 4;  // picoev max_fd

static void
kill_callback(picoev_loop* loop, int fd, int events, void* cb_arg);


static void
init_main_loop(void)
{
    if (main_loop == NULL) {
        picoev_init(max_fd);
        main_loop = picoev_create_loop(60);
    }
}

static void
kill_callback(picoev_loop* loop, int fd, int events, void* cb_arg)
{
    if ((events & PICOEV_TIMEOUT) != 0) {
        DEBUG("force shutdown...");
        loop_done = 0;
    }
}

static inline void
kill_server(int timeout)
{
    if (main_loop == NULL) {
        return;
    }

    //stop accepting
    if (!picoev_del(main_loop, listen_sock)) {
        activecnt--;
        DEBUG("activecnt:%d", activecnt);
    }

    //shutdown timeout
    if (timeout > 0) {
        //set timeout
        (void)picoev_add(main_loop, listen_sock, PICOEV_TIMEOUT, timeout, kill_callback, NULL);
    } else {
        (void)picoev_add(main_loop, listen_sock, PICOEV_TIMEOUT, 1, kill_callback, NULL);
    }
}

static void
sigint_cb(int signum)
{
    DEBUG("call SIGINT");
    kill_server(0);
    if(!catch_signal){
        catch_signal = 1;
    }
}

static void
sigpipe_cb(int signum)
{
    DEBUG("call SIGPIPE");
}

static int
init_drizzle(void)
{
    drizzle = drizzle_create(NULL);
    if (drizzle == NULL) {
        PyErr_SetString(PyExc_IOError, "drizzle_st create failed");
        RDEBUG("drizzle_st create failed");
        return -1;
    }
    drizzle_add_options(drizzle, DRIZZLE_NON_BLOCKING);
    return 1;
}


int
io_wait(drizzle_con_st *con, drizzle_return_t ret)
{
    drizzle_return_t dret;
    int fd = 0, events = 0;
    PyObject *fileno, *state, *args, *res;

    if (ret == DRIZZLE_RETURN_OK) {
        return 0;
    }else if (ret == DRIZZLE_RETURN_IO_WAIT) {
        events = con->events;

        YDEBUG("IO_WAIT con:%p events:%d", con, events);
        if (external_io_wait) {
            fd = drizzle_con_fd(con);
            if (fd == -1){
                return -1;
            }

            fileno = PyLong_FromLong((long)fd);
            if (fileno == NULL) {
                return -1;
            }
            state = PyLong_FromLong((long)events);
            if (state == NULL) {
                Py_DECREF(fileno);
                return -1;
            }

            args = PyTuple_Pack(2, fileno, state);
            if (args == NULL) {
                Py_DECREF(fileno);
                Py_DECREF(state);
                return -1;
            }

            YDEBUG("call external_io_wait ...");
            res = PyObject_CallObject(external_io_wait, args);
            Py_DECREF(args);

            if (res == NULL) {
                return -1;
            }
            Py_XDECREF(res);
            dret = drizzle_con_set_revents(con, events);
            if (dret != DRIZZLE_RETURN_OK){
                RDEBUG("ret %d:%s", dret, drizzle_error(drizzle));
                return -1;
            }
            return 1;
        } else {
            DEBUG("call drizzle_con_wait ...");
            dret = drizzle_con_wait(drizzle);

            if (dret != DRIZZLE_RETURN_OK){
                RDEBUG("ret %d:%s", dret, drizzle_error(drizzle));
                return -1;
            }
            return 1;
        }
    }else{
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
        return -1;
    }
    return 0;
}


static void
trampoline_callback(picoev_loop* loop, int fd, int events, void* cb_arg)
{

    ClientObject *clientObj = NULL;
    client_t *client = NULL;
    PyObject *greenlet = NULL, *res = NULL;

    DEBUG("trampoline_callback fd:%d", fd);
    if(!picoev_del(loop, fd)){
        activecnt--;
        DEBUG("activecnt:%d", activecnt);
    }

    clientObj = (ClientObject*)cb_arg;
    client = clientObj->client;

    if ((events & PICOEV_TIMEOUT) != 0) {
        RDEBUG("** trampoline_callback timeout **");
        //TODO ERROR
    } else if ((events & PICOEV_READ) != 0) {
        drizzle_con_set_revents(client->con, POLLIN);
        DEBUG("set revents POLLIN");
    } else if ((events & PICOEV_WRITE) != 0) {
        drizzle_con_set_revents(client->con, POLLOUT);
        DEBUG("set revents POLLOUT");
    }
    greenlet = clientObj->greenlet;

    YDEBUG("switch greenlet:%p", greenlet);
    res = greenlet_switch(greenlet, hub_switch_value, NULL);
    Py_XDECREF(res);
    if (greenlet_dead(greenlet)) {
        DEBUG("greenlet is dead:%p", greenlet);
        Py_XDECREF(greenlet);
    }
}

static int
internal_io_wait(ClientObject *clientObj)
{
    int events = 0, ret = 0, fd = 0;
    PyObject *current = NULL, *parent = NULL, *res = NULL;

    client_t *client = clientObj->client;
    drizzle_con_st *con = client->con;
    
    fd = client->client_fd;
    events = con->events;
    YDEBUG("IO_WAIT con:%p events:%d", con, events);
    
    current = clientObj->greenlet;
    parent = greenlet_getparent(current);
    
    DEBUG("current:%p parent:%p", current, parent);
    DEBUG("current refcnt:%d", Py_REFCNT(current));

    if (events & POLLIN) {
        DEBUG("set read event fd:%d", fd);
        ret = picoev_add(main_loop, fd, PICOEV_READ, TIMEOUT_SECS, trampoline_callback, clientObj);
        if(ret == 0){
            activecnt++;
        }
    }else if (events & POLLOUT) {
        DEBUG("set write event fd:%d", fd);
        ret = picoev_add(main_loop, fd, PICOEV_WRITE, TIMEOUT_SECS, trampoline_callback, clientObj);
        if(ret == 0){
            activecnt++;
        }
    }
    YDEBUG("switch greenlet:%p", parent);
    res = greenlet_switch(parent, hub_switch_value, NULL);
    Py_XDECREF(res);
    return 1;
}

static io_state
check_state(ClientObject *clientObj, io_state state)
{

    DEBUG("check io_state:%d", state);

    if (state == STATE_IO_WAIT) {
        internal_io_wait(clientObj);
        if (PyErr_Occurred()) {
            PyErr_Print();
            return STATE_ERROR;
        }
        return STATE_IO_WAIT;
    } else if (state == STATE_ERROR) {
        return state;
    }
    return STATE_OK;
}

static io_state 
return_result(client_t *client)
{
    drizzle_result_st *result = NULL;
    drizzle_con_st *con = NULL;
    drizzle_return_t ret = DRIZZLE_RETURN_OK;

    con = client->con;
    result = client->result;

    ret = drizzle_result_write(con, result, true);
    if (ret == DRIZZLE_RETURN_IO_WAIT) {
        return STATE_IO_WAIT;
    } else if (ret == DRIZZLE_RETURN_OK) {
        drizzle_result_free(client->result);
        client->result = NULL;
        client->state = CALL_HANDLER;
        return STATE_OK;
    } else {
        //ERROR
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
        drizzle_result_free(client->result);
        client->result = NULL;
        return STATE_ERROR;
    }
}

static io_state 
handshake_return_result(client_t *client)
{
    drizzle_result_st *result = NULL;
    drizzle_con_st *con = NULL;
    drizzle_return_t ret = DRIZZLE_RETURN_OK;

    con = client->con;
    
    result = drizzle_result_create(con, NULL);
    if (result == NULL) {
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
        return STATE_ERROR;
    }

    client->result = result;
    
    return return_result(client);

}

static io_state 
handshake_client_read(client_t *client)
{
    drizzle_con_st *con = NULL;
    drizzle_return_t ret;
    
    con = client->con;
    ret = drizzle_handshake_client_read(con);

    DEBUG("handshake_read con:%p ret:%d", con, ret);
    if (ret == DRIZZLE_RETURN_IO_WAIT) {
        return STATE_IO_WAIT;
    } else if (ret == DRIZZLE_RETURN_OK) {
        client->state = CALL_HANDLER;
        return STATE_OK;
    } else {
        //ERROR
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
        return STATE_ERROR;
    }
    /* return STATE_OK; */
}

static io_state 
handshake_server_write(client_t *client)
{
    drizzle_con_st *con = NULL;
    drizzle_return_t ret;
    
    client->state = HANDSHAKE_WRITE;
    con = client->con;
    drizzle_con_set_protocol_version(con, 10);
    drizzle_con_set_server_version(con, SERVER);
    drizzle_con_set_thread_id(con, 1);
    drizzle_con_set_scramble(con, (const uint8_t *)"ABCDEFGHIJKLMNOPQRST");
    drizzle_con_set_capabilities(con, DRIZZLE_CAPABILITIES_NONE);
    drizzle_con_set_charset(con, 8);
    drizzle_con_set_status(con, DRIZZLE_CON_STATUS_NONE);
    drizzle_con_set_max_packet_size(con, DRIZZLE_MAX_PACKET_SIZE);
    
    ret = drizzle_handshake_server_write(con);
    DEBUG("handshake_write con:%p ret:%d", con, ret);
    if (ret == DRIZZLE_RETURN_IO_WAIT) {
        return STATE_IO_WAIT;
    } else if (ret == DRIZZLE_RETURN_OK) {
        client->state = HANDSHAKE_READ;
        return STATE_OK;
    } else {
        //ERROR
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
        return STATE_ERROR;
    }
    /* return STATE_OK; */
}

static int
handshake_client(ClientObject *clientObj)
{
    io_state state = STATE_ERROR;
    client_t *client = clientObj->client;
    DEBUG("client:%p", client);

    
    while (1) {
        state = handshake_server_write(client);
        state = check_state(clientObj, state);
        BDEBUG("write state:%d", state);
        if (state == STATE_OK) {
            break;
        } else if (state == STATE_ERROR) {
            return -1;
        }
    }

    while (1) { 
        state = handshake_client_read(client);
        state = check_state(clientObj, state);
        BDEBUG("read state:%d", state);
        if (state == STATE_OK) {
            break;
        } else if (state == STATE_ERROR) {
            return -1;
        }
    }


    while (1) {
        state = handshake_return_result(client);
        state = check_state(clientObj, state);
        BDEBUG("handshake_return_result state:%d", state);
        if (state == STATE_OK) {
            break;
        } else if (state == STATE_ERROR) {
            return -1;
        }
    }
    //All OK
    return 1;
}

static PyObject* 
read_command(ClientObject *clientObj)
{
    PyObject *o;
    drizzle_con_st *con;
    client_t *client;
    drizzle_return_t ret;
    drizzle_command_t command;
    io_state state = STATE_ERROR;
    uint8_t *data= NULL;
    size_t total, offset, size;

    client = clientObj->client;
    con = client->con;

    while (1) {
        data = (uint8_t *)drizzle_con_command_read(con, &command, &offset, &size, &total, &ret);
        DEBUG("command read ret:%d", ret);
        if (ret == DRIZZLE_RETURN_LOST_CONNECTION || (ret == DRIZZLE_RETURN_OK && command == DRIZZLE_COMMAND_QUIT)) {
            DEBUG("QUIT");
            if (data) {
                /* free(data); */
            }
            return NULL;
        } 
        
        if (ret == DRIZZLE_RETURN_IO_WAIT) {
            state = STATE_IO_WAIT;
        } else if (ret == DRIZZLE_RETURN_OK) {
            state = STATE_OK;
        } else {
            //ERROR
            RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
            return NULL;
        }
        state = check_state(clientObj, state);
        BDEBUG("command_read state:%d", state);
        if (state == STATE_OK) {
            break;
        } else if (state == STATE_ERROR) {
            return NULL;
        }
    }
    DEBUG("offset:%d size:%d total:%d", offset, size, total);
    o = Py_BuildValue("(is#)", (int)command, data, size);
    /* free(data); */

    return o;
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
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
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
    io_state state = STATE_ERROR;
    client_t *client = clientObj->client;
    
    if (drizzle_column_create(client->result, &column) == NULL) {
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
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
        if(PyTuple_Check(item)){
            drizzle_column_set_type(&column, DRIZZLE_COLUMN_TYPE_VARCHAR);
            drizzle_column_set_size(&column, 0);
            drizzle_column_set_name(&column, "");
            drizzle_column_set_orig_name(&column, "");

retry:
            ret = drizzle_column_write(client->result, &column);
            if (ret == DRIZZLE_RETURN_IO_WAIT) {
                state = check_state(clientObj, state);
                if (state == STATE_OK) {
                    goto retry;
                } else if (state == STATE_ERROR) {
                    goto error;
                }

            } else if (ret == DRIZZLE_RETURN_OK) {
                Py_DECREF(item);
                continue;
            } else {
                //ERROR
                RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
                goto error;
            }
        } else {
            //TODO Error
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

static PyObject* 
write_result(ClientObject *clientObj, PyObject *o)
{
    drizzle_result_st *result = NULL;
    drizzle_con_st *con = NULL;
    drizzle_return_t ret = DRIZZLE_RETURN_OK;
    io_state state = STATE_ERROR;

    client_t *client = clientObj->client;
    con = client->con;
    
    result = drizzle_result_create(con, NULL);
    if (result == NULL) {
        RDEBUG("ret %d:%s", ret, drizzle_error(drizzle));
        return NULL;
    }

    client->result = result;
    
    while (1) {
        if (Py_None == o) {
            DEBUG("return None");
            state = return_result(client);
        } else {
            state = return_result(client);
        }
        state = check_state(clientObj, state);
        BDEBUG("handshake_return_result state:%d", state);
        if (state == STATE_OK) {
            break;
        } else if (state == STATE_ERROR) {
            return NULL;
        }
    }
}

static PyObject*
disage_handler(PyObject *self, PyObject *o)
{
    int ret = 0;
    PyObject *args = NULL, *res = NULL, *result_res = NULL;
    ClientObject *clientObj = (ClientObject*)o;
    
    ret = handshake_client(clientObj);
    DEBUG("handshake ret:%d", ret);
    
    if (ret == -1) {
        //TOD set Error
        //ERROR
        Py_RETURN_NONE;
    }
    
    while (1) {    
        args = read_command(clientObj);
        DEBUG("args:%p", args);
        /* state = handshake_client(client); */
        if (args == NULL) {
            //ERROR
            Py_RETURN_NONE;
        }
        res = PyObject_CallObject(app, args);
        Py_DECREF(args);
        if (res == NULL) {
            //ERROR
            Py_RETURN_NONE;
        }
        result_res = write_result(clientObj, res);
        Py_XDECREF(result_res);
        Py_XDECREF(res);
    }

    Py_RETURN_NONE;
}

static PyObject *handler_func = NULL;
static PyMethodDef handler_def = {"_handler",   (PyCFunction)disage_handler, METH_O, 0};

static PyObject*
get_disage_handler(void)
{
    if(handler_func == NULL){
        handler_func = PyCFunction_NewEx(&handler_def, (PyObject *)NULL, NULL);
    }
    return handler_func;
}

static void
call_disage_handler(drizzle_con_st *con)
{
    ClientObject *clientObj;
    PyObject *handler, *greenlet, *args, *res;
    
    clientObj = (ClientObject*)ClientObject_new(con);
    if (clientObj == NULL) {
        // ERROR
        return;
    }

    handler = get_disage_handler();
    args = PyTuple_Pack(1, clientObj);
    if (args == NULL) {
        //TODO Error
        return;
    }
    greenlet = greenlet_new(handler, NULL);
    if (greenlet == NULL) {
        //TODO Error
        Py_DECREF(args);
        return;
    }
    /* Py_DECREF(greenlet_getparent(greenlet)); */
    clientObj->greenlet = greenlet;

    DEBUG("start client:%p", clientObj);
    res = greenlet_switch(greenlet, args, NULL);
    Py_DECREF(args);
    Py_XDECREF(res);
    DEBUG("end client:%p", clientObj);
    Py_DECREF(clientObj);
    /* if (greenlet_dead(greenlet)) { */
        /* Py_DECREF(greenlet); */
    /* } */
}


static void
accept_callback(picoev_loop* loop, int fd, int events, void* cb_arg)
{
    drizzle_con_st *con;
    drizzle_return_t ret;
    
    DEBUG("call accept_callback");
    if ((events & PICOEV_TIMEOUT) != 0) {
        RDEBUG("** timeout _**");
        return;
    }else if ((events & PICOEV_READ) != 0) {
        
        drizzle_con_set_revents(listen_conn, POLLIN);

        while (1) {
            con = drizzle_con_accept(drizzle, NULL, &ret);
            
            DEBUG("accept ret:%d", ret);
            if (ret == DRIZZLE_RETURN_IO_WAIT) {
                DEBUG("IO_WAIT?");
                return;
            }
            if (ret != DRIZZLE_RETURN_OK) {
                //TODO Error
                RDEBUG("drizzle_con_accept:%s\n", drizzle_error(drizzle));
                return;
            }
            
            call_disage_handler(con);
        }

    }
}

static PyObject*
disage_set_wait_callback(PyObject *obj, PyObject *args)
{
    PyObject *temp = NULL;

    if (!PyArg_ParseTuple(args, "O", &temp)) {
        return NULL;
    }

    if (!PyCallable_Check(temp)) {
        PyErr_SetString(PyExc_TypeError, "must be callable");
        return NULL;
    }

    if (external_io_wait) {
        Py_DECREF(external_io_wait);
    }
    
    external_io_wait = temp;
    Py_INCREF(external_io_wait);

    Py_RETURN_NONE;
}

static PyObject*
disage_listen(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    char *host = DRIZZLE_DEFAULT_TCP_HOST;
    char *unix_socket = NULL;
    int mysql = 1, port = 3306;
    drizzle_con_st *conn = NULL;
    
    static char *kwlist[] = { "host", "port", "unix_socket", NULL } ;
  
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|sis:listen",
           kwlist, &host, &port, &unix_socket)){
        return NULL;
    }

    if (listen_conn) {
        PyErr_SetString(PyExc_IOError, "already listened");
        return NULL;
    }
    Py_BEGIN_ALLOW_THREADS;
    conn = drizzle_con_create(drizzle, NULL);
    Py_END_ALLOW_THREADS;

    if (conn == NULL) {
        PyErr_SetString(PyExc_IOError, drizzle_error(drizzle));
        return NULL;
    }
    
    DEBUG("host :%s", host);
    DEBUG("port :%d", port);

    Py_BEGIN_ALLOW_THREADS;
    drizzle_con_add_options(conn, DRIZZLE_CON_LISTEN);
    drizzle_con_set_backlog(conn, backlog);
    drizzle_con_set_tcp(conn, host, port);

    if (mysql) {
        DEBUG("set mysql");
        drizzle_con_add_options(conn, DRIZZLE_CON_MYSQL);
    }
    Py_END_ALLOW_THREADS;

    if (drizzle_con_listen(conn) != DRIZZLE_RETURN_OK) {
        PyErr_SetString(PyExc_IOError, drizzle_error(drizzle));
        drizzle_con_free(conn);
        return NULL;
    }

    listen_sock = drizzle_con_fd(conn);
    listen_conn = conn;
    DEBUG("listen:%p fd:%d", listen_conn, listen_sock);
    Py_RETURN_NONE;
}

static void 
builtin_loop(void)
{
    int ret = 0;
    loop_done = 1;
    PyObject *watchdog_result = NULL;
    init_main_loop();

    PyOS_setsig(SIGPIPE, sigpipe_cb);
    PyOS_setsig(SIGINT, sigint_cb);
    PyOS_setsig(SIGTERM, sigint_cb);
    
    ret = picoev_add(main_loop, listen_sock, PICOEV_READ, ACCEPT_TIMEOUT_SECS, accept_callback, NULL);
    if(ret == 0){
        activecnt++;
    }
    while (likely(loop_done == 1 && activecnt > 0)) {
        picoev_loop_once(main_loop, 10);
        if (watchdog) {
            watchdog_result = PyObject_CallFunction(watchdog, NULL);
            if(PyErr_Occurred()){
                PyErr_Print();
                PyErr_Clear();
            }
            Py_XDECREF(watchdog_result);
        }
        /* DEBUG("wait...."); */
    }
    Py_DECREF(app);
    Py_XDECREF(watchdog);

}

static void
run_loop(void)
{
    DEBUG("DRIZZLE_MAX_SCRAMBLE_SIZE:%d", DRIZZLE_MAX_SCRAMBLE_SIZE);
    builtin_loop();
    DEBUG("fin loop");
}


static PyObject*
disage_run(PyObject *obj, PyObject *args, PyObject *kwargs)
{
    PyObject *temp = NULL;

    static char *kwlist[] = { "app", NULL } ;
  
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O:run",
           kwlist, &temp)){
        return NULL;
    }
    if (app) {
        Py_DECREF(app);
    }
    app = temp;
    Py_INCREF(app);

    run_loop();
    
    DEBUG("fin");
    Py_CLEAR(app);
    picoev_destroy_loop(main_loop);
    picoev_deinit();
    main_loop = NULL;
    drizzle_con_free(listen_conn);

    Py_RETURN_NONE;
}

static PyMethodDef dismage_methods[] = {
    {"listen", (PyCFunction)disage_listen, METH_VARARGS | METH_KEYWORDS, "listen"},
    {"run", (PyCFunction)disage_run, METH_VARARGS | METH_KEYWORDS, "run"},
    {"set_wait_callback", (PyCFunction)disage_set_wait_callback, METH_VARARGS, "set_wait_callback"},
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

    if (init_drizzle() < 0){
        INITERROR;
    }
    hub_switch_value = PyTuple_New(0);
#ifdef PY3
    return m;
#endif
}


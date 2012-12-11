import dismage
from dismage import patch
patch.patch_all()
import time
import pymysql

running = False

def start_server(app):
    global running 
    if running:
        return

    dismage.listen()
    running = True

    return app

class ServerRunner(object):

    def __init__(self, app):
        self.app = app
        self.running = False

    def run(self, shutdown=False):
        if self.running:
            return

        dismage.listen(port=3307)
        self.running = True
        if shutdown:
            dismage.schedule_call(1, dismage.shutdown, 1)
        dismage.run(self.app)

class ClientRunner(object):


    def __init__(self, app, func, shutdown=True):
        self.func = func
        self.app = app
        self.shutdown = shutdown
        self.result = None

    def run(self):

        def _call():
            try:
                r = self.func()
                self.result = r
            finally:
                if self.shutdown:
                    dismage.shutdown(1)

        dismage.spawn(_call)

    def get_result(self):
        return (self.app.cmd_type, self.app.cmd, self.result)

def run_client(client_func=None, app_factory=None):
    application = app_factory()
    s = ServerRunner(application)
    r = ClientRunner(application, client_func)
    r.run()
    s.run()
    return r.get_result()

def run_query(sql):
    import pymysql
    con = pymysql.connect(host="127.0.0.1", user="admin", passwd="", port=3307, db="test")
    try:
        c = con.cursor()
        c.execute(sql)
        r = c.fetchone()
        return r
    finally:
        con.close()

class BaseApp(object):
    
    cmd_type = None
    cmd = None

    def __call__(self, cmd_type, cmd, start_result):

        self.cmd_type = cmd_type
        self.cmd = cmd

        if cmd_type == dismage.COMMAND_INIT_DB or "COMMIT" in cmd:
            return
        columns = [(dismage.COLUMN_TYPE_VARCHAR, 10, 'name'), (dismage.COLUMN_TYPE_INT24, 8, 'age')]
        start_result(columns=columns)
        return [('john', 30), ('smith', 28)]



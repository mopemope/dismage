import dimage
from dimage import patch
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

        dismage.listen()
        self.running = True
        if shutdown:
            dismage.schedule_call(1, dismage.shutdown, 1)
        dimage.run(app)

class ClientRunner(object):


    def __init__(self, app, func, shutdown=True):
        self.func = func
        self.app = app
        self.shutdown = shutdown

    def run(self):

        def _call():
            try:
                r = self.func()
                self.receive_data = r
            finally:
                if self.shutdown:
                    dismage.shutdown(1)

        dimage.spawn(_call)

    def get_result(self):
        return

def run_client(client_func=None, app_factory=None):
    application = app_factory()
    s = ServerRunner(application)
    r = ClientRunner(application, client_func)
    r.run()
    s.run()
    return


from dismage import *
from multiprocessing import Process
import signal

workers = []

def app(cmd, cmd_data, start_result):

    print("cmd:%s data:%s start_response:%s" % (cmd, cmd_data, start_result))
    if "COMMIT" in cmd_data:
        return
    #column = (type, size, name)
    columns = [(COLUMN_TYPE_VARCHAR, 10, 'name'), (COLUMN_TYPE_INT24, 8, 'age')]
    # print(columns)
    start_result(columns=columns)
    return [('john', 30), ('smith', 28)]

def kill_all(sig, st):
    for w in workers:
        w.terminate()

def start(num=4):
    for i in range(num):
        p = Process(name="worker-%d" % i, target=run, args=(app,))
        workers.append(p)
        p.start()

signal.signal(signal.SIGTERM, kill_all)
listen(port=3307)
start()



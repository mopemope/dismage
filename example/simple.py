from dismage import *
from dismage import patch
patch.patch_all()

workers = []

def app(cmd, cmd_data, start_result):

    print("cmd:%s data:%s start_response:%s" % (cmd, cmd_data, start_result))
    if cmd == COMMAND_INIT_DB or "COMMIT" in cmd_data:
        return
    #column = (type, size, name)
    columns = [(COLUMN_TYPE_VARCHAR, 10, 'name'), (COLUMN_TYPE_INT24, 8, 'age')]
    # print(columns)
    start_result(columns=columns)
    return [('john', 30), ('smith', 28)]

listen(port=3307)
run(app)



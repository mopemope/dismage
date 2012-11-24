from dismage import *

def r(cmd, cmd_data, start_result):

    print("cmd:%s data:%s start_response:%s" % (cmd, cmd_data, start_result))
    if "COMMIT" in cmd_data:
        return
    #column = (type, size, name)
    columns = [(COLUMN_TYPE_VARCHAR, 10, 'name'), (COLUMN_TYPE_INT24, 8, 'age')]
    start_result(columns=columns)
    return [('john', 30), ('smith', 28)]

print(dir())
listen(port=3307)
run(r)



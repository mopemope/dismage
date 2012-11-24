Dismage
----------------------------------------

Dismage は MySQL プロトコルサーバー です。

wsgi like な インターフェイスを持ち、簡単にMySQL プロトコルサーバーを書く事ができます。


Basic Usage
-----------------------------------------

simple app::

    import dismage

    def simple_handler(cmd, cmd_data, start_result):

        print("cmd:%s data:%s start_response:%s" % (cmd, cmd_data, start_result))
        if "COMMIT" in cmd_data:
            return
        #column = (type, size, name)
        columns = [(dismage.COLUMN_TYPE_VARCHAR, 10, 'name'), (dismage.COLUMN_TYPE_INT24, 8, 'age')]
        #print(columns)
        start_result(columns=columns)
        return [('john', 30), ('smith', 28)]

    dismage.listen(port=3307)
    dismage.run(simple_handler)


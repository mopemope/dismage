from base import *
import dismage

class App(BaseApp):
    pass
    

def test_simple():
    
    query = "SELECT * FROM test"

    def client():

        return run_query(query)
    
    cmd_type, cmd, q  = run_client(client, App)
    assert(cmd_type == dismage.COMMAND_DRIZZLE_QUERY)
    assert(cmd == query)



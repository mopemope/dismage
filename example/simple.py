from dismage import *

def r(*args, **kwargs):
    print(args)

listen(port=3307)
run(r)



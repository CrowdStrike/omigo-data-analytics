def myfunc_inner_args(x):
    def __myfunc_inner__(y):
        return "Hello World: {}, {}".format(x, y)

    return __myfunc_inner__

def myfunc_inner_kwargs(x = 0):
    def __myfunc_inner__(y):
        return "Hello World: {}, {}".format(x, y)

    return __myfunc_inner__

def myfunc_inner_args_kwargs(x, z = 0):
    def __myfunc_inner__(y):
        return "Hello World: {}, {}, {}".format(x, y, z)

    return __myfunc_inner__

def myfunc_inner_noargs():
    def __myfunc_inner__(y):
        return "Hello World: {}".format(y)

    return __myfunc_inner__

def myfunc_noargs():
   return "Hello World"

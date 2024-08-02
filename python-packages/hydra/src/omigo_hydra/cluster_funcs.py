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

# Enforcing module/class/func structure
class ClusterBuiltinFuncs:
    def builtin_sum(arr):
        return sum(arr)
    
    def builtin_abs(x):
        return abs(x)
    
    def builtin_len(x):
        return len(x)
    
    def builtin_min(x, y):
        return min(x, y)
    
    def builtin_max(x, y):
        return max(x, y)
    
    def builtin_set(x, y):
        return set(x, y)
    
    def builtin_ceil(x):
        return math.ceil(x)
    
    def builtin_floor(x):
        return math.floor(x)

def __step6_explode_func__(mp):
    results = []
    results.append({"label": "unresolved", "count": str(int(mp["total_hosts"]) - int(mp["resolved_hosts"]))})
    results.append({"label": "resolved", "count": mp["resolved_hosts"]})
    return results


import inspect
import importlib
import math
from omigo_core import tsv, utils

# TODO: Currying
# https://stackoverflow.com/questions/3589311/get-defining-class-of-unbound-method-object-in-python-3/25959545#25959545
def get_class_that_defined_method(method):
    # check if it is a method
    if inspect.ismethod(method):
        for cls in inspect.getmro(method.__self__.__class__):
            if method.__name__ in cls.__dict__:
                return cls

    # check if it is a function
    if inspect.isfunction(method):
        return getattr(inspect.getmodule(method), method.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0], None)

    raise Exception("get_class_that_defined_method: not able to find the class: {}".format(method)) 

# TODO: these methods are hacky version of class loading and referencing
def get_fully_qualified_class_name(clsref):
    # validation
    if (clsref is None):
        raise Exception("get_fully_qualified_class_name : clsref is None")

    # get function qualified name
    class_name = func.__name__

    # get the module name
    module_name = func.__module__

    # generate fully qualified name
    return "{}.{}".format(module_name, class_name)

# TODO: these methods are hacky version of class loading and referencing
def get_fully_qualified_name(func):
    # validation
    if (func is None):
        raise Exception("get_fully_qualified_name: func is None")

    # get function qualified name
    func_name = func.__qualname__

    # get the module name
    module_name = func.__module__

    # generate fully qualified name
    return "{}.{}".format(module_name, func_name)

# TODO: these methods are hacky version of class loading and referencing
def load_fully_qualified_func(fully_qual_func_name):
    # only omigo extensions are supported. TODO
    if (fully_qual_func_name.startswith("omigo_") == False):
        utils.debug("Non omigo extensions will not be supported in future: {}".format(fully_qual_func_name))

    # split into parts
    parts = fully_qual_func_name.split(".")

    # need fully qualified name
    if (len(parts) < 2):
        raise Exception("Not able to parse the function full qualified name: {}".format(fully_qual_func_name))
    
    # check for package or class based function hierarchy
    if (len(parts) == 2):
        # this is assumed to be package functions. TODO 
        utils.debug("Global functions may not be supported in future. Use their cluster_funcs counterparts")

        # assume its package.func_name
        module_name = parts[0]
        func_name = parts[1]

        # load method
        utils.debug("load_fully_qualified_func: {}, func_name: {}, module_name: {}".format(fully_qual_func_name, func_name, module_name))

        # check loading of the class 
        try:
            # load module
            m = importlib.import_module(module_name)

            # check for function
            return getattr(m, func_name)
        except Exception as e:
            utils.error("function not found: {}".format(fully_qual_func_name))
            raise e 
    else:
        # split function name from module name
        func_name = parts[-1]
        class_name = parts[-2]
        module_name = ".".join(parts[0:-2])

        # load method
        utils.debug("load_fully_qualified_func: {}, func_name: {}, class_name: {}, module_name: {}".format(fully_qual_func_name, func_name, class_name, module_name))

        # check loading of the class 
        try:
            # load module
            m = importlib.import_module(module_name)
            if (class_name in m.__dict__.keys()):
                # load class
                class_reference = m.__dict__[class_name]

                # check for function
                return getattr(class_reference, func_name)
            else:
                raise Exception("class name not found in the module: {}, class: {}".format(module_name, class_name)) 
        except Exception as e:
            utils.error("function not found: {}".format(fully_qual_func_name))
            raise e 

# TODO: these methods are hacky version of class loading and referencing
def verify_tsv_subclass_func(fully_qual_func_name):
    try:
        f = load_fully_qualified_func(fully_qual_func_name)
        # check if f is a function
        if (inspect.ismethod(f) or inspect.isfunction(f)):
            # get the class reference
            class_reference = get_class_that_defined_method(f)
            utils.debug("class for function: {}, {}".format(fully_qual_func_name, class_reference))

            # check if class is of TSV type
            if (class_reference == tsv.TSV):
                return True
            else:
                # check if bases classes 
                # check for base classes
                tsv_base_classes = list(filter(lambda t: t == tsv.TSV, class_reference.__bases__))
                if (len(tsv_base_classes) > 0):
                    return True
                else:
                    utils.error("verify_tsv_subclass_func: failed. fully_qual_func_name is not inheriting from TSV class: {}".format(fully_qual_func_name))
                    return False
        else:
            utils.error("verify_tsv_subclass_func: failed. fully_qual_func_name is not function: {}".format(fully_qual_func_name))
            return False
    except Exception as e:
        utils.error("verify_tsv_subclass_func: failed: {}".format(e))
        return False

# >>> type(lambda x: x+2)
# <class 'function'>
# >>> cluster_class_reflection.get_fully_qualified_name(lambda x: x+2)
# '__main__.<lambda>'
def is_lambda_func(func):
    # check type
    if (isinstance(func, (type(is_lambda_func))) and get_fully_qualified_name(func).endswith(".<lambda>")):
        return True
    else:
        return False

# check for builtin functions that dont have __dict__
# >>> cluster_class_reflection.get_fully_qualified_name(sum)
# 'builtins.sum'
def is_builtin_func(func):
    # check type
    if (type(func) in (type(sum), type(math.ceil))):
        return True
    else:
        return False

# TODO: return True
def has_valid_reflective_name(func):
    # if (get_fully_qualified_name(func).startswith("omigo_") == True):
    #     return True
    # else:
    #     return False
    return True


# get the hydra version of the class extension
def get_hydra_class(class_ref, fallback_modules = None):
    # get the module and class name
    module = inspect.getmodule(class_ref)
    module_name = module.__name__
    class_name = class_ref.__name__

    # construct a new class name
    hydra_class_name = "Hydra{}".format(class_name)

    # trace
    utils.trace("get_hydra_class: class_name: {}, module_name: {}, hydra_class_name: {}".format(class_name, module_name, hydra_class_name))

    # check if the existing module has the hydra class present
    for member in inspect.getmembers(module):
        # the member is a tuple (name, reference)
        utils.trace("get_hydra_class: member: {}".format(member[0]))

        # compare
        if (member[0] == hydra_class_name):
            # found the hydra class
            utils.info_once("get_hydra_class: found hydra version for class: {}: {}.{}".format(class_ref.__name__, module_name, member[0]))
            return member[1]

    # look into fallback_module if that is defined
    if (fallback_modules is not None):
        # iterate through all fallback modules
        for fallback_module in fallback_modules:
            # the hydra class doesnt exist in primary location. check cluster_tsv
            for member in inspect.getmembers(fallback_module):
                # the member is a tuple (name, reference)
                if (member[0] == hydra_class_name):
                    # found the hydra class
                    utils.info_once("get_hydra_class: found hydra version for class: {}: {}.{}".format(class_ref.__name__, fallback_module.__name__, member[0]))
                    return member[1]

    # raise exception that no hydra version found
    utils.warn("get_hydra_class: Not able to find the hydra version for class: {}".format(class_ref.__name__))
    return None

def load_class_ref_by_name(class_name):
    # split
    parts = class_name.split(".")

    # validation
    if (len(parts) < 2):
        raise Exception("Invalid class name: {}".format(class_name))

    # get module and class base name
    module_name = ".".join(parts[0:-1])
    class_base_name = parts[-1]

    # load module
    module = importlib.import_module(module_name)
    class_ref = getattr(module, class_base_name)

    # return
    return class_ref

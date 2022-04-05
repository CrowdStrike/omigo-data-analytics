import inspect
import importlib
from omigo_core import tsv
from omigo_core import utils 

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
def get_fully_qualified_name(func):
    # validation
    if (func is None):
        raise Exception("get_full_qualified_name: func is None")

    # get function qualified name
    func_name = func.__qualname__

    # get the module name
    module_name = func.__module__

    # generate fully qualified name
    return "{}.{}".format(module_name, func_name)

# TODO: these methods are hacky version of class loading and referencing
def load_fully_qualified_func(fully_qual_func_name):
    # only omigo extensions are supported
    if (fully_qual_func_name.startswith("omigo_") == False):
        raise Exception("Non omigo extensions are not supported: {}".format(fully_qual_func_name))

    # split into parts
    parts = fully_qual_func_name.split(".")

    # need fully qualified name
    if (len(parts) < 3):
        raise Exception("Global functions are not supported: {}".format(fully_qual_func_name))

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


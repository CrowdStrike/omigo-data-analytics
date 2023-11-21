from omigo_core import tsv
from omigo_core import utils
import base64
import dill
import json

# class to serialize all the data values as json
# TBD: use json.loads properly 
class JsonSer:
    def to_json(self, transient_keys = []):
        # create map
        mp = {}

        # take everything except the excluded one. if anything is JsonSer, call to_json() method
        for k in self.__dict__.keys():
            if (k not in transient_keys):
                if (isinstance(self.__dict__[k], JsonSer)):
                    mp[k] = self.__dict__[k].to_json()
                elif (isinstance(self.__dict__[k], list)):
                    if (len(self.__dict__[k]) > 0):
                        # check if all array elements are part of JsonSer
                        if (isinstance(self.__dict__[k][0], JsonSer)):
                            result = []
                            # call to_json()
                            for t in self.__dict__[k]:
                                result.append(t.to_json())
                            mp[k] = result
                        else:
                            mp[k] = self.__dict__[k]
                    else:
                        mp[k] = self.__dict__[k]
                else:
                    mp[k] = self.__dict__[k]

        # call the generic json converter
        return json.loads(json.dumps(mp, default = lambda o: o.__dict__))

    # pretty print json
    def pretty_print(self):
        print(json.dumps(self.to_json(), indent = 4, sort_keys = True))

# This extension is to call apis in cluster mode
class ClusterBaseOperand(JsonSer):
    def __init__(self, class_name):
        self.class_name = class_name
    
    def validate(self):
        raise Exception("This is not implemented for base class") 
        
# Cluster Operand class
class ClusterOperand(ClusterBaseOperand):
    def __init__(self, data_type, value):
        super().__init__("ClusterOperand")

        # initialize the variables
        self.data_type = data_type
        self.value = value

        # TODO: this is not working with derived class. validation        
        # if (self.validate() == False):
        #     raise Exception("Wrong data type used: {}, expected: {}".format(type(self.value), self.data_type))
    
# Cluster Bool class 
class ClusterBool(ClusterOperand):
    def __init__(self, value):
        super().__init__("bool", value)
        
    def validate(self):
        return isinstance(self.value, bool)
        
# Cluster String class 
class ClusterStr(ClusterOperand):
    def __init__(self, value):
        super().__init__("str", value)
        
    def validate(self):
        return isinstance(self.value, str)
        
# Cluster Int class 
class ClusterInt(ClusterOperand):
    def __init__(self, value):
        super().__init__("int", value)
        
    def validate(self):
        return isinstance(self.value, int)
    
# Cluster Float class 
class ClusterFloat(ClusterOperand):
    def __init__(self, value):
        super().__init__("float", value)
        
    def validate(self):
        return isinstance(self.value, (float, int))

# Cluster Array class 
class ClusterArrayBaseType(ClusterOperand):
    def __init__(self, data_type, value):
        super().__init__(data_type, value)

    # validation for array type 
    def validate_array(self, allowed_data_types):
        # check if none
        if (self.value is None):
            return True
        
        # this must be array
        if (isinstance(self.value, (list, tuple)) == False):
            return False
        
        # all values within array should be of expected type
        for x in self.value:
            if (x is not None and type(x) not in allowed_data_types):
                return False
           
        # return 
        return True
        
# Cluster Array of Bool class 
class ClusterArrayEmpty(ClusterArrayBaseType):
    def __init__(self, value):
        super().__init__("array_empty", value)

    def validate(self):
        return len(self.value) == 0 

# Cluster Array of Bool class 
class ClusterArrayBool(ClusterArrayBaseType):
    def __init__(self, value):
        super().__init__("array_bool", value)
        raise Exception("Deprecated")

    def validate(self):
        self.validate_array([bool])
        
# Cluster Array of Stringclass 
class ClusterArrayStr(ClusterArrayBaseType):
    def __init__(self, value):
        super().__init__("array_str", value)
        raise Exception("Deprecated")

    def validate(self):
        self.validate_array([str])
        
# Cluster Array of Int class 
class ClusterArrayInt(ClusterArrayBaseType):
    def __init__(self, value):
        super().__init__("array_int", value)
        raise Exception("Deprecated")

    def validate(self):
        self.validate_array([int])

# Cluster Array of Float class. TODO: int is also float which is little hacky here 
class ClusterArrayFloat(ClusterArrayBaseType):
    def __init__(self, value):
        super().__init__("array_float", value)
        raise Exception("Deprecated")

    def validate(self):
        self.validate_array([float, int])
        
# Cluster Dict class 
class ClusterDict(ClusterOperand):
    def __init__(self, value):
        super().__init__("dict", value)

    def validate(self):
        # check for None
        if (self.value is None):
            return True
        
        return isinstance(self.value, dict)
    
# Cluster Object class 
class ClusterObject(ClusterOperand):
    def __init__(self, value):
        super().__init__("object", value)

    def validate(self):
        # check for None
        if (self.value is None):
            return True
        
        return isinstance(self.value, ClusterOperand)    

# Cluster PyObject class 
class ClusterPyObject(ClusterOperand):
    def __init__(self, value):
        super().__init__("pyobject", base64.b64encode(dill.dumps(value)).decode("ascii"))

    def validate(self):
        # check for None
        if (self.value is None):
            return True
        
        return isinstance(self.value, ClusterOperand)    

# Cluster Array of Object class 
class ClusterArrayObject(ClusterArrayBaseType):
    def __init__(self, value):
        super().__init__("array_object", value)

    def validate(self):
        self.validate_array((ClusterOperand))

# Cluster Array of PyObject class 
class ClusterArrayPyObject(ClusterArrayBaseType):
    def __init__(self, value):
        super().__init__("array_pyobject", value)

    def validate(self):
        self.validate_array((ClusterOperand))

# Cluster Function class
class ClusterFunc(ClusterBaseOperand):
    def __init__(self, name, func_type, value):
        super().__init__("ClusterFunc")
        self.name = name 
        self.func_type = func_type
        self.value = value
        self.data_type = "function"

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterFunc(
            json_obj["name"],
            json_obj["func_type"],
            json_obj["value"],
            json_obj["data_type"])

# TODO: remove array_* and keep only array_object
def load_native_objects(cluster_operand):
    # read data type and value
    data_type = cluster_operand.data_type
    value = cluster_operand.value

    # check for data type and deserialize
    if (data_type == "bool"):
        return bool(value)
    elif (data_type == "str"):
        return str(value)
    elif (data_type == "int"):
        return int(value)
    elif (data_type == "float"):
        return float(value)
    elif (data_type == "object"):
        return load_native_objects(value)
    elif (data_type == "pyobject"):
        return dill.loads(base64.b64decode(value.encode("ascii")))
    # elif (data_type == "array_bool"):
    #     return list([bool(x) for x in value])
    # elif (data_type == "array_str"):
    #     return list([str(x) for x in value])
    # elif (data_type == "array_int"):
    #     return list([int(x) for x in value])
    # elif (data_type == "arrary_float"):
    #     return list([float(x) for x in value])
    elif (data_type == "array_object"):
        return list([load_native_objects(x) for x in value])
    elif (data_type == "array_pyobject"):
        return list([load_native_objects(x) for x in value])
    elif (data_type == "dict"):
        return dict([(k, load_native_objects(value[k])) for k in value.keys()])
    elif (data_type == "function"):
        return load_native_cluster_func(cluster_operand)
    else:
        raise Exception("load_native_objects: unknown data type: {}".format(data_type)) 

def load_native_cluster_func(cluster_func):
    # check for possible values of func_type
    func_type = cluster_func.func_type
    value = cluster_func.value 

    # check for different function types
    if (func_type == "lambda"):
        # decode and call the lambda function
        return dill.loads(base64.b64decode(value.encode("ascii")))
    elif (func_type == "library"):
        # read function name. TODO: this is supposed to be string
        func_name = value["name"]
        func = cluster_class_reflection.load_fully_qualified_func(func_name)

        # read the serialized argument strings
        args = value["args"]
        kwargs = value["kwargs"]

        # parse the strings if they were not empty
        func_args = load_native_objects(args)
        func_kwargs = load_native_objects(kwargs)

        # lookup function by name
        # func = getattr(cluster_funcs, func_name)
        cluster_class_reflection.load_fully_qualified_func(func_name)

        # based on what set of attributes are defined, call the function
        if ((func_args is not None and len(func_args) > 0) and (func_kwargs is not None and len(func_kwargs) > 0)):
            return func(*func_args, **func_kwargs)
        elif ((func_args is not None and len(func_args) > 0) and (func_kwargs is None or len(func_kwargs) == 0)):
            return func(*func_args)
        elif ((func_args is None or len(func_args) == 0) and (func_kwargs is not None and len(func_kwargs) > 0)):
            return func(**func_kwargs)
        else:
            return func
    elif (func_type == "javascript"):
        raise Exception("Need library support to load java script function as executable code")
    else:
        raise Exception("Unknown function type: {}".format(func_type))

# Cluster Function for lambda class
class ClusterFuncLambda(ClusterFunc):
    def __init__(self, func):
        super().__init__("", "lambda", base64.b64encode(dill.dumps(func)).decode("ascii"))

    def validate(self):
        raise Exception("TBD")

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        value = json_obj["value"]
        try:
            func = dill.loads(base64.b64decode(value.encode("ascii"))) 
            return ClusterFuncLambda(func)
        except Exception as e:
            utils.info("ClusterFuncLambda: from_json: exception in deserializing json: {}".format(json.dumps(json_obj)))
            raise e 
       
# TODO: This dill serializer for args and kwargs need to be changed. 
# TODO: the base class parameters are not serialized
# TODO: the args and kwargs need to be empty. Use new() methods
# TODO: the args and kwargs are serialized cluster object
# Cluster Function for library functions class
class ClusterFuncLibrary(ClusterFunc):
    def __init__(self, name, *args, **kwargs):
        super().__init__(name, "library", None)

        # convert the arbitary args and kwargs into ClusterOperands
        jargs = []
        for x in args:
            jargs.append(cluster_operand_serializer(x))

        # convert the arbitary args and kwargs into ClusterOperands
        jkwargs = {}
        for k in kwargs.keys():
            jkwargs[k] = cluster_operand_serializer(kwargs[k])

        # store in the class
        self.value = {
            "name": self.name, 
            "args": ClusterArrayObject(jargs),
            "kwargs": ClusterDict(jkwargs)
        }

    def validate(self):
        raise Exception("TBD")

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        # fetch the value part
        value = json_obj["value"]

        # take function parameters
        name = value["name"]

        # read the serialized argument strings
        args = cluster_operand_deserializer(value["args"])
        kwargs = cluster_operand_deserializer(value["kwargs"])

        # parse the strings if they were not empty
        func_args = load_native_objects(args)
        func_kwargs = load_native_objects(kwargs)
 
        # based on what set of attributes are defined, call the function
        if (func_args is not None and func_kwargs is not None):
            return ClusterFuncLibrary(name, *func_args, **func_kwargs)
        elif (func_args is not None and func_kwargs is None):
            return ClusterFuncLibrary(name, *func_args)
        elif (func_args is None and func_kwargs is not None):
            return ClusterFuncLibrary(name, **func_kwargs)
        else:
            return ClusterFuncLibrary(name)
    
# Cluster Function for javascript functions class
class ClusterFuncJS(ClusterFunc):
    def __init__(self, js_func):
        super().__init__("", "javascript", js_func)

    def validate(self):
        raise Exception("TBD") 

    def from_json(json_obj):
        # check for None
        if (json_obj is None):
            return None

        return ClusterFuncJS(json_obj["value"])
        
def __get_data_types__(arr):
    if (arr is None):
        return []
    
    mp = {}
    for x in arr:
        mp[x.data_type] = 1
        
    return list(set(mp.keys()))
   
# Cluster Array of functions  class
class ClusterArrayFunction(ClusterFunc):
    def __init__(self, value):
        super().__init__("", "array_function", value)
        raise Exception("Deprecated")

    # check if array
    def validate(self):
        # check for None
        if (self.value is None):
            return True

        # should be array
        if (isinstance(self.value, (list, tuple)) == False):
            return False

        # each value should be ClusterFunc type
        for x in self.value:
            if (x is not None and isinstance(x, ClusterFunc) == False):
                return False

        # return
        return True

# helper method to map cluster library function to TSV class 
def map_to_cluster_func(func):
    # print(type(func))
    cluster_func = None
    if (isinstance(func, (type(tsv.__init__), type(tsv.enable_debug_mode)))):
        # check the fully qualified name
        fqn = cluster_class_reflection.get_fully_qualified_name(func)

        # check if it is lambda
        if (cluster_class_reflection.is_lambda_func(func)):
            cluster_func = ClusterFuncLambda(func)
        else:
            cluster_func = ClusterFuncLibrary(cluster_class_reflection.get_fully_qualified_name(func))
    elif (cluster_class_reflection.is_builtin_func(func)):
        # check if the function has valid name
        if (cluster_class_reflection.has_valid_reflective_name(func)):
            cluster_func = ClusterFuncLibrary(cluster_class_reflection.get_fully_qualified_name(func))
        else:
            raise Exception("Please use functions using fully qualified names under omigo: {}".format(cluster_class_reflection.get_fully_qualified_name(func)))
    else:
        raise Exception("Unknown function type: {}".format(func))
        
    return cluster_func       

def cluster_operand_serializer(value):
    # check for None
    if (value is None):
        return None
    
    # check for bool
    if (isinstance(value, bool)):
        return ClusterBool(value)
    
    # check for str
    if (isinstance(value, str)):
        return ClusterStr(value)
    
    # check for int
    if (isinstance(value, int)):
        return ClusterInt(value)
    
    # check for float
    if (isinstance(value, float)):
        return ClusterFloat(value)
    
    # check for ClusterOperand. TODO
    if (isinstance(value, ClusterOperand)):
        utils.warn("This logic for ClusterOperand is not clear")
        return ClusterObject(value)
    
    # check for array 
    if (isinstance(value, (list, tuple))):
        # determine data type if there are any values
        if (len(value) > 0):
            arr_values = []
            for x in value:
                arr_values.append(cluster_operand_serializer(x))
    
            # get data_types from the array. Use arr_values for consistency
            # TODO: this is confusing. till what level the serialization should be done 
            data_types = __get_data_types__(arr_values)
            # if (len(data_types) == 1):
            #     if (data_types[0] == "bool"):
            #         return ClusterArrayBool(value)
            #     elif (data_types[0] == "str"):
            #         return ClusterArrayStr(value)
            #     elif (data_types[0] == "int"):
            #         return ClusterArrayInt(value)
            #     elif (data_types[0] == "float"):
            #         return ClusterArrayFloat(value)
            #     elif (data_types[0] == "object"):
            #         return ClusterArrayObject(arr_values)
            #     elif (data_types[0] == "pyobject"):
            #         return ClusterArrayPyObject(arr_values)
            #     elif (data_types[0] == "function"):
            #         return ClusterArrayFunction(arr_values)
            #     else:
            #         raise Exception("Invalid data types: {}".format(data_types))
            # arrays can be of completely heteregenous types
            # elif (len(data_types) == 2):
            #     if ("int" in data_types and "float" in data_types):
            #         return ClusterArrayFloat(arr_values)
            #     else:
            #         raise Exception("Invalid data types: {}".format(data_types))
            # else:
            #    return ClusterArrayObject(arr_values)
            return ClusterArrayObject(arr_values)
        else:
            return ClusterArrayEmpty(value)

    # check for dictionary 
    if (isinstance(value, dict)):
        return ClusterDict(dict([(k, cluster_operand_serializer(value[k])) for k in value.keys()]))
 
    # builtin methods dont have __dict__. Use them through cluster_funcs package
    if (cluster_class_reflection.is_builtin_func(value)):
        return map_to_cluster_func(value)
    
    # check for functions
    if (isinstance(value, (type(tsv.enable_debug_mode), type(tsv.__init__), type(cluster_operand_serializer)))):
        return map_to_cluster_func(value)

    # functions already serialized are return as such. TODO
    if (isinstance(value, (ClusterFunc, ClusterFuncLambda, ClusterFuncLibrary, ClusterFuncJS))):
        return value
            
    raise Exception("Unknown data type: {}".format(type(value)))

def cluster_operand_deserializer(obj):
    # check for none
    if (obj is None):
        return None
   
    # create a map and check if string needs to be parsed or map.
    # TODO: this is because value can be both string or a map 
    mp = None
    if (isinstance(obj, str)):
        mp = json.loads(obj)
    elif (isinstance(obj, dict)):
        mp = obj
    else:
        raise Exception("Not able to deserialize: {}, {}".format(type(obj), obj))
   
    # check for class name  
    if ("class_name" not in mp.keys()):
        raise Exception("Invalid json string to deserialize: {}".format(mp))
        
    # check the class names are of valid types
    if (mp["class_name"] not in ("ClusterOperand", "ClusterFunc")):
        raise Exception("Invalid class to deserialize: {}".format(mp["class_name"]))
     
    # TODO: This is accessing data_type without proper base class 
    data_type = mp["data_type"]
    value = mp["value"]

    # check for all possible data types
    if (data_type == "object"):
        return ClusterObject(cluster_operand_deserializer(value))
    if (data_type == "pyobject"):
        return ClusterPyObject(cluster_operand_deserializer(value))
    elif (data_type == "bool"):
        return ClusterBool(bool(value))
    elif (data_type == "str"):
        return ClusterStr(str(value))
    elif (data_type == "int"):
        return ClusterInt(int(value))
    elif (data_type == "float"):
        return ClusterFloat(float(value))
    # elif (data_type == "array_bool"):
    #     result = []
    #     for x in value:
    #         result.append(bool(x))
    #     return ClusterArrayBool(result)
    # elif (data_type == "array_str"):
    #     result = []
    #     for x in value:
    #         result.append(str(x))
    #     return ClusterArrayStr(result)
    # elif (data_type == "array_int"):
    #     result = []
    #     for x in value:
    #         result.append(int(x))
    #     return ClusterArrayInt(result)
    # elif (data_type == "array_float"):
    #     result = []
    #     for x in value:
    #         result.append(float(x))
    #     return ClusterArrayFloat(result)
    elif (data_type == "array_object"):
        result = []
        for x in value:
            result.append(cluster_operand_deserializer(x))
        return ClusterArrayObject(result)
    elif (data_type == "array_pyobject"):
        result = []
        for x in value:
            result.append(cluster_operand_deserializer(x))
        return ClusterArrayPyObject(result)
    elif (data_type == "dict"):
        return ClusterDict(dict([(k, cluster_operand_deserializer(value[k])) for k in value.keys()]))
    elif (data_type == "function"):
        func_type = mp["func_type"]
        if (func_type == "lambda"):
            return ClusterFuncLambda.from_json(mp)
        elif (func_type == "library"):
            return ClusterFuncLibrary.from_json(mp)
        elif (func_type == "javascript"):
            return ClusterFuncJS.from_json(mp)
        else:
            raise Exception("Unable to deserialize: {}".format(obj))
    else:
        raise Exception("Unable to deserialize: {}".format(obj))


package omigo_ext.hydra;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import java.util.*;

abstract class JsonSer {
    public JSONObject toJson() {
        List<String> transientKeys = null;
        JSONObject jsonObj = new JSONObject();
        for(String key: this.getObjectKeys()) {
            // System.out.println("JsonSer: toJson: key: " + key);
            if (transientKeys != null && transientKeys.contains(key) == true) {
                continue;
            }

            Object value = this.getObjectValue(key);
            // System.out.println("JsonSer: toJson: value: " + value.getClass().getName() + " : " + value);
            if (value instanceof JsonSer) {
                JsonSer jsonSerValue = (JsonSer) value;
                jsonObj.put(key, jsonSerValue.toJson());
                // System.out.println("JsonSer: toJson: jsonSerValue: " + jsonObj);
            } else if (value instanceof List) {
                List listValue = (List) value;
                if (listValue.size() > 0) {
                    Object value0 = listValue.get(0);
                    if (value0 instanceof JsonSer) {
                        JsonSer jsonSerValue0 = (JsonSer) value0;
                        if (jsonSerValue0 != null) {
                            List<JSONObject> jsonSerList = new ArrayList<JSONObject>();
                            for (Object v: listValue) {
                                JsonSer vJsonSer = (JsonSer) v;
                                jsonSerList.add(vJsonSer.toJson());
                            }
                            jsonObj.put(key, new JSONArray(jsonSerList));
                            // System.out.println("JsonSer: toJson: listValue: " + jsonObj);
                        }
                    } else {
                        jsonObj.put(key, value);
                    }
                } else {
                    // TODO: What is this
                    jsonObj.put(key, value);
                    // System.out.println("JsonSer: toJson: 1: " + jsonObj);
                }
            // TODO: This is broken in python
            } else if (value instanceof Map) {
                Map<String, Object> mp = (Map<String, Object>) value;
                for (Map.Entry<String, Object> entry: mp.entrySet()) {
                    if (entry.getValue() instanceof JsonSer) {
                        JsonSer jsonSerValue = (JsonSer) entry.getValue();
                        jsonObj.put(entry.getKey(), jsonSerValue.toJson());
                    } else {
                        jsonObj.put(entry.getKey(), entry.getValue());
                    }
                }
            } else {
                // TODO: What is this
                jsonObj.put(key, value);
                // System.out.println("JsonSer: toJson: 2: " + jsonObj);
            }
        }

        return jsonObj;
    }

    abstract List<String> getObjectKeys();
    abstract Object getObjectValue(String key);
    abstract void putObject(String key, Object value);
}

abstract class ClusterBaseOperand extends JsonSer {
    public Map<String, Object> objectMap;

    public ClusterBaseOperand(String className) {
        this.objectMap = new HashMap<String, Object>();
        this.putObject("class_name", className);
    }

    public String getClassName() {
        return (String) this.getObjectValue("class_name");
    }

    // public List<String> getObjectKeys() {
    //     return Arrays.asList(new String[]{"class_name"});
    // }

    public Object getObjectValue(String key) {
        return this.objectMap.get(key);
    }

    public void putObject(String key, Object value) {
        this.objectMap.put(key,  value);
    }

    public abstract void validate();
}

abstract class ClusterOperand extends ClusterBaseOperand {
    public ClusterOperand(String dataType, Object value) {
        super("ClusterOperand");

        this.putObject("data_type", dataType);
        this.putObject("value", value);
    }

    public List<String> getObjectKeys() {
        return Arrays.asList(new String[]{"class_name", "data_type", "value"});
    }

    public String getDataType() {
        return (String) this.getObjectValue("data_type");
    } 
}

class ClusterBool extends ClusterOperand {
    public ClusterBool(Boolean value) {
        super("bool", value);
    }

    public void validate() {
        if (this.getObjectValue("value") instanceof Boolean == false) {
            throw new RuntimeException("Validation Failed");
        }
    }
}

class ClusterStr extends ClusterOperand {
    public ClusterStr(String value) {
        super("str", value);
    }

    public void validate() {
        if (this.getObjectValue("value") instanceof String == false) {
            throw new RuntimeException("Validation Failed");
        }
    }
}

class ClusterInt extends ClusterOperand {
    public ClusterInt(Integer value) {
        super("int", value);
    }

    public void validate() {
        if (this.getObjectValue("value") instanceof Integer == false) {
            throw new RuntimeException("Validation Failed");
        }
    }
}

class ClusterFloat extends ClusterOperand {
    public ClusterFloat(Double value) {
        super("float", value);
    }

    public void validate() {
        if (this.getObjectValue("value") instanceof Double == false) {
            throw new RuntimeException("Validation Failed");
        }
    }
}

// Arrays are implemented as List
abstract class ClusterArrayBaseType extends ClusterOperand {
    public ClusterArrayBaseType(String dataType, Object value) {
        super(dataType, value);
    }

    public void validateArray(String allowedDataTypes[]) {
        if (this.getObjectValue("value") == null) {
            return; 
        }

        if (this.getObjectValue("value") instanceof List == false) {
            throw new RuntimeException("Validation Failed");
        }

        for (Object elt: (List) this.getObjectValue("value")) {
            if (elt != null) {
                boolean flag = false; 
                for (String dt: allowedDataTypes) {
                      if (elt.getClass().getName().equals(dt)) {
                          flag = true;
                          break;
                      }
                }
                if (flag == false) {
                    throw new RuntimeException("Validation Failed");
                }
            }
        }
    }
}

class ClusterArrayEmpty extends ClusterArrayBaseType {
    public ClusterArrayEmpty(Object value) {
        super("array_empty", value);
    }

    public void validate() {
        Object value = this.getObjectValue("value");
        if (value != null) {
            List<Object> listValue = (List<Object>) value;
            if (listValue.isEmpty() == false) {
                throw new RuntimeException("Validation Failed");
            }
        }
    }
}

class ClusterArrayBool extends ClusterArrayBaseType {
    public ClusterArrayBool(Object value) {
        super("array_bool", value);
    }

    public void validate() {
        super.validateArray(new String[] { "java.lang.Boolean" });
    }
}

class ClusterArrayStr extends ClusterArrayBaseType {
    public ClusterArrayStr(Object value) {
        super("array_str", value);
    }

    public void validate() {
        super.validateArray(new String[] { "java.lang.String" });
    }
}

class ClusterArrayInt extends ClusterArrayBaseType {
    public ClusterArrayInt(Object value) {
        super("array_int", value);
    }

    public void validate() {
        super.validateArray(new String[] { "java.lang.Integer" });
    }
}

class ClusterArrayFloat extends ClusterArrayBaseType {
    public ClusterArrayFloat(Object value) {
        super("array_float", value);
    }

    public void validate() {
        super.validateArray(new String[] { "java.lang.Integer", "java.lang.Float", "java.lang.Double" });
    }
}

class ClusterDict extends ClusterOperand {
    public ClusterDict(Object value) {
        super("dict", value);
    }

    public void validate() {
        if (this.getObjectValue("value") != null && (this.getObjectValue("value") instanceof Map) == false) {
            throw new RuntimeException("Validation Failed");
        }
    }
}

class ClusterObject extends ClusterOperand {
    public ClusterObject(Object value) {
        super("object", value);
    }

    public void validate() {
        if (this.getObjectValue("value") != null && (this.getObjectValue("value") instanceof ClusterOperand) == false) {
            throw new RuntimeException("Validation Failed");
        }
    }
}

class ClusterPyObject extends ClusterOperand {
    public ClusterPyObject(Object value) {
        super("pyobject", value);
    }

    public void validate() {
        throw new RuntimeException("Validation Failed");
    }
}

class ClusterArrayObject extends ClusterArrayBaseType {
    public ClusterArrayObject(Object value) {
        super("array_object", value);
    }

    public void validate() {
        super.validateArray(new String[] { "ClusterOperand" });
    }
}

class ClusterArrayPyObject extends ClusterArrayBaseType {
    public ClusterArrayPyObject(Object value) {
        super("array_pyobject", value);
    }

    public void validate() {
        throw new RuntimeException("Validation Failed");
    }
}

class ClusterFunc extends ClusterBaseOperand {
    public ClusterFunc(String name, String funcType, Object value) {
        super("ClusterFunc");

        this.putObject("name", name);
        this.putObject("func_type", funcType);
        this.putObject("value", value);
        this.putObject("data_type", "function");
    }

    public List<String> getObjectKeys() {
        return Arrays.asList(new String[]{"class_name", "name", "func_type", "value", "data_type"});
    }

    public void validate() {
        // TODO: What to validate
    }

    public static ClusterFunc fromJson(JSONObject jsonObj) {
        return new ClusterFunc(
            (String) jsonObj.get("name"),
            (String) jsonObj.get("func_type"),
            jsonObj.get("value")
        );
    }
}

class ClusterFuncLambda extends ClusterFunc {
    public ClusterFuncLambda(Object func) {
        super(null, null, null);
        throw new RuntimeException("Not implemented");
    }

    public void validate() {
        throw new RuntimeException("Not implemented");
    }
}

class ClusterFuncLibrary extends ClusterFunc {
    public ClusterFuncLibrary(String name, List<Object> args, Map<String, Object> kwargs) {
        super(name, "library", null);

        List<Object> jargs = new ArrayList<Object>();
        for (Object arg: args) {
            jargs.add(ClusterData.clusterOperandSerializer(arg));
        }

        // Java doesnt support optional kwargs like python
        if (kwargs != null && kwargs.size() > 0) {
            throw new RuntimeException("Java doesnt support kwargs");
        }

        // Map<String, ClusterObject> jkwargs = new HashMap<String, ClusterObject>();
        // for (Map.Entry<String, Object> entry: kwargs.entrySet()) {
        //     jkwargs.put(entry.getKey(), new ClusterObject(entry.getValue()));
        // }

        Map<String, Object> mapValue = new HashMap<String, Object>();
        mapValue.put("name", (String) this.getObjectValue("name"));
        mapValue.put("args", new ClusterArrayObject(jargs));
        // mapValue.put("kwargs", new ClusterDict(jkwargs));
        mapValue.put("kwargs", null);

        this.putObject("value", mapValue);
    }

    public void validate() {
        throw new RuntimeException("TBD");
    }

    public static ClusterFuncLibrary fromJson(JSONObject jsonObj) {
        throw new RuntimeException("TBD");
    }
}

class ClusterFuncJS extends ClusterFunc {
    public ClusterFuncJS(String jsFunc) {
        super(null, null, null);
        throw new RuntimeException("TBD");
    }

    public void validate() {
        throw new RuntimeException("TBD");
    }

    public static ClusterFuncJS fromJson(JSONObject jsonObj) {
        throw new RuntimeException("TBD");
    }
}

class ClusterArrayFunction extends ClusterFunc {
    public ClusterArrayFunction(Object value) {
        super("", "array_function", value);
    }

    public void validate() {
        if (this.getObjectValue("value") == null) {
            return; 
        }

        if (this.getObjectValue("value") instanceof List == false) {
            throw new RuntimeException("Validation Failed");
        }

        String allowedDataTypes[] = { "ClusterFunc" };
        for (Object elt: (List)this.getObjectValue("value")) {
            if (elt != null) {
                boolean flag = false; 
                for (String dt: allowedDataTypes) {
                      if (elt.getClass().getName().equals(dt)) {
                          flag = true;
                          break;
                      }
                }
                if (flag == false) {
                    throw new RuntimeException("Validation Failed");
                }
            }
        }
    }
}

// Main Class
public class ClusterData {
    public static Object loadNativeObjects(ClusterOperand clusterOperand) {
        throw new RuntimeException("TBD");
    }

    public static ClusterBaseOperand clusterOperandSerializer(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return new ClusterBool((Boolean) value);
        }

        if (value instanceof String) {
            return new ClusterStr((String) value);
        }

        if (value instanceof Integer) {
            return new ClusterInt((Integer) value);
        }

        if (value instanceof Double) {
            return new ClusterFloat((Double) value);
        }

        if (value instanceof ClusterOperand) {
            return new ClusterObject((ClusterOperand) value);
        }

        if (value instanceof List) {
            List<Object> listValues = (List<Object>) value;
            if (listValues.isEmpty() == false) {
                List<ClusterBaseOperand> arrValues = new ArrayList<ClusterBaseOperand>();
                for (Object x: listValues) {
                    arrValues.add(ClusterData.clusterOperandSerializer(x));
                }

                List<String> dataTypes = ClusterData.__get_data_types__(arrValues);
                if (dataTypes.size() == 1) {
                    if (dataTypes.get(0) == "bool")
                        return new ClusterArrayBool(value);
                    else if (dataTypes.get(0).equals("str"))
                        return new ClusterArrayStr(value);
                    else if (dataTypes.get(0).equals("int"))
                        return new ClusterArrayInt(value);
                    else if (dataTypes.get(0).equals("float"))
                        return new ClusterArrayFloat(value);
                    else if (dataTypes.get(0).equals("object"))
                        return new ClusterArrayObject(value);
                    else if (dataTypes.get(0).equals("pyobject"))
                        return new ClusterArrayPyObject(value);
                    else if (dataTypes.get(0).equals("function"))
                        return new ClusterArrayFunction(value);
                    else
                        throw new RuntimeException("Unable to parse: " + dataTypes);
                } else if (dataTypes.size() == 2) {
                    if (dataTypes.contains("int") && dataTypes.contains("float"))
                        return new ClusterArrayFloat(value);
                    else
                        throw new RuntimeException("Unable to parse: " + dataTypes);
                } else {
                    throw new RuntimeException("Unable to parse: " + dataTypes);
                }
            } else {
                return new ClusterArrayEmpty(value);
            }
        }

        if (value instanceof Map) {
            return new ClusterDict(value);
        }

        // Function is not supported here

        throw new RuntimeException("Unable to parse: " + value);
    }

    // TODO: ClusterBaseOperand instead of ClusterOperand
    private static List<String> __get_data_types__(List<ClusterBaseOperand> arr) {
        List<String> result = new ArrayList<String>();
        if (arr == null || arr.isEmpty()) {
            return result;
        }

        // TODO
        for (ClusterBaseOperand x: arr) {
            if (x.getClassName().equals("function")) {
                throw new RuntimeException("This flow is broken in python");
            }

            ClusterOperand clusterOperand = (ClusterOperand) x;
            String dataType = clusterOperand.getDataType();
            if (result.contains(dataType) == false) {
                result.add(dataType);
            }
        }

        return result;
    }
 
    public static ClusterBaseOperand clusterOperandDeserializer(Object obj) {
        if (obj == null) {
            return null;
        }

        System.out.println("clusterOperandDeserializer: obj: " + obj);
        JSONObject jsonObj = null;
        if (obj instanceof JSONObject) {
            jsonObj = (JSONObject) obj;
        } else if (obj instanceof String) {
            Object result = JSONValue.parse((String) obj);
            if (result instanceof JSONObject) {
                jsonObj = (JSONObject) result;
            } else {
                // TODO
                throw new RuntimeException("Non JSONObject not supported");
            }
        } else if (obj instanceof Map) {
            // TODO: not sure
            throw new RuntimeException("Not clear: Unable to deserialize: " + obj);
        } else {
            throw new RuntimeException("Unable to deserialize: " + obj);
        }
        
        if (jsonObj.containsKey("class_name") == false)
            throw new RuntimeException("clusterOperandDeserializer: class_name not found: " + jsonObj);

        String className = (String) jsonObj.get("class_name");
        if (className.equals("ClusterOperand") == false && className.equals("ClusterFunc") == false)
            throw new RuntimeException("clusterOperandDeserializer: class_name not found: " + jsonObj);

        String dataType = (String) jsonObj.get("data_type");
        Object value = jsonObj.get("value");
 
        if (dataType.equals("object")) {
            return new ClusterObject(ClusterData.clusterOperandDeserializer(value));
        } else if (dataType.equals("pyobject")) {
            return new ClusterPyObject(ClusterData.clusterOperandDeserializer(value));
        } else if (dataType.equals("bool")) {
            return new ClusterBool((Boolean) value);
        } else if (dataType.equals("str")) {
            return new ClusterStr((String) value);
        } else if (dataType.equals("int")) {
            return new ClusterInt((Integer) value);
        } else if (dataType.equals("float")) {
            return new ClusterFloat((Double) value);
        } else if (dataType.equals("array_empty")) {
            return new ClusterArrayEmpty((List) value);
        } else if (dataType.equals("array_bool")) {
            List<Object> result = new ArrayList<Object>();
            for (Object x: (List) value) {
                result.add((Boolean) x);
            }
            return new ClusterArrayBool(result);
        } else if (dataType.equals("array_str")) {
            List<Object> result = new ArrayList<Object>();
            for (Object x: (List) value) {
                result.add((String) x);
            }
            return new ClusterArrayStr(result);
        } else if (dataType.equals("array_int")) {
            List<Object> result = new ArrayList<Object>();
            for (Object x: (List) value) {
                result.add((Integer) x);
            }
            return new ClusterArrayInt(result);
        } else if (dataType.equals("array_float")) {
            List<Object> result = new ArrayList<Object>();
            for (Object x: (List) value) {
                result.add((Double) x);
            }
            return new ClusterArrayFloat(result);
        } else if (dataType.equals("array_object")) {
            List<Object> result = new ArrayList<Object>();
            for (Object x: (List) value) {
                result.add(ClusterData.clusterOperandDeserializer(x));
            }
            return new ClusterArrayObject(result);
        } else if (dataType.equals("array_pyobject")) {
            List<Object> result = new ArrayList<Object>();
            for (Object x: (List) value) {
                result.add(ClusterData.clusterOperandDeserializer(x));
            }
            return new ClusterArrayPyObject(result);
        } else if (dataType.equals("dict")) {
            // TODO
            return new ClusterDict((Map) value);
        } else if (dataType.equals("function")) {
            String funcType = (String) jsonObj.get("func_type");
            throw new RuntimeException("TBD");
        } else {
            throw new RuntimeException("TBD");
        }
    }

    public static void main(String args[]) {
        // System.out.println((new ClusterBool(false)).toJson());
        // System.out.println((new ClusterStr("string")).toJson());
        // System.out.println((new ClusterInt(1)).toJson());
        // System.out.println((new ClusterFloat(1.0)).toJson());
        // System.out.println((new ClusterArrayBool(Arrays.asList(new Boolean[] {true, false}))).toJson());
        // System.out.println((new ClusterArrayStr(Arrays.asList(new String[] {"1.0", "2.0"}))).toJson());
        // System.out.println((new ClusterArrayInt(Arrays.asList(new Integer[] {1, 2}))).toJson());
        // System.out.println((new ClusterArrayFloat(Arrays.asList(new Double[] {1.0, 2.0}))).toJson());

        // ClusterOperand x1a = new ClusterArrayFloat(Arrays.asList(new Double[]{1.0, 2.0, 4.0}));
        // ClusterOperand x2a = new ClusterObject(x1a);
        // ClusterBaseOperand x1b = ClusterData.clusterOperandDeserializer(x2a.toJson()); 
        // System.out.println(x2a.toJson());
        // System.out.println(x1b.toJson());

        ClusterFunc f1 = new ClusterFuncLibrary("transform", Arrays.asList(new Object[]{"col1", "col2", Integer.valueOf(1)}), null);
        System.out.println(f1.toJson());

        // List<Object> arrlist = Arrays.asList(new Object[]{"a", "b"});
        // for (Object x: arrlist) {
        //     System.out.println(x.getClass().getName());
        //     System.out.println(x instanceof String);
        // }
    }
}

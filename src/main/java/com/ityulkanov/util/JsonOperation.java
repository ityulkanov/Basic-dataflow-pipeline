package com.ityulkanov.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JsonOperation {
    JsonOperation() {
    }

    public enum Type {
        ARRAY, PRIMITIVE, OBJECT, NOT_NULL, NULL
    }

    public static JsonObject renameField(JsonObject obj, String propName, String newName) {
        if (obj.has(propName)) {
            JsonElement value = obj.get(propName);
            obj.add(newName, value);
            obj.remove(propName);
        }
        return obj;
    }

    /**
     * updProperty if obj of type primitive, changes the property name and removes the property from the passed JsonObject
     *
     * @param from     This is the override object
     * @param to       This is the object to which the property is to be added
     * @param propName This is the property name
     */
    public static void moveProperty(JsonObject from, JsonObject to, String propName) {
        if (matchPropToType(from, propName, Type.PRIMITIVE)) {
            to.addProperty(propName, from.getAsJsonPrimitive(propName).getAsString());
            from.remove(propName);
        }
    }

    public static void modifyObjArray(JsonObject obj, JsonArray objArray, String propName, String propToAdd, String value, String type) {
        if (matchPropToType(obj, propName, Type.PRIMITIVE)) {
            JsonObject modifiedObj = new JsonObject();
            modifiedObj.addProperty(value, obj.getAsJsonPrimitive(propName).getAsString());
            modifiedObj.addProperty(type, propToAdd);
            objArray.add(modifiedObj);
            obj.remove(propName);
        }
    }

    public static JsonObject moveFieldToRoot(JsonObject rootObj, String subObjName, String srcFieldName, String targetFieldName) {
        if (matchPropToType(rootObj, subObjName, Type.OBJECT)) {
            JsonObject subObj = rootObj.getAsJsonObject(subObjName);
            JsonElement srcFieldVal = subObj.get(srcFieldName);
            if (srcFieldVal != null) {
                rootObj.add(targetFieldName, srcFieldVal);
                subObj.remove(srcFieldName);
            }
        }
        return rootObj;
    }

    public static JsonObject moveAllFieldsToRoot(JsonObject rootObj, String subObjName) {
        if (matchPropToType(rootObj, subObjName, Type.OBJECT)) {
            JsonObject subObj = rootObj.getAsJsonObject(subObjName);
            // Move all fields from subObj to rootObj
            for (String key : subObj.keySet()) {
                rootObj.add(key, subObj.get(key));
            }
            // Remove the sub-object
            rootObj.remove(subObjName);
        }
        return rootObj;
    }

    public static JsonObject addFieldToSubObj(JsonObject rootObj, String subObjName, String fieldName, String fieldVal) {
        if (rootObj.has(subObjName) && rootObj.get(subObjName).isJsonObject()) {
            JsonObject subObj = rootObj.getAsJsonObject(subObjName);
            subObj.addProperty(fieldName, fieldVal);
            rootObj.add(subObjName, new Gson().toJsonTree(subObj));
        }
        return rootObj;
    }

    public static JsonObject renameFieldToSubObj(JsonObject rootObj, String subObjName, String srcFieldName, String targetFieldName) {
        if (matchPropToType(rootObj, subObjName, Type.OBJECT)) {
            JsonObject subObj = rootObj.getAsJsonObject(subObjName);
            JsonOperation.renameField(subObj, srcFieldName, targetFieldName);
            rootObj.add(subObjName, new Gson().toJsonTree(subObj));
        }
        return rootObj;
    }

    public static JsonObject copyFieldToRoot(JsonObject rootObj, String subObjName, String srcFieldName, String targetFieldName) {
        if (matchPropToType(rootObj, subObjName, Type.OBJECT)) {
            JsonObject subObj = rootObj.getAsJsonObject(subObjName);
            JsonElement srcFieldVal = subObj.get(srcFieldName);
            if (srcFieldVal != null) {
                rootObj.add(targetFieldName, srcFieldVal);
            }
        }
        return rootObj;
    }

    /**
     * matchPropToType if a given JsonObject (obj) has a property with a specified name
     * (name) and if the type of this property matches a specified type (type).
     *
     * @param obj  JsonObject to evaluate
     * @param name specifies the name of the property
     * @param type can be ARRAY, PRIMITIVE, OBJECT, or NOT_NULL, NULL
     * @return if such property with this type exists
     */
    public static boolean matchPropToType(JsonObject obj, String name, Type type) {
        if (obj.has(name) && obj.get(name) != null) {
            switch (type) {
                case ARRAY:
                    return obj.get(name).isJsonArray();
                case PRIMITIVE:
                    return obj.get(name).isJsonPrimitive();
                case OBJECT:
                    return obj.get(name).isJsonObject();
                case NOT_NULL:
                    return !obj.get(name).isJsonNull();
                case NULL:
                    return obj.get(name).isJsonNull();
                default:
                    log.error("Invalid type specified");
                    return false;
            }
        }
        return false;
    }

    /**
     * Copies a property from the `srcObj` object to the `dest` object with a new property name.
     *
     * @param srcObj      the source `JsonObject` containing the property to copy
     * @param oldPropName the name of the property in `srcObj` to copy
     * @param destObj     the target `JsonObject` to which the property will be added
     * @param newPropName the name of the property to use in `dest`
     */
    public static void copyProperty(JsonObject srcObj, String oldPropName, JsonObject destObj, String newPropName) {
        if (srcObj == null || destObj == null || oldPropName == null || newPropName == null) {
            return;
        }
        if (matchPropToType(srcObj, oldPropName, Type.PRIMITIVE)) {
            String prop = srcObj.get(oldPropName).getAsString();
            destObj.addProperty(newPropName, prop);
        }
    }


    /**
     * Safely get a JsonObject member from parent. If missing, null, or not an object, returns an empty JsonObject.
     */
    public static JsonObject safeJsonObject(JsonObject parent, String memberName) {
        if (parent == null || memberName == null) {
            return new JsonObject();
        }
        if (!matchPropToType(parent, memberName, Type.OBJECT)) {
            return new JsonObject();
        }
        return parent.getAsJsonObject(memberName);
    }
}
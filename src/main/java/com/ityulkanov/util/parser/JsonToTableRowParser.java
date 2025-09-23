package com.ityulkanov.util.parser;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ityulkanov.exception.JsonToBQDataParsingException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class JsonToTableRowParser {
    private JsonToTableRowParser() {
    }

    /**
     * Parses property files to Table Row
     *
     * @param columnList  list of JSON fields
     * @param fields      list of BQ fields
     * @param prop        reversed property files
     * @param errorFields list of errors
     * @return BQ table row result
     */
    public static TableRow parseToTableRow(JsonObject columnList, FieldList fields, Properties prop, List<String> errorFields) {
        TableRow row = new TableRow();
        Map<String, String> propertyMap = createPropertyMap(prop);
        for (String jsonFieldName : columnList.keySet()) {
            JsonElement column = columnList.get(jsonFieldName);
            String bqFieldName;
            if (prop.isEmpty()) {
                bqFieldName = jsonFieldName;
            } else {
                bqFieldName = propertyMap.getOrDefault(jsonFieldName, null);
            }
            if (bqFieldName != null) {
                Field field;
                try {
                    field = fields.get(bqFieldName);
                } catch (IllegalArgumentException e) {
                    errorFields.add(jsonFieldName);
                    continue;
                }
                if (field != null) {
                    LegacySQLTypeName type = field.getType();
                    Field.Mode mode = field.getMode();
                    if (!Field.Mode.REPEATED.equals(mode)) {
                        if (!(LegacySQLTypeName.RECORD.equals(type))) {
                            processPrimitiveType(columnList, jsonFieldName, bqFieldName, type, row, errorFields);
                        } else {
                            processRecordType(columnList, jsonFieldName, bqFieldName, field, row, prop, errorFields, JsonToTableRowParser::parseToTableRow);
                        }
                    } else {
                        processRepeatedType(column, jsonFieldName, bqFieldName, type, field, row, prop, errorFields, JsonToTableRowParser::parseToTableRow);
                    }
                }
            }

        }
        return row;
    }

    private static Map<String, String> createPropertyMap(Properties prop) {
        return prop.stringPropertyNames().stream()
                .collect(Collectors.toMap(fieldName -> fieldName, prop::getProperty));
    }


    private static void processRepeatedType(
            JsonElement column,
            String jsonFieldName,
            String bqFieldName,
            LegacySQLTypeName type,
            Field field,
            TableRow row,
            Properties prop,
            List<String> errorFields, JsonToTableRowFunction parser) {
        if (column != null) {
            try {
                JsonArray fieldArray = column.getAsJsonArray();
                if (LegacySQLTypeName.RECORD.equals(type)) {
                    processArrayOfRecords(bqFieldName, field, fieldArray, row, prop, errorFields, parser);
                } else if (LegacySQLTypeName.NUMERIC.equals(type)) {
                    processArrayOfNumeric(bqFieldName, jsonFieldName, fieldArray, row, errorFields);
                } else if (LegacySQLTypeName.INTEGER.equals(type)) {
                    processArrayOfInteger(bqFieldName, jsonFieldName, fieldArray, row, errorFields);
                } else if (LegacySQLTypeName.FLOAT.equals(type)) {
                    processArrayOfFloat(bqFieldName, jsonFieldName, fieldArray, row, errorFields);
                } else if (LegacySQLTypeName.BOOLEAN.equals(type)) {
                    processArrayOfBoolean(bqFieldName, jsonFieldName, fieldArray, row, errorFields);
                } else if (LegacySQLTypeName.TIMESTAMP.equals(type)) {
                    processArrayOfTimestamp(bqFieldName, jsonFieldName, fieldArray, row, errorFields);
                } else if (LegacySQLTypeName.DATE.equals(type)) {
                    processArrayOfDate(bqFieldName, jsonFieldName, fieldArray, row, errorFields);
                } else {
                    processArrayOfString(bqFieldName, jsonFieldName, fieldArray, row, errorFields);
                }
            } catch (Exception e) {
                throw new JsonToBQDataParsingException(e.getMessage());
            }
        }
    }

    private static <T> void processArray(
            String bqFieldName,
            String jsonFieldName,
            JsonArray fieldArray,
            TableRow row,
            List<String> errorFields,
            Function<String, T> parser) {
        List<T> repeatedRow = new ArrayList<>();
        for (JsonElement element : fieldArray) {
            try {
                String colString = element.getAsString();
                T value = parser.apply(colString);
                repeatedRow.add(value);
            } catch (Exception e) {
                repeatedRow.add(null);
                errorFields.add(jsonFieldName);
            }
        }
        row.set(bqFieldName, repeatedRow);
    }

    private static void processArrayOfRecords(String bqFieldName, Field field, JsonArray fieldArray, TableRow row, Properties prop, List<String> errorFields, JsonToTableRowFunction parseFunction) {
        List<TableRow> repeatedRow = new ArrayList<>();
        FieldList subFields1 = field.getSubFields();
        for (JsonElement element : fieldArray) {
            JsonObject arrayRecord = element.getAsJsonObject();
            TableRow arrayFieldRecord = parseFunction.apply(arrayRecord, subFields1, prop, errorFields);
            repeatedRow.add(arrayFieldRecord);
        }
        row.set(bqFieldName, repeatedRow);
    }

    private static void processRecordType(JsonObject columnList, String jsonFieldName, String bqFieldName, Field field, TableRow row, Properties prop, List<String> errorFields, JsonToTableRowFunction parser) {
        FieldList subFields = field.getSubFields();
        JsonElement elemField = columnList.get(jsonFieldName);
        if (elemField != null) {
            try {
                JsonObject objField = elemField.getAsJsonObject();
                TableRow recordField = parser.apply(objField, subFields, prop, errorFields);
                row.set(bqFieldName, recordField);
            } catch (Exception e) {
                throw new JsonToBQDataParsingException(e.getMessage());
            }
        }
    }

    private static void processArrayOfNumeric(String bqFieldName, String jsonFieldName, JsonArray fieldArray, TableRow row, List<String> errorFields) {
        processArray(bqFieldName, jsonFieldName, fieldArray, row, errorFields, JsonToBQPrimitivePropertiesParser::parseNumeric);
    }

    private static void processArrayOfString(String bqFieldName, String jsonFieldName, JsonArray fieldArray, TableRow row, List<String> errorFields) {
        processArray(bqFieldName, jsonFieldName, fieldArray, row, errorFields, str -> str);
    }

    private static void processArrayOfFloat(String bqFieldName, String jsonFieldName, JsonArray fieldArray, TableRow row, List<String> errorFields) {
        processArray(bqFieldName, jsonFieldName, fieldArray, row, errorFields, Float::valueOf);
    }

    private static void processArrayOfTimestamp(String bqFieldName, String jsonFieldName, JsonArray fieldArray, TableRow row, List<String> errorFields) {
        processArray(bqFieldName, jsonFieldName, fieldArray, row, errorFields, JsonToBQPrimitivePropertiesParser::parseTimestamp);
    }

    private static void processArrayOfDate(String bqFieldName, String jsonFieldName, JsonArray fieldArray, TableRow row, List<String> errorFields) {
        processArray(bqFieldName, jsonFieldName, fieldArray, row, errorFields, JsonToBQPrimitivePropertiesParser::parseDate);
    }

    private static void processArrayOfBoolean(String bqFieldName, String jsonFieldName, JsonArray fieldArray, TableRow row, List<String> errorFields) {
        processArray(bqFieldName, jsonFieldName, fieldArray, row, errorFields, JsonToBQPrimitivePropertiesParser::parseBoolean);
    }

    private static void processArrayOfInteger(String bqFieldName, String jsonFieldName, JsonArray fieldArray, TableRow row, List<String> errorFields) {
        processArray(bqFieldName, jsonFieldName, fieldArray, row, errorFields, JsonToBQPrimitivePropertiesParser::parseInteger);
    }

    private static <T> void processPrimitive(
            TableRow row, String bqFieldName, String colString, Function<String, T> parser) {
        T value = parser.apply(colString);
        row.set(bqFieldName, value);
    }

    private static void processPrimitiveType(JsonObject columnList, String jsonFieldName, String bqFieldName, LegacySQLTypeName type, TableRow row, List<String> errorFields) {
        try {
            String colString = JsonToBQPrimitivePropertiesParser.getJsonPrimitive(columnList, jsonFieldName);
            if (LegacySQLTypeName.NUMERIC.equals(type)) {
                processPrimitive(row, bqFieldName, colString, JsonToBQPrimitivePropertiesParser::parseNumeric);
            } else if (LegacySQLTypeName.INTEGER.equals(type)) {
                processPrimitive(row, bqFieldName, colString, JsonToBQPrimitivePropertiesParser::parseInteger);
            } else if (LegacySQLTypeName.FLOAT.equals(type)) {
                processPrimitive(row, bqFieldName, colString, JsonToBQPrimitivePropertiesParser::parseFloat);
            } else if (LegacySQLTypeName.BOOLEAN.equals(type)) {
                processPrimitive(row, bqFieldName, colString, JsonToBQPrimitivePropertiesParser::parseBoolean);
            } else if (LegacySQLTypeName.TIMESTAMP.equals(type)) {
                processPrimitive(row, bqFieldName, colString, JsonToBQPrimitivePropertiesParser::parseTimestamp);
            } else if (LegacySQLTypeName.DATE.equals(type)) {
                processPrimitive(row, bqFieldName, colString, JsonToBQPrimitivePropertiesParser::parseDate);
            } else if (!(LegacySQLTypeName.RECORD.equals(type))) {
                row.set(bqFieldName, colString);
            }
        } catch (Exception e) {
            errorFields.add(jsonFieldName);
            row.set(bqFieldName, null);
        }
    }
}
package com.ityulkanov.util.parser;


import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.FieldList;
import com.google.gson.JsonObject;

import java.util.List;
import java.util.Properties;

@FunctionalInterface
public interface JsonToTableRowFunction {
    TableRow apply(JsonObject jsonObject, FieldList fields, Properties prop, List<String> errorFields);
}

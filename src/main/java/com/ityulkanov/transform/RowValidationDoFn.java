package com.ityulkanov.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.FieldList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.ExceptionUtil;
import com.ityulkanov.util.parser.JsonToTableRowParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.ityulkanov.cons.ContentInfo.SCHEMA_MISS_MATCH;
import static com.ityulkanov.cons.ContentInfo.TYPE_CAST_ERR;
import static com.ityulkanov.cons.ContentInfo.TYPE_CAST_MSG;


/**
 * This DoFn class will be used for validating data type for the fields of json
 * string against table schema and converting the original json to the TableRow object
 */
@Slf4j
public class RowValidationDoFn extends DoFn<String, TableRow> {

    private final FieldList fields;
    private final TupleTag<TableRow> validationOut;
    private final TupleTag<ErrorTblDtl> validationFail;
    private final String tableName;
    private final Properties properties;

    public RowValidationDoFn(
            FieldList fields,
            TupleTag<TableRow> validationOut,
            TupleTag<ErrorTblDtl> validationFail,
            String tableName, Properties properties) {
        this.fields = fields;
        this.validationOut = validationOut;
        this.validationFail = validationFail;
        this.tableName = tableName;
        this.properties = properties;
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver out) {
        String json = context.element();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        try {
            List<String> errorFields = new ArrayList<>();
            TableRow tableRow;
            tableRow = JsonToTableRowParser.parseToTableRow(jsonObject, fields, properties, errorFields);
            if (errorFields.isEmpty()) {
                if (tableRow.size() > 1) {
                    out.get(validationOut).output(tableRow);
                } else {
                    String timeStamp = context.timestamp().toString();
                    ErrorTblDtl errTblDet = new ErrorTblDtl(tableName, json, String.format("No rows parsed. Properties amount: %d, fields amount: %d", properties.size(), fields.size()), "Invalid Record", timeStamp);
                    out.get(validationFail).output(errTblDet);
                }
            } else {
                out.get(validationOut).output(tableRow);
                String timeStamp = context.timestamp().toString();
                String fullTypeCastErrorMessage = getTypeCastErrorString(errorFields);
                ErrorTblDtl errTblDet = new ErrorTblDtl(tableName, json, fullTypeCastErrorMessage, TYPE_CAST_ERR, timeStamp);
                out.get(validationFail).output(errTblDet);
            }
        } catch (Exception e) {
            String excepAsString = ExceptionUtil.getExceptionAsString(e);
            log.error("Exception occurred: {}", excepAsString);
            String timeStamp = context.timestamp().toString();
            ErrorTblDtl errTblDet = new ErrorTblDtl(tableName, json, excepAsString, SCHEMA_MISS_MATCH, timeStamp);
            out.get(validationFail).output(errTblDet);
        }
    }

    private static String getTypeCastErrorString(List<String> errorFields) {
        StringBuilder stringBuilder = new StringBuilder(TYPE_CAST_MSG);
        int size = errorFields.size();
        for (int i = 0; i < size; i++) {
            stringBuilder.append(errorFields.get(i));
            if (i < size - 1) {
                stringBuilder.append(", ");
            }
        }
        return stringBuilder.toString();
    }
}
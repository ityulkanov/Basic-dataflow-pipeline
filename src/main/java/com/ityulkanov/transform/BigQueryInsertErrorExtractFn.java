package com.ityulkanov.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.ityulkanov.cons.ContentInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * This DoFn class will be used to log BQ target table failed insertion records to exception table
 */
@Slf4j
public class BigQueryInsertErrorExtractFn extends DoFn<BigQueryInsertError, TableRow> {

    private final String tableId;

    public BigQueryInsertErrorExtractFn(String tableId) {
        this.tableId = tableId;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        BigQueryInsertError bigQueryInsertError = context.element();
        TableRow row = bigQueryInsertError.getRow();

        String error = bigQueryInsertError.getError().toString();
        log.error("ERROR: {} for table row: {}", error, row);
        TableRow convertedRow = new TableRow();
        convertedRow.set(ContentInfo.TABLE_ID, tableId);
        convertedRow.set(ContentInfo.TABLE_ROW, row.toString());
        convertedRow.set(ContentInfo.ERROR, error);
        convertedRow.set(ContentInfo.ERROR_TYPE, ContentInfo.ACT_BQ_INSERT);
        convertedRow.set(ContentInfo.ERR_TIMESTMP, context.timestamp().toString());
        if (error.contains("row too large")) {
            log.error("ERROR: Table row is too large. Check logs for the details.");
        }
        Gson gson = new Gson();
        String json = gson.toJson(row);
        log.error("The json that failed to load: {} ", json);
        context.output(convertedRow);
    }
}

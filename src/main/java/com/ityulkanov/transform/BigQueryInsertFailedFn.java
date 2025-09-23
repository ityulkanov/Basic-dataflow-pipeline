package com.ityulkanov.transform;


import com.google.api.services.bigquery.model.TableRow;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.util.ErrorTblDtl;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * This DoFn class will be used for transforming any parse failed json to BQ table Row and log to Exception table
 */
@Slf4j
public class BigQueryInsertFailedFn extends DoFn<ErrorTblDtl, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            ErrorTblDtl bigQueryInsertError = context.element();
            TableRow convertedRow = new TableRow();
            convertedRow.set(ContentInfo.TABLE_ID, bigQueryInsertError.getTableId());
            convertedRow.set(ContentInfo.TABLE_ROW, bigQueryInsertError.getTableRow());
            convertedRow.set(ContentInfo.ERROR, bigQueryInsertError.getError());
            convertedRow.set(ContentInfo.ERROR_TYPE, bigQueryInsertError.getErrorType());
            convertedRow.set(ContentInfo.ERR_TIMESTMP, bigQueryInsertError.getTimeStamp());
            log.info(convertedRow.toString());
            context.output(convertedRow);
        } catch (Exception e) {
            log.info(ContentInfo.INFO_ERR_TAB_INSERT_FAILED);
        }
    }
}

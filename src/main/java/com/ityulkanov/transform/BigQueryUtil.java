package com.ityulkanov.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.util.ErrorTblDtl;
import com.ityulkanov.util.ExceptionUtil;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryUtil.class);

    private BigQueryUtil() {
    }

    public static FieldList getTblFields(String projectId, String dataSet, String tableName) {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Table table = bigquery.getTable(TableId.of(projectId, dataSet, tableName));
        Schema schema = table.getDefinition().getSchema();
        if (schema != null) {
            return schema.getFields();
        } else {
            throw new IllegalArgumentException("Table schema is null");
        }
    }

    public static BigQueryIO.Write<TableRow> configureBigQueryIO(String tableName) {
        return BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .to(tableName);
    }

    //todo combine these 2 methods into one, give normal transformations names
    public static void insertErrorRecord(
            PCollectionTuple jsonMessages,
            TupleTag<ErrorTblDtl> transformFailGen,
            String errTbl) {

        try {
            jsonMessages
                    .get(transformFailGen)
                    .apply(
                            ContentInfo.INFO_CREATE_FAILED_RECORD,
                            ParDo.of(new BigQueryInsertFailedFn())
                    )
                    .apply(
                            ContentInfo.INFO_WRITE_FAILED_RECORD,
                            configureBigQueryIO(errTbl));
        } catch (Exception e) {
            LOGGER.error("{} : {}", ContentInfo.INFO_ERR_TAB_INSERT_FAILED, ExceptionUtil.getExceptionAsString(e));
        }
    }

    public static void insertInvalidJsonErrorRecord(
            PCollectionTuple jsonMessages,
            TupleTag<ErrorTblDtl> transformFailGen,
            String errTbl
    ) {
        jsonMessages.get(transformFailGen)
                .apply(ContentInfo.INFO_WRITE_JSON_TO_ERR_TABLE, ParDo.of(new BigQueryInsertFailedFn()))
                .apply(
                        ContentInfo.INFO_WRITE_FAILED_REC,
                        configureBigQueryIO(errTbl));
    }

    public static WriteResult writeRowsToTableWithFileLoads(
            PCollectionTuple rows,
            TupleTag<TableRow> validRowsTag,
            String project,
            String dataset,
            String table) {
        return rows.get(validRowsTag)
                .apply(ContentInfo.INFO_WRITE_SUCCESS_REC_TO_CL, configureBigQueryIO(String.format("%s:%s.%s", project, dataset, table)));
    }
}

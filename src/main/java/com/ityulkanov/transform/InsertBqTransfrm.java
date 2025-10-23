package com.ityulkanov.transform;


import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.FieldList;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.util.ErrorTblDtl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Properties;

import static com.ityulkanov.transform.BigQueryUtil.configureBigQueryIO;


/**
 * This PTransform class will be used for Insert Table Row to Target BQ table
 * It will also log the insert failed record to exception table.
 */
@Slf4j
public class InsertBqTransfrm extends PTransform<PCollectionTuple, WriteResult> {

    private final String projectId;
    private final String dataSet;
    private final String tbleName;
    private final String errTbl;
    private final TupleTag<String> tag;
    private final TupleTag<TableRow> transformOut;
    private final TupleTag<ErrorTblDtl> transformFail;
    private final FieldList fields;
    private final Properties prop;

    public InsertBqTransfrm(TupleTag<String> tag, TupleTag<TableRow> transformOut, TupleTag<ErrorTblDtl> transformFail,
                            String projectId, String dataSet, String tbleName, String errTbl, FieldList fields) {

        this.tag = tag;
        this.transformOut = transformOut;
        this.transformFail = transformFail;
        this.projectId = projectId;

        this.dataSet = dataSet;
        this.tbleName = tbleName;
        this.errTbl = errTbl;
        this.fields = fields;
        this.prop = new Properties();

    }

    public InsertBqTransfrm(TupleTag<String> tag, TupleTag<TableRow> transformOut, TupleTag<ErrorTblDtl> transformFail,
                            String projectId, String dataSet, String tbleName, String errTbl, FieldList fields, Properties prop) {
        this.tag = tag;
        this.transformOut = transformOut;
        this.transformFail = transformFail;
        this.projectId = projectId;

        this.dataSet = dataSet;
        this.tbleName = tbleName;
        this.errTbl = errTbl;
        this.fields = fields;
        this.prop = prop;
    }

    @Override
    @NonNull
    public WriteResult expand(PCollectionTuple inputTuple) {
        PCollectionTuple outTableRow = inputTuple.get(tag).apply(ContentInfo.INFO_JSON_TO_TABLE_ROW, ParDo
                .of(new RowValidationDoFn(fields, transformOut, transformFail, tbleName, prop)).withOutputTags(transformOut, TupleTagList.of(transformFail)));

        WriteResult clLeadwriteResult = outTableRow.get(transformOut)
                .apply(ContentInfo.INFO_WRITE_SUCCESS_REC_TO_CL, configureBigQueryIO(projectId + ContentInfo.SEP_COLON + dataSet + ContentInfo.SEP_DOT + tbleName));

        clLeadwriteResult.getFailedInsertsWithErr()
                .apply(ContentInfo.INFO_BQ_INSERT_ERR_EXTRACT, ParDo.of(new BigQueryInsertErrorExtractFn(tbleName)))
                .apply(ContentInfo.INFO_BQ_INSERT_ERR_WRITE, configureBigQueryIO(errTbl));

        return outTableRow.get(transformFail)
                .apply(ContentInfo.INFO_WRITE_JSON_TO_ERR_TABLE, ParDo.of(new BigQueryInsertFailedFn()))
                .apply(ContentInfo.INFO_WRITE_FAILED_REC, configureBigQueryIO(errTbl));
    }
}
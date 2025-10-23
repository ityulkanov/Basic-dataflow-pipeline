package com.ityulkanov.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.FieldList;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.util.ErrorTblDtl;
import lombok.NonNull;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Properties;

public class IBTTrustedTransform extends PTransform<PCollectionTuple, WriteResult> {
    static final TupleTag<String> ibtTag = new TupleTag<String>() {
    };
    static final TupleTag<ErrorTblDtl> transformFail = new TupleTag<ErrorTblDtl>() {
    };
    static final TupleTag<TableRow> ltransformOut = new TupleTag<TableRow>() {
    };

    TupleTag<String> tag;
    String projectId;
    String dataSet;
    String ibtTableName;
    String errTbl;
    FieldList csfieldlist;
    Properties fieldsMappping;

    public IBTTrustedTransform(
            TupleTag<String> tag,
            String projectId,
            String dataSet,
            String ibtTableName,
            String errTbl,
            FieldList csfieldlist,
            Properties fieldsMappping
    ) {
        this.tag = tag;
        this.projectId = projectId;
        this.dataSet = dataSet;
        this.ibtTableName = ibtTableName;
        this.errTbl = errTbl;
        this.csfieldlist = csfieldlist;
        this.fieldsMappping = fieldsMappping;
    }

    @Override
    @NonNull
    public WriteResult expand(PCollectionTuple inputTuple) {
        PCollectionTuple ibtTuple = inputTuple.get(tag).apply("coreSales transform", ParDo
                .of(new IbtTrustedFn(ibtTag, transformFail)).withOutputTags(ibtTag, TupleTagList.of(transformFail)));

        WriteResult outResult = ibtTuple.apply("BQ  trusted record insert ",
                new InsertBqTransfrm(ibtTag, ltransformOut, transformFail,
                        projectId, dataSet, ibtTableName, errTbl, csfieldlist, fieldsMappping));

        ibtTuple.get(transformFail)
                .apply(ContentInfo.INFO_WRITE_JSON_TO_ERR_TABLE, ParDo.of(new BigQueryInsertFailedFn()))
                .apply(ContentInfo.INFO_WRITE_FAILED_REC,
                        BigQueryIO.writeTableRows().to(errTbl).withoutValidation()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        return outResult;

    }
}

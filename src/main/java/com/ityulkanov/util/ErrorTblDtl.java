package com.ityulkanov.util;

import com.google.common.base.MoreObjects;
import lombok.Setter;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class ErrorTblDtl implements Serializable {

    public String getTableId() {
        return tableId;
    }

    public String getTableRow() {
        return tableRow;
    }

    public String getError() {
        return error;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    String tableId;
    String tableRow;
    @Setter
    String error;
    String errorType;
    @Setter
    String timeStamp;

    public ErrorTblDtl() {
    }

    public ErrorTblDtl(String tableId, String tableRow, String error, String errorType, String timeStamp) {
        this.tableId = tableId;
        this.tableRow = tableRow;
        this.error = error;
        this.errorType = errorType;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("tableId", tableId)
                .add("tableRow", tableRow)
                .add("errorType", errorType)
                .add("error", error)
                .add("timeStamp", timeStamp)
                .toString();

    }

}

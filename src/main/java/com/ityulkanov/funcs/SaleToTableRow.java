package com.ityulkanov.funcs;

import com.google.api.services.bigquery.model.TableRow;
import com.ityulkanov.avro.Sale;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

import java.time.Instant;

/**
 * Converting from internal class into table rows
 */
@Slf4j
public class SaleToTableRow extends DoFn<Sale, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        Sale sale = c.element();
        Instant instant = Instant.now();
        String timestampAsString = instant.toString();
        TableRow tableRow = new TableRow()
                .set("product_name", sale != null ? sale.getProductName() : null)
                .set("store_id", sale != null ? sale.getStoreID() : null)
                .set("product_id", sale != null ? sale.getProductID() : null)
                .set("price", sale != null ? sale.getPrice() : null)
                .set("discount", sale != null ? sale.getDiscount() : null)
                .set("updated_price", sale != null ? sale.getUpdatedPrice() : null)
                .set("transaction_id", sale != null ? sale.getTransactionID() : null)
                .set("timestamp", timestampAsString)
                .set("sales_date", sale != null ? sale.getSalesDate() : null);
        log.debug("TableRow: {}", tableRow);
        c.output(tableRow);
    }
}
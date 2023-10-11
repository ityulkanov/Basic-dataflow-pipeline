package com.ityulkanov.funcs;

import com.google.gson.Gson;
import com.ityulkanov.model.Sale;
import org.apache.beam.sdk.transforms.DoFn;

public class ProcessJson extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String jsonStr = c.element();
        assert jsonStr != null;
        Gson gson = new Gson();
        Sale sale = gson.fromJson(jsonStr, Sale.class);
        sale.setProductName(sale.getProductName().toUpperCase());
        sale.setTransactionID(String.format("%s_%s_%s", sale.getStoreID(), sale.getProductID(), sale.getSalesDate()));
        sale.setUpdatedPrice(sale.getPrice() - sale.getDiscount());
        c.output(gson.toJson(sale));
    }
}

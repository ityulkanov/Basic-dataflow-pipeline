package com.ityulkanov.funcs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.ityulkanov.avro.Sale;
import org.apache.beam.sdk.transforms.DoFn;

import java.lang.reflect.Type;

/**
 * Converting from JSON to internal class and proceed with adding new fields
 */
public class ProcessJson extends DoFn<String, Sale> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String jsonStr = c.element();
        assert jsonStr != null;
        Gson gson = new GsonBuilder().registerTypeAdapter(Sale.class, new AvroGeneratedClassDeserializer()).create();
        Sale sale = gson.fromJson(jsonStr, Sale.class);
        c.output(sale);
    }

    public static class AvroGeneratedClassDeserializer implements JsonDeserializer<Sale> {
        @Override
        public Sale deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();

            String salesDate = jsonObject.get("sales_date").getAsString().toLowerCase();
            String storeId = jsonObject.get("store_id").getAsString().toLowerCase();
            String productID = jsonObject.get("product_id").getAsString().toLowerCase();
            String productName = jsonObject.get("product_name").getAsString().toUpperCase();
            double price = jsonObject.get("price").getAsDouble();
            double discount = jsonObject.get("discount").getAsDouble();
            double updatedPrice;
            String transactionID;
            if (jsonObject.get("updated_price") != null) {
                updatedPrice = jsonObject.get("updated_price").getAsDouble();
            } else {
                updatedPrice = price - discount;
            }
            if (jsonObject.get("transaction_id") != null) {
                transactionID = jsonObject.get("transaction_id").getAsString();
            } else {
                transactionID = String.format("%s_%s_%s", storeId, productID, salesDate).toLowerCase();
            }

            return Sale.newBuilder()
                    .setSalesDate(salesDate)
                    .setStoreID(storeId)
                    .setProductID(productID)
                    .setProductName(productName)
                    .setPrice(price)
                    .setDiscount(discount)
                    .setTransactionID(transactionID)
                    .setUpdatedPrice(updatedPrice)
                    .build();
        }
    }
}



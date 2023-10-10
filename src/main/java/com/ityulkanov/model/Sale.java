package com.ityulkanov.model;


import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class Sale {
    @SerializedName("sales_date")
    private String salesDate;
    @SerializedName("store_id")
    private String storeID;
    @SerializedName("product_id")
    private String productID;
    @SerializedName("product_name")
    private String productName;
    @SerializedName("price")
    private double price;
    @SerializedName("discount")
    private double discount;
    @SerializedName("updated_price")
    private double updatedPrice;
    @SerializedName("transaction_id")
    private String transactionID;
}
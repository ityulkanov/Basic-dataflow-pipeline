package com.ityulkanov.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ityulkanov.cons.ContentInfo;
import com.ityulkanov.util.ErrorTblDtl;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class IbtTrustedTransform extends PTransform<PCollection<String>, PCollectionTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(IbtTrustedTransform.class);
    private final TupleTag<TableRow> transformOut;
    private final TupleTag<ErrorTblDtl> transformFail;
    private final Properties fieldsMapping;

    public IbtTrustedTransform(TupleTag<TableRow> transformOut, TupleTag<ErrorTblDtl> transformFail, Properties fieldsMapping) {
        this.transformOut = transformOut;
        this.transformFail = transformFail;
        this.fieldsMapping = fieldsMapping;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
        return input.apply("Transform IBT for Trusted", ParDo.of(new DoFn<String, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String jsonString = c.element();
                Gson gson = new Gson();

                try {
                    JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
                    TableRow trustedRow = new TableRow();

                    // Add Base_Info from the root
                    if (jsonObject.has("base_info")) {
                        trustedRow.set("Base_Info", gson.fromJson(jsonObject.get("Base_Info"), TableRow.class));
                    }

                    // Flatten payload
                    if (jsonObject.has("payload")) {
                        JsonObject payload = jsonObject.getAsJsonObject("payload");

                        for (Map.Entry<String, JsonElement> entry : payload.entrySet()) {
                            String key = entry.getKey();
                            String newKey = fieldsMapping.getProperty(key, key);

                            if ("lineItems".equals(key)) {
                                if (entry.getValue().isJsonArray()) {
                                    JsonArray lineItemsArray = entry.getValue().getAsJsonArray();
                                    List<TableRow> orderLines = new ArrayList<>();
                                    for (JsonElement itemElement : lineItemsArray) {
                                        JsonObject lineItem = itemElement.getAsJsonObject();
                                        TableRow orderLineRow = new TableRow();
                                        orderLineRow.set("Product_Code", lineItem.has("productCode") ? lineItem.get("productCode").getAsString() : null);
                                        orderLineRow.set("Quantity", lineItem.has("quantity") ? lineItem.get("quantity").getAsLong() : null);
                                        orderLineRow.set("Cost_Value", lineItem.has("costValue") ? lineItem.get("costValue").getAsBigDecimal() : null);
                                        orderLines.add(orderLineRow);
                                    }
                                    trustedRow.set(newKey, orderLines);
                                }
                            } else {
                                if (entry.getValue() != null && !entry.getValue().isJsonNull()) {
                                    // Handle type conversions if necessary, e.g., for timestamps
                                    if ("createdAt".equals(key) || "updatedAt".equals(key)) {
                                        trustedRow.set(newKey, entry.getValue().getAsString()); // BQ sink handles String to TIMESTAMP
                                    } else {
                                        trustedRow.set(newKey, entry.getValue().getAsString());
                                    }
                                }
                            }
                        }
                    }

                    c.output(transformOut, trustedRow);

                } catch (Exception e) {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));
                    String exceptionAsString = sw.toString();
                    LOG.error("Error transforming IBT message for trusted layer: {}", exceptionAsString);
                    ErrorTblDtl errorTblDtl = new ErrorTblDtl();
                    c.output(transformFail, errorTblDtl);
                }
            }
        }).withOutputTags(transformOut, TupleTagList.of(transformFail)));
    }
}
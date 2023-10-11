package com.ityulkanov.funcs;

import com.ityulkanov.model.Sale;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ByteArrayToUser extends DoFn<byte[], Sale> {

    private transient GenericDatumReader<GenericRecord> datumReader;

    @Setup
    public void setUp() {
        // TODO make sure it's exists
        Path fileName = Path.of("schema/avro_schema.avsc");
        var schema = "";
        try {
            schema = Files.readString(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Schema avroSchema = new Schema.Parser().parse(schema); // Replace with your actual Avro schema string
        datumReader = new GenericDatumReader<>(avroSchema);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        byte[] avroBytes = c.element();

        Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
        GenericRecord record = datumReader.read(null, decoder);

        Sale sale = new Sale();
        sale.setProductID(record.get("product_id").toString());
        sale.setProductName(record.get("product_name").toString());
        sale.setStoreID(record.get("store_id").toString());
        sale.setPrice(Double.parseDouble(record.get("price").toString()));
        sale.setDiscount(Double.parseDouble(record.get("discount").toString()));
        sale.setUpdatedPrice(Double.parseDouble(record.get("updated_price").toString()));
        sale.setTransactionID(record.get("transaction_id").toString());
        sale.setSalesDate(record.get("sales_date").toString());


        c.output(sale);
    }
}
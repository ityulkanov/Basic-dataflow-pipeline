package com.ityulkanov.funcs;

import com.google.gson.Gson;
import com.ityulkanov.model.Sale;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.ByteArrayOutputStream;

public class ConvertToAvro extends DoFn<String, byte[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        {
            ReflectDatumWriter<Sale> writer = new ReflectDatumWriter<>(Sale.class);
            DataFileWriter<Sale> dataFileWriter = new DataFileWriter<>(writer);
            Gson gson = new Gson();
            String jsonInput = c.element();
            Sale sale = gson.fromJson(jsonInput, Sale.class);
            // get Schema from class
            Schema schema = ReflectData.get().getSchema(Sale.class);
            // Convert User class to Avro and output
            try {
                ByteArrayOutputStream avroOutput = new ByteArrayOutputStream();
                dataFileWriter.create(schema, avroOutput);
                dataFileWriter.append(sale);
                c.output(avroOutput.toByteArray());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
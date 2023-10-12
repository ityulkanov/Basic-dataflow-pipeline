package com.ityulkanov.funcs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.ityulkanov.avro.Sale;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * converting from inner Sale class into ByteArray
 */
public class ConvertToAvro extends DoFn<Sale, byte[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
            Sale sale = c.element();
            DatumWriter<Sale> userDatumWriter = new SpecificDatumWriter<>(Sale.class);
            DataFileWriter<Sale> dataFileWriter = new DataFileWriter<>(userDatumWriter);
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                if (sale != null) {
                    dataFileWriter.create(sale.getSchema(), byteArrayOutputStream);
                    dataFileWriter.append(sale);
                    dataFileWriter.close();
                    c.output(byteArrayOutputStream.toByteArray());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

package com.ityulkanov.funcs;

import com.ityulkanov.avro.Sale;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;

/**
 * Converting raw file into internal Class
 */
@Slf4j
public class ByteArrayToSale extends DoFn<byte[], Sale> {
    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        byte[] avroBytes = c.element();
        DatumReader<Sale> userDatumReader = new SpecificDatumReader<>(Sale.class);
        assert avroBytes != null;
        try (DataFileReader<Sale> fileReader = new DataFileReader<>(new SeekableByteArrayInput(avroBytes), userDatumReader)) {
            for (Sale sale : fileReader) {
                c.output(sale);
                log.debug("Sale: {}", sale);
            }
        }
    }
}
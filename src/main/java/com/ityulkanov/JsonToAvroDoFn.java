package com.ityulkanov;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.transforms.DoFn;
import java.io.ByteArrayOutputStream;

public class JsonToAvroDoFn extends DoFn<String, byte[]> {

    private final String avroSchemaStr;
    private transient Schema avroSchema;

    public JsonToAvroDoFn(String avroSchemaStr) {
        this.avroSchemaStr = avroSchemaStr;
    }

    @Setup
    public void setup() {
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        String jsonInput = context.element();

        try {
            // Decode JSON to Avro GenericRecord
            Decoder jsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, jsonInput);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
            GenericRecord record = reader.read(null, jsonDecoder);

            // Encode GenericRecord to Avro binary
            ByteArrayOutputStream avroOutput = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
            Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(avroOutput, null);
            writer.write(record, binaryEncoder);
            binaryEncoder.flush();

            context.output(avroOutput.toByteArray());

        } catch (Exception e) {
            // Handle exception, e.g., skip bad records, log them, or throw a runtime exception
        }
    }
}
package com.ityulkanov.funcs;

import junit.framework.TestCase;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

@RunWith(JUnit4.class)
public class TrimJsonTest extends TestCase {
    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testTrimJson() {
        String testInput = " {\n" +
                "    \"sales_date\": \"2019-01-01\",\n" +
                "    \"store_id\": \"store_1\",\n" +
                "    \"product_id\": \"product_1\",\n" +
                "    \"product_name\": \"product_name_1\",\n" +
                "    \"price\": 100.0,\n" +
                "    \"discount\": 10.0,\n" +
                "    \"updated_price\": 90.0,\n" +
                "    \"transaction_id\": \"transaction_1\"\n" +
                "  }";
        List<String> input = List.of(testInput);
        PCollection<String> output =
                p.apply(Create.of(input))
                        .apply(ParDo.of(new TrimJson()));
        String expectedOutput = "{\n" +
                "    \"sales_date\": \"2019-01-01\",\n" +
                "    \"store_id\": \"store_1\",\n" +
                "    \"product_id\": \"product_1\",\n" +
                "    \"product_name\": \"product_name_1\",\n" +
                "    \"price\": 100.0,\n" +
                "    \"discount\": 10.0,\n" +
                "    \"updated_price\": 90.0,\n" +
                "    \"transaction_id\": \"transaction_1\"\n" +
                "  }";
        PAssert.that(output).containsInAnyOrder(expectedOutput);

        p.run().waitUntilFinish();
    }


}
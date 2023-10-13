package com.ityulkanov.funcs;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Cleaning json to remove the extra spaces
 */
public class TrimJson extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String rawJson = c.element();
        assert rawJson != null;
        String trimmedJson = rawJson.trim();
        c.output(trimmedJson);
    }
}
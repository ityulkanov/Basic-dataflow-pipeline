package com.ityulkanov.util;


import com.ityulkanov.dataflow.options.CombinedJobCommonOptions;

public class CloudStorageUtil {
    private CloudStorageUtil() {
    }

    public static String getBucketName(CombinedJobCommonOptions options, String outputDirectory) {
        return options.getBucketName() + "/" + outputDirectory;
    }
}

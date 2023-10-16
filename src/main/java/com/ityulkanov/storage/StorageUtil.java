package com.ityulkanov.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
@Slf4j
public class StorageUtil {
    public static String readGCS(String projectID, String bucket, String filePath) {
        Storage storage;
        try {
            storage = StorageOptions.newBuilder()
                    .setProjectId(projectID)
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .build().getService();
        } catch (IOException e) {
            log.error("error instantiating storage object", e);
            throw new RuntimeException(e);
        }
        Blob blob = storage.get(bucket, filePath);
        return new String(blob.getContent());
    }
}

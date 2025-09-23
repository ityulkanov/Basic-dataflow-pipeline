package com.ityulkanov.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesFileReader {

    private PropertiesFileReader() {
    }

    public static Properties readProperties(String propertyFile) {
        Properties prop = new Properties();
        try (InputStream input = PropertiesFileReader.class.getClassLoader().getResourceAsStream(propertyFile)) {
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }
}
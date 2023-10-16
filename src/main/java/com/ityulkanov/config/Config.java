package com.ityulkanov.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import com.ityulkanov.storage.StorageUtil;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class Config implements Serializable{
    public Project project;
    public Source source;
    public Sink sink;


    public static Config LoadConfig(String configPath) {
        ObjectMapper mapper = new ObjectMapper();
        String[] configPathArray = configPath.split(":");
        log.info("loading config file from: " + configPath);
        String configString = StorageUtil.readGCS(configPathArray[0], configPathArray[1], configPathArray[2]);
        try {
            return mapper.readValue(configString, Config.class);
        } catch (JsonProcessingException e) {
            log.error("error while parsing config file", e);
            throw new RuntimeException(e);
        }
    }
}


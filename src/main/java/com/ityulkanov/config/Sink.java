package com.ityulkanov.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sink implements Serializable {
    public String folder;
    public String BQDataset;
    public String BQTable;
}
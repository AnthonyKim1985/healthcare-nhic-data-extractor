package org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParameterInfo implements Serializable {
    private Integer dataSetYear;
    private String databaseName;
    private String tableName;
    private Integer columnType;
    private String columnName;
    private String columnValue;
    private String columnOperator;
    private String header;
}
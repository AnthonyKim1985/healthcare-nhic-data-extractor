package org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.AdjacentTableInfo;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class ParameterKey implements Serializable {
    private Integer dataSetYear;
    private String databaseName;
    private String tableName;
    private String header;
}

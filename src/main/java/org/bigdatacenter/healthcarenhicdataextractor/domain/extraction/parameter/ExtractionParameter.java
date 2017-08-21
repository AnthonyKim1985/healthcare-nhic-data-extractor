package org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.AdjacentTableInfo;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.ParameterInfo;
import org.bigdatacenter.healthcarenhicdataextractor.domain.transaction.TrRequestInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Data
@AllArgsConstructor
public class ExtractionParameter implements Serializable {
    private String databaseName;
    private TrRequestInfo requestInfo;
    private List<ParameterInfo> parameterInfoList;
    private Set<AdjacentTableInfo> adjacentTableInfoSet;
}
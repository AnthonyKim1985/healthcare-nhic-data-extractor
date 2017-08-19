package org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.extraction;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class DataExtractionTask implements Serializable {
    private String hdfsLocation;
    private String hiveQuery;
    private String header;
}

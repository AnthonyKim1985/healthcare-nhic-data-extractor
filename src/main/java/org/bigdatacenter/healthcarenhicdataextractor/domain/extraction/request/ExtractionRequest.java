package org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarenhicdataextractor.domain.transaction.TrRequestInfo;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class ExtractionRequest implements Serializable {
    private TrRequestInfo requestInfo;
    private List<QueryTask> queryTaskList;
}

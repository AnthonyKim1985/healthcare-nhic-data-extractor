package org.bigdatacenter.healthcarenhicdataextractor.service;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.extraction.DataExtractionTask;

public interface RawDataDBService {
    void extractData(DataExtractionTask dataExtractionTask);

    void createTable(TableCreationTask tableCreationTask);
}

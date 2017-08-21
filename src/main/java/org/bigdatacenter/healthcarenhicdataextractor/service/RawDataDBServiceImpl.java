package org.bigdatacenter.healthcarenhicdataextractor.service;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarenhicdataextractor.persistence.RawDataDBMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RawDataDBServiceImpl implements RawDataDBService {
    private final RawDataDBMapper rawDataDBMapper;

    @Autowired
    public RawDataDBServiceImpl(RawDataDBMapper rawDataDBMapper) {
        this.rawDataDBMapper = rawDataDBMapper;
    }

    @Override
    public void extractData(DataExtractionTask dataExtractionTask) {
        rawDataDBMapper.extractData(dataExtractionTask);
    }

    @Override
    public void createTable(TableCreationTask tableCreationTask) {
        rawDataDBMapper.createTable(tableCreationTask);
    }
}
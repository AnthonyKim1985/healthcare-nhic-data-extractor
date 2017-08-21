package org.bigdatacenter.healthcarenhicdataextractor.rabbitmq;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarenhicdataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.script.ShellScriptResolver;
import org.bigdatacenter.healthcarenhicdataextractor.service.RawDataDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.sql.Timestamp;
import java.util.List;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final ShellScriptResolver shellScriptResolver;

    private final RabbitAdmin rabbitAdmin;

    private final RawDataDBService rawDataDBService;

    @Autowired
    public RabbitMQReceiverImpl(ShellScriptResolver shellScriptResolver, RabbitAdmin rabbitAdmin, RawDataDBService rawDataDBService) {
        this.shellScriptResolver = shellScriptResolver;
        this.rabbitAdmin = rabbitAdmin;
        this.rawDataDBService = rawDataDBService;
    }

    @Override
    public void runReceiver(ExtractionRequest extractionRequest) {
        if (extractionRequest == null)
            throw new NullPointerException(String.format("%s - Error occurs at RabbitMQReceiver: extraction request is null", currentThreadName));
        else if (extractionRequest.getQueryTaskList().size() == 0)
            throw new NullPointerException(String.format("%s - Error occurs at RabbitMQReceiver: There are no tasks to process.", currentThreadName));

        final String databaseName = extractionRequest.getDatabaseName();
        final TrRequestInfo requestInfo = extractionRequest.getRequestInfo();
        final Integer dataSetUID = requestInfo.getDataSetUID();

        final RestTemplate restTemplate = new RestTemplate();
        restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());

        runQueryTask(extractionRequest);
        runArchiveTask(databaseName, requestInfo);
    }

    private void runQueryTask(ExtractionRequest extractionRequest) {
        final List<QueryTask> queryTaskList = extractionRequest.getQueryTaskList();
        final int queryTaskListSize = queryTaskList.size();

        for (int i = 0; i < queryTaskListSize; i++) {
            final QueryTask queryTask = queryTaskList.get(i);
            final TableCreationTask tableCreationTask = queryTask.getTableCreationTask();
            final DataExtractionTask dataExtractionTask = queryTask.getDataExtractionTask();

            final long queryBeginTime = System.currentTimeMillis();
            logger.info(String.format("%s - Remaining %d query processing", currentThreadName, (queryTaskListSize - i)));

            if (tableCreationTask != null) {
                logger.info(String.format("%s - Start table creation at Hive Query: %s", currentThreadName, tableCreationTask.getQuery()));
                rawDataDBService.createTable(tableCreationTask);
            }

            if (dataExtractionTask != null) {
                logger.info(String.format("%s - Start data extraction at Hive Query: %s", currentThreadName, dataExtractionTask.getQuery()));
                rawDataDBService.extractData(dataExtractionTask);

                //
                // TODO: Merge Reducer output files in HDFS, download merged file to local file system.
                //
                final String hdfsLocation = dataExtractionTask.getHdfsLocation();
                final String header = dataExtractionTask.getHeader();
                shellScriptResolver.runReducePartsMerger(hdfsLocation, header);
            }

            //
            // TODO: Update transaction database
            //

            final long queryEndTime = System.currentTimeMillis() - queryBeginTime;
            logger.info(String.format("%s - Finish Hive Query: %s, Elapsed time: %d ms", currentThreadName, queryTask, queryEndTime));
        }
    }

    private void runArchiveTask(String databaseName, TrRequestInfo requestInfo) {
        //
        // TODO: Archive the extracted data set and finally send the file to FTP server.
        //
        final String archiveFileName = String.format("%s_%s.tar.gz", requestInfo.getUserID(), String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
        final String ftpLocation = String.format("/%s/%s", requestInfo.getUserID(), databaseName);

        final long archiveFileBeginTime = System.currentTimeMillis();
        logger.info(String.format("%s - Start archiving the extracted data set: %s", currentThreadName, archiveFileName));
        shellScriptResolver.runArchiveExtractedDataSet(archiveFileName, ftpLocation);
        logger.info(String.format("%s - Finish archiving the extracted data set: %s, Elapsed time: %d ms", currentThreadName, archiveFileName, (System.currentTimeMillis() - archiveFileBeginTime)));

        //
        // TODO: Update meta database
        //
        final String ftpURI = String.format("%s/%s", ftpLocation, archiveFileName);
        metadbService.insertFtpRequest(new FtpInfo(requestInfo.getDataSetUID(), requestInfo.getUserID(), ftpURI));
    }
}
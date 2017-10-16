package org.bigdatacenter.healthcarenhicdataextractor.api;

import com.google.gson.Gson;
import org.bigdatacenter.healthcarenhicdataextractor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarenhicdataextractor.config.RabbitMQConfig;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.response.ExtractionResponse;
import org.bigdatacenter.healthcarenhicdataextractor.exception.RESTException;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction.ExtractionRequestResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@RequestMapping("/extraction/api")
public class DataExtractionController {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);
    private static final String currentThreadName = Thread.currentThread().getName();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final RabbitTemplate rabbitTemplate;

    private final ExtractionRequestResolver extractionRequestResolver;

    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    @Autowired
    public DataExtractionController(ExtractionRequestResolver extractionRequestResolver, RabbitTemplate rabbitTemplate, DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller) {
        this.extractionRequestResolver = extractionRequestResolver;
        this.rabbitTemplate = rabbitTemplate;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
    }

    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "dataExtraction", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ExtractionResponse dataExtraction(@RequestBody ExtractionParameter extractionParameter, HttpServletResponse httpServletResponse) {
        if (extractionParameter == null) {
            throw new RESTException(String.format("(dataSetUID=null / threadName=%s) - The extractionParameter is null.", currentThreadName), httpServletResponse);
        } else if (extractionParameter.getRequestInfo() == null) {
            throw new RESTException(String.format("(dataSetUID=null / threadName=%s) - The requestInfo at extractionParameter is null.", currentThreadName), httpServletResponse);
        } else if (extractionParameter.getRequestInfo().getDataSetUID() == null) {
            throw new RESTException(String.format("(dataSetUID=%d / threadName=%s) - The dataSetUID of requestInfo at extractionParameter is null.", extractionParameter.getRequestInfo().getDataSetUID(), currentThreadName), httpServletResponse);
        }

        final ExtractionRequest extractionRequest;
        final ExtractionResponse extractionResponse;
        final Integer dataSetUID = extractionParameter.getRequestInfo().getDataSetUID();

        try {
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - extractionParameter: %s", dataSetUID, currentThreadName, extractionParameter));
            extractionRequest = extractionRequestResolver.buildExtractionRequest(extractionParameter);

            synchronized (this) {
                rabbitTemplate.convertAndSend(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, extractionRequest);
            }

            final String jobAcceptedTime = dateFormat.format(new Date(System.currentTimeMillis()));
            final String jsonForExtractionRequest = new Gson().toJson(extractionRequest, ExtractionRequest.class);
            extractionResponse = new ExtractionResponse(jobAcceptedTime, jsonForExtractionRequest);
        } catch (Exception e) {
            e.printStackTrace();
            dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_REJECTED);
            throw new RESTException(String.format("(dataSetUID=%d / threadName=%s) - Bad request (%s)", dataSetUID, currentThreadName, e.getMessage()), httpServletResponse);
        }

        return extractionResponse;
    }
}
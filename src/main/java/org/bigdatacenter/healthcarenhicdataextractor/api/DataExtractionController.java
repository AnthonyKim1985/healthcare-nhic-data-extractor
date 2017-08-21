package org.bigdatacenter.healthcarenhicdataextractor.api;

import com.google.gson.Gson;
import org.bigdatacenter.healthcarenhicdataextractor.config.RabbitMQConfig;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;
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

@RestController
@RequestMapping("/extraction/api")
public class DataExtractionController {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final RabbitTemplate rabbitTemplate;

    private final ExtractionRequestResolver extractionRequestResolver;

    @Autowired
    public DataExtractionController(ExtractionRequestResolver extractionRequestResolver, RabbitTemplate rabbitTemplate) {
        this.extractionRequestResolver = extractionRequestResolver;
        this.rabbitTemplate = rabbitTemplate;
    }

    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "dataExtraction", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public @ResponseBody String dataExtraction(@RequestBody ExtractionParameter extractionParameter, HttpServletResponse httpServletResponse) {
        final ExtractionRequest extractionRequest;
        try {
            logger.info(String.format("%s - extractionParameter: %s", currentThreadName, extractionParameter));
            extractionRequest = extractionRequestResolver.buildExtractionRequest(extractionParameter);

            synchronized (this) {
                rabbitTemplate.convertAndSend(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, extractionRequest);
            }
        } catch (Exception e) {
            throw new RESTException(String.format("Bad request (%s)", e.getMessage()), httpServletResponse);
        }

        return new Gson().toJson(extractionRequest, ExtractionRequest.class);
    }
}

package org.bigdatacenter.healthcarenhicdataextractor.api;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction.ExtractionRequestResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final ExtractionRequestResolver extractionRequestResolver;

    @Autowired
    public DataExtractionController(ExtractionRequestResolver extractionRequestResolver) {
        this.extractionRequestResolver = extractionRequestResolver;
    }

    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "dataExtraction", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String dataExtraction(@RequestBody ExtractionParameter extractionParameter, HttpServletResponse httpServletResponse) {
//        ExtractionParameter extractionParameter = new Gson().fromJson(json, ExtractionParameter.class);
        logger.info(extractionParameter.toString());
        extractionRequestResolver.buildExtractionRequest(extractionParameter);
        return "OK";
    }

    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "test_api", method = RequestMethod.POST)
    public String testParameterKeyAPI(@RequestBody ParameterKey key) {
        logger.info(key.toString());
        return "OK";
    }
}

package org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;

public interface ExtractionRequestResolver {
    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}

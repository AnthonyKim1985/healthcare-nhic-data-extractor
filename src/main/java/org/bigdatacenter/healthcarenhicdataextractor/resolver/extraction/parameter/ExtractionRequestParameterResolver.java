package org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction.parameter;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.ParameterInfo;

import java.util.List;

public interface ExtractionRequestParameterResolver {
    ExtractionRequestParameter buildRequestParameter(List<ParameterInfo> parameterInfoList);
}

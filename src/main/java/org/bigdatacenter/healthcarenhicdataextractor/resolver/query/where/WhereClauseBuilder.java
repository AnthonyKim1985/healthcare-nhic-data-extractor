package org.bigdatacenter.healthcarenhicdataextractor.resolver.query.where;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterValue;

import java.util.List;

public interface WhereClauseBuilder {
    String buildClause(List<ParameterValue> parameterValueList);
}
package org.bigdatacenter.healthcarenhicdataextractor.resolver.query.join;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.query.JoinParameter;

import java.util.List;

public interface JoinClauseBuilder {
    String buildClause(List<JoinParameter> joinParameterList);

    String buildClause(JoinParameter sourceJoinParameter, JoinParameter targetJoinParameter, Boolean isKogesDataSet);
}

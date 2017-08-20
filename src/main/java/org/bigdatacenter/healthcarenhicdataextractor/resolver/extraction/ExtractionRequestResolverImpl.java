package org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.ParameterInfo;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.query.JoinParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction.parameter.ExtractionRequestParameterResolver;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.query.join.JoinClauseBuilder;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.query.select.SelectClauseBuilder;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.query.where.WhereClauseBuilder;
import org.bigdatacenter.healthcarenhicdataextractor.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ExtractionRequestResolverImpl implements ExtractionRequestResolver {
    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final JoinClauseBuilder joinClauseBuilder;

    private final ExtractionRequestParameterResolver extractionRequestParameterResolver;

    @Autowired
    public ExtractionRequestResolverImpl(SelectClauseBuilder selectClauseBuilder, WhereClauseBuilder whereClauseBuilder, JoinClauseBuilder joinClauseBuilder, ExtractionRequestParameterResolver extractionRequestParameterResolver) {
        this.selectClauseBuilder = selectClauseBuilder;
        this.whereClauseBuilder = whereClauseBuilder;
        this.joinClauseBuilder = joinClauseBuilder;
        this.extractionRequestParameterResolver = extractionRequestParameterResolver;
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        if (extractionParameter == null)
            throw new NullPointerException(String.format("%s - extractionParameter is null.", currentThreadName));

        final TrRequestInfo requestInfo = extractionParameter.getRequestInfo();
        final String joinCondition = requestInfo.getJoinCondition();
        final Integer joinConditionYear = requestInfo.getJoinConditionYear();

        final List<ParameterInfo> parameterInfoList = extractionParameter.getParameterInfoList();
        final ExtractionRequestParameter extractionRequestParameter = extractionRequestParameterResolver.buildRequestParameter(parameterInfoList);

        final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = extractionRequestParameter.getYearParameterMap();
        final Map<Integer/* Year */, Set<ParameterKey>> yearJoinKeyMap = extractionRequestParameter.getYearJoinKeyMap();

        //
        // TODO: 1. 추출 연산을 위한 임시 테이블들을 생성한다.
        //
        for (Integer year : yearParameterMap.keySet()) {
            Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(year);
            Set<ParameterKey> joinTargetKeySet = yearJoinKeyMap.get(year);

            //
            // TODO: 1.1. 조인 기준 연도가 있고 (값이 0 보다 크고) 기준연도가 아니라면 스킵한다.
            //
            if (joinConditionYear > 0)
                if (!Objects.equals(joinConditionYear, year))
                    continue;

            //
            // TODO: 1.2. 임시 테이블 생성 쿼리를 생성한다. (유니크 컬럼을 갖고 있는 테이블만 대상)
            //
            List<JoinParameter> joinParameterList = new ArrayList<>();
            for (ParameterKey parameterKey : joinTargetKeySet) {
                final String databaseName = parameterKey.getDatabaseName();
                final String tableName = parameterKey.getTableName();

                String selectClause = selectClauseBuilder.buildClause(databaseName, tableName, joinCondition);
                logger.info(String.format("%s - selectClause: %s", currentThreadName, selectClause));

                String whereClause = whereClauseBuilder.buildClause(parameterMap.get(parameterKey));
                logger.info(String.format("%s - whereClause: %s", currentThreadName, whereClause));

                String query = String.format("%s %s", selectClause, whereClause);
                logger.info(String.format("%s - query: %s", currentThreadName, query));

                String extrDbName = String.format("%s_extracted", databaseName);
                String extrTableName = String.format("%s_%s", tableName, CommonUtil.getHashedString(query));
                logger.info(String.format("%s - hashedTableName: %s", currentThreadName, extrTableName));

                joinParameterList.add(new JoinParameter(extrDbName, extrTableName, joinCondition, "key_seq"));
            }

            //
            // TODO: 1.3. 임시 데이블들의 조인 연산을 위한 테이블 생성 쿼리를 생성한다.
            //
            String joinQuery = joinClauseBuilder.buildClause(joinParameterList);
            logger.info(String.format("%s - joinQuery: %s", currentThreadName, joinQuery));
        }

        //
        // TODO: 2. 원시 데이터 셋 테이블과 조인연산 수행을 위한 쿼리를 생성한다.
        //

        return null;
    }
}
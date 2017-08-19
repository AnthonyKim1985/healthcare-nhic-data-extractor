package org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.ParameterInfo;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarenhicdataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.query.join.JoinClauseBuilder;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.query.select.SelectClauseBuilder;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.query.where.WhereClauseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;

@Component
public class ExtractionRequestResolverImpl implements ExtractionRequestResolver {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class RequestParameter implements Serializable {
        private Map<Integer/* Year */, List<ParameterKey>> yearJoinKeyMap;
        private Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap;
    }

    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final JoinClauseBuilder joinClauseBuilder;

    @Autowired
    public ExtractionRequestResolverImpl(SelectClauseBuilder selectClauseBuilder, WhereClauseBuilder whereClauseBuilder, JoinClauseBuilder joinClauseBuilder) {
        this.selectClauseBuilder = selectClauseBuilder;
        this.whereClauseBuilder = whereClauseBuilder;
        this.joinClauseBuilder = joinClauseBuilder;
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        if (extractionParameter == null)
            throw new NullPointerException(String.format("%s - extractionParameter is null.", currentThreadName));

        final TrRequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Integer joinConditionYear = requestInfo.getJoinConditionYear();
        final List<ParameterInfo> parameterInfoList = extractionParameter.getParameterInfoList();
        final RequestParameter requestParameter = buildRequestParameter(parameterInfoList);

        final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = requestParameter.getYearParameterMap();
        final Map<Integer/* Year */, List<ParameterKey>> yearJoinKeyMap = requestParameter.getYearJoinKeyMap();

        for (Integer year : yearParameterMap.keySet()) {
            Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(year);
            List<ParameterKey> joinTargetKeyList = yearJoinKeyMap.get(year);

            //
            // TODO: 조인 기준 연도가 있고(값이 0 보다 크고) 기준연도가 아니라면 스킵한다.
            //
            if (joinConditionYear > 0)
                if (!Objects.equals(joinConditionYear, year))
                    continue;

            //
            // TODO: 데이터 추출 쿼리를 생성한다. (유니크 컬럼을 갖고 있는 테이블만 대상)
            //
            for (ParameterKey parameterKey : joinTargetKeyList) {
                String selectClause = selectClauseBuilder.buildClause(parameterKey.getDatabaseName(), parameterKey.getTableName(), parameterKey.getHeader());
                logger.info(String.format("%s - selectClause: %s", currentThreadName, selectClause));

                String whereClause = whereClauseBuilder.buildClause(parameterMap.get(parameterKey));
                logger.info(String.format("%s - whereClause: %s", currentThreadName, whereClause));
            }

            //
            // TODO: 데이블 조인 쿼리를 생성한다.
            //
        }


        return null;
    }

    private RequestParameter buildRequestParameter(List<ParameterInfo> parameterInfoList) {
        final Map<Integer/* Year */, List<ParameterKey>> yearJoinKeyMap = new HashMap<>();
        final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = new HashMap<>();

        for (ParameterInfo parameterInfo : parameterInfoList) {
            final Integer dataSetYear = parameterInfo.getDataSetYear();
            final String databaseName = parameterInfo.getDatabaseName();
            final String tableName = parameterInfo.getTableName();
            final String header = parameterInfo.getHeader();

            final Integer columnType = parameterInfo.getColumnType();
            final String columnName = parameterInfo.getColumnName();
            final String columnValue = parameterInfo.getColumnValue();

            ParameterKey parameterKey = new ParameterKey(dataSetYear, databaseName, tableName, header);
            ParameterValue parameterValue = new ParameterValue(columnType, columnName, columnValue);

            if (columnType == 1) {
                List<ParameterKey> joinKeyList = yearJoinKeyMap.get(dataSetYear);

                //noinspection Duplicates
                if (joinKeyList == null) {
                    joinKeyList = new ArrayList<>();

                    joinKeyList.add(parameterKey);
                    yearJoinKeyMap.put(dataSetYear, joinKeyList);
                } else {
                    joinKeyList.add(parameterKey);
                }
            }

            List<ParameterValue> parameterValueList;
            Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(dataSetYear);

            if (parameterMap == null) {
                parameterMap = new HashMap<>();
                parameterValueList = new ArrayList<>();

                parameterValueList.add(parameterValue);
                parameterMap.put(parameterKey, parameterValueList);
                yearParameterMap.put(dataSetYear, parameterMap);
            } else {
                parameterValueList = parameterMap.get(parameterKey);

                //noinspection Duplicates
                if (parameterValueList == null) {
                    parameterValueList = new ArrayList<>();

                    parameterValueList.add(parameterValue);
                    parameterMap.put(parameterKey, parameterValueList);
                } else {
                    parameterValueList.add(parameterValue);
                }
            }
        }

        return new RequestParameter(yearJoinKeyMap, yearParameterMap);
    }
}
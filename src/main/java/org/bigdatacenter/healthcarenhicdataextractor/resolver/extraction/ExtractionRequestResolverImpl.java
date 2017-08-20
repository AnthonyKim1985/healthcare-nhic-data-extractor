package org.bigdatacenter.healthcarenhicdataextractor.resolver.extraction;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.info.ParameterInfo;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.query.JoinParameter;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.task.creation.TableCreationTask;
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
        final String databaseName = extractionParameter.getDatabaseName();
        final String joinCondition = requestInfo.getJoinCondition();
        final Integer joinConditionYear = requestInfo.getJoinConditionYear();

        final List<ParameterInfo> parameterInfoList = extractionParameter.getParameterInfoList();
        final ExtractionRequestParameter extractionRequestParameter = extractionRequestParameterResolver.buildRequestParameter(parameterInfoList);

        final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = extractionRequestParameter.getYearParameterMap();
        final Map<Integer/* Year */, Set<ParameterKey>> yearJoinKeyMap = extractionRequestParameter.getYearJoinKeyMap();

        final List<QueryTask> queryTaskList = new ArrayList<>();

        //
        // TODO: 1. 추출 연산을 위한 임시 테이블들을 생성한다.
        //
        for (Integer year : yearParameterMap.keySet()) {
            Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(year);
            Set<ParameterKey> joinTargetKeySet = yearJoinKeyMap.get(year);

            //
            // TODO: 1.1. 조인 대상키가 없으면 해당 연도를 스킵힌다.
            //
            if (joinTargetKeySet == null || joinTargetKeySet.isEmpty())
                continue;

            //
            // TODO: 1.2. 조인 기준 연도가 있고 (값이 0 보다 크고) 기준연도가 아니라면 스킵한다.
            //
            if (joinConditionYear > 0)
                if (!Objects.equals(joinConditionYear, year))
                    continue;

            //
            // TODO: 1.3. 임시 테이블 생성 쿼리를 생성한다. (유니크 컬럼을 갖고 있는 테이블만 대상)
            //
            List<JoinParameter> joinParameterList = new ArrayList<>();
            for (ParameterKey parameterKey : joinTargetKeySet) {
                final String tableName = parameterKey.getTableName();

                final String selectClause = selectClauseBuilder.buildClause(databaseName, tableName, joinCondition);
                final String whereClause = whereClauseBuilder.buildClause(parameterMap.get(parameterKey));
                final String query = String.format("%s %s", selectClause, whereClause);
                logger.info(String.format("%s - query: %s", currentThreadName, query));

                final String extrDbName = String.format("%s_extracted", databaseName);
                final String extrTableName = String.format("%s_%s", tableName, CommonUtil.getHashedString(query));
                final String dbAndHashedTableName = String.format("%s.%s", extrDbName, extrTableName);
                logger.info(String.format("%s - dbAndHashedTableName: %s", currentThreadName, dbAndHashedTableName));

                TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, query);

                queryTaskList.add(new QueryTask(tableCreationTask, null));
                joinParameterList.add(new JoinParameter(extrDbName, extrTableName, joinCondition, "key_seq"));
            }

            //
            // TODO: 1.4. 임시 데이블들의 조인 연산을 위한 테이블 생성 쿼리를 생성한다.
            //
            final String joinQuery = joinClauseBuilder.buildClause(joinParameterList);
            logger.info(String.format("%s - joinQuery: %s", currentThreadName, joinQuery));

            final String joinDbName = String.format("%s_join_%s_integrated", databaseName, joinCondition);
            final String joinTableName = String.format("%s_%s", getJoinedTableName(joinTargetKeySet), CommonUtil.getHashedString(joinQuery));
            final String dbAndHashedTableName = String.format("%s.%s", joinDbName, joinTableName);

            TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, joinQuery);
            queryTaskList.add(new QueryTask(tableCreationTask, null));
        }

        //
        // TODO: 2. 원시 데이터 셋 테이블과 조인연산 수행을 위한 쿼리를 생성한다.
        //

        return new ExtractionRequest(requestInfo, queryTaskList);
    }

    private String getJoinedTableName(Set<ParameterKey> joinTargetKeySet) {
        final List<ParameterKey> joinTargetKeyList = new ArrayList<>(joinTargetKeySet);
        final StringBuilder joinedTableNameBuilder = new StringBuilder();
        final Integer joinTargetKeyListSize = joinTargetKeyList.size();

        for (int i = 0; i < joinTargetKeyListSize; i++) {
            ParameterKey parameterKey = joinTargetKeyList.get(i);
            joinedTableNameBuilder.append(parameterKey.getTableName());

            if (i < joinTargetKeyListSize - 1)
                joinedTableNameBuilder.append("__");
        }

        return joinedTableNameBuilder.toString();
    }
}
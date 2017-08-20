package org.bigdatacenter.healthcarenhicdataextractor.resolver.query.join;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.query.JoinParameter;
import org.bigdatacenter.healthcarenhicdataextractor.resolver.query.select.SelectClauseBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JoinClauseBuilderImpl implements JoinClauseBuilder {
    private final SelectClauseBuilder selectClauseBuilder;

    @Autowired
    public JoinClauseBuilderImpl(SelectClauseBuilder selectClauseBuilder) {
        this.selectClauseBuilder = selectClauseBuilder;
    }

    @Override
    public String buildClause(List<JoinParameter> joinParameterList) {
        if (joinParameterList == null || joinParameterList.isEmpty())
            return null;

        final StringBuilder joinQueryBuilder = new StringBuilder();

        final char entryTableAlias = 'A';
        final JoinParameter entryJoinParameter = joinParameterList.get(0);

        if (joinParameterList.size() == 1) {
            joinQueryBuilder.append(selectClauseBuilder.buildClause(entryJoinParameter.getDatabaseName(), entryJoinParameter.getTableName(), entryJoinParameter.getProjection()));
        } else {
            joinQueryBuilder.append(String.format("SELECT DISTINCT %c.%s FROM %s.%s %c",
                    entryTableAlias, entryJoinParameter.getProjection(), entryJoinParameter.getDatabaseName(), entryJoinParameter.getTableName(), entryTableAlias));

            for (int i = 1; i < joinParameterList.size(); i++) {
                JoinParameter joinParameter = joinParameterList.get(i);

                final char prevTableAlias = (char) (entryTableAlias + i - 1);
                final char currentTableAlias = (char) (entryTableAlias + i);

                joinQueryBuilder.append(String.format(" INNER JOIN %s.%s %c ON (%c.%s = %c.%s)",
                        joinParameter.getDatabaseName(), joinParameter.getTableName(), currentTableAlias,
                        prevTableAlias, joinParameter.getJoinKey(),
                        currentTableAlias, joinParameter.getJoinKey()));
            }
        }

        return joinQueryBuilder.toString();
    }
}

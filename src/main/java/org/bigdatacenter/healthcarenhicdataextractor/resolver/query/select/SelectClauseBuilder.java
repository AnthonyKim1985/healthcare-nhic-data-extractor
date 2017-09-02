package org.bigdatacenter.healthcarenhicdataextractor.resolver.query.select;

public interface SelectClauseBuilder {
    String buildClause(String dbName, String tableName);

    String buildClause(String dbName, String tableName, String projections, Boolean enableDistinct);

    String buildClause(String dbName, String tableName, String projections, String snpRs, Integer affy5MapNumber);
}
package org.bigdatacenter.healthcarenhicdataextractor.api.caller.statistic;

public interface StatisticAPICaller {
    void callCreateStatistic(Integer dataSetUID, String databaseName, String tableName);
}
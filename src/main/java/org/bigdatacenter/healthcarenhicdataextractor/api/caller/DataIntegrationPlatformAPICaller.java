package org.bigdatacenter.healthcarenhicdataextractor.api.caller;

public interface DataIntegrationPlatformAPICaller {
    Integer PROCESS_STATE_CODE_COMPLETED = 1;
    Integer PROCESS_STATE_CODE_PROCESSING = 2;
    Integer PROCESS_STATE_CODE_REJECTED = 3;

    void callUpdateJobStartTime(Integer dataSetUID, Long jobStartTime);

    void callUpdateJobEndTime(Integer dataSetUID, Long jobEndTime);

    void callUpdateElapsedTime(Integer dataSetUID, Long elapsedTime);

    void callUpdateProcessState(Integer dataSetUID, Integer processState);
}
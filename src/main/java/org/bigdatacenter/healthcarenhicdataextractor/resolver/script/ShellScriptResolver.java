package org.bigdatacenter.healthcarenhicdataextractor.resolver.script;

public interface ShellScriptResolver {
    void runReducePartsMerger(String hdfsLocation, String header, String homePath);

    void runArchiveExtractedDataSet(String archiveFileName, String ftpLocation, String homePath);
}

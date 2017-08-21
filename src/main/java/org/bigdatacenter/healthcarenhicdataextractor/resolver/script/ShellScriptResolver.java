package org.bigdatacenter.healthcarenhicdataextractor.resolver.script;

public interface ShellScriptResolver {
    void runReducePartsMerger(String hdfsLocation, String header);

    void runArchiveExtractedDataSet(String archiveFileName, String ftpLocation);
}

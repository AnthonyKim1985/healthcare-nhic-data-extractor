package org.bigdatacenter.healthcarenhicdataextractor.rabbitmq;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;

public interface RabbitMQReceiver {
    void runReceiver(ExtractionRequest extractionRequest);
}

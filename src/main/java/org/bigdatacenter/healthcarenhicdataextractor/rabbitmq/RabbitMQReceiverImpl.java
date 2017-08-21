package org.bigdatacenter.healthcarenhicdataextractor.rabbitmq;

import org.bigdatacenter.healthcarenhicdataextractor.domain.extraction.request.ExtractionRequest;
import org.springframework.stereotype.Component;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    @Override
    public void runReceiver(ExtractionRequest extractionRequest) {

    }
}

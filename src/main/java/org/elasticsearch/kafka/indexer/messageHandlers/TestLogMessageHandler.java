package org.elasticsearch.kafka.indexer.messageHandlers;


import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.kafka.indexer.ConsumerConfig;
import org.elasticsearch.kafka.indexer.MessageHandler;
import org.elasticsearch.kafka.indexer.mappers.TestLogMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by fpschina on 16-1-25.
 */
public class TestLogMessageHandler extends MessageHandler {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TestLogMessageHandler.class);

    private String[] slits = null;
    private TestLogMapper testLogMapper = null;
    private ObjectMapper mapper = new ObjectMapper();

    public TestLogMessageHandler(TransportClient transportClient, ConsumerConfig consumerConfig) throws Exception {
        super(transportClient, consumerConfig);
        logger.info("Initialized org.elasticsearch.kafka.consumer.messageHandlers.TestLogMessageHandler");

    }


    @Override
    public byte[] transformMessage(String str) throws Exception {
        return str.getBytes();
    }




}

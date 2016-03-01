package org.elasticsearch.kafka.indexer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.jute.Index;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.kafka.indexer.jobs.IndexerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaClient {


    private static final Logger logger = LoggerFactory.getLogger(KafkaClient.class);

    //使用high lever AIP KafkaConsumer
    private String kafkaClientId;
    private IndexerJob indexerJob;
    private ConsumerConnector consumerConnector;
    private String topic;
    private final ConsumerConfig consumerConfig;
    private String zookeeperList;
    private int count = 0;



    public KafkaClient(final ConsumerConfig config, String kafkaClientId, String topic, IndexerJob indexerJob) throws Exception {
        logger.info("Instantiating KafkaClient");
        this.consumerConfig = config;
        this.topic = topic;
        this.kafkaClientId = kafkaClientId;
        zookeeperList = config.kafkaZookeeperList.trim();
        this.indexerJob = indexerJob;
        logger.info("### KafkaClient Config: ###");
        logger.info("kafkaZookeeperList: {}", config.kafkaZookeeperList);
        logger.info("kafkaBrokersList: {}", config.kafkaBrokersList);
        logger.info("kafkaClientId: {}", kafkaClientId);
        logger.info("topic: {}", topic);
        initConsumer();

    }


    public void initConsumer() throws Exception {
        try {
            Properties props = new Properties();
            props.put("zookeeper.connect", zookeeperList);
            props.put("group.id", kafkaClientId);
            props.put("zookeeper.session.timeout.ms", "4000");
            props.put("zookeeper.sync.time.ms", "200");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.offset.reset", "smallest");//
            props.put("serializer.class", "kafka.serializer.StringEncode"
                    + "r");

            kafka.consumer.ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(props);
            consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);


            logger.info("Initialized Kafka Consumer successfully for topic {}", topic);
        } catch (Exception e) {
            logger.error("Failed to initialize Kafka Consumer: " + e.getMessage());
            throw e;
        }
    }



	public void getMessagesFromKafka() {
		logger.debug("Starting getMessagesFromKafka() ...");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(
                new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector
                .createMessageStreams(topicCountMap, keyDecoder, valueDecoder);

        KafkaStream<String, String> stream = consumerMap.get(
                "test").get(0);
        ConsumerIterator<String, String> it = stream.iterator();

        while (it.hasNext()) {

            IndexerJob.messageList.add(it.next().message());
            if(IndexerJob.messageList.size() ==  consumerConfig.numOfWriteToES){
                indexerJob.prepareMessage();
            }
        }

	}

    public void close() {
        consumerConnector.shutdown();
    }


}

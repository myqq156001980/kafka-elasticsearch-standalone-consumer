package org.elasticsearch.kafka.indexer.jobs;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.kafka.indexer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class IndexerJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(IndexerJob.class);
    private ConsumerConfig consumerConfig;
    private MessageHandler msgHandler;
    private TransportClient esClient;
    public static MessageList messageList = new MessageList();
    public KafkaClient kafkaConsumerClient;


    private final String currentTopic;
    private int esIndexingRetrySleepTimeMs;
    private int numberOfEsIndexingRetryAttempts;
    private IndexerJobStatus indexerJobStatus;

    public IndexerJob(ConsumerConfig config, String topic) throws Exception {
        this.consumerConfig = config;
        this.currentTopic = topic;
        messageList.setTopic(topic);

        //初始化job 的状态
        indexerJobStatus = new IndexerJobStatus(-1L, IndexerJobStatusEnum.Created, topic);
        esIndexingRetrySleepTimeMs = config.getEsIndexingRetrySleepTimeMs();
        numberOfEsIndexingRetryAttempts = config.getNumberOfEsIndexingRetryAttempts();
        initElasticSearch();
        initKafka();
        createMessageHandler();
        indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Initialized);
    }

    void initKafka() throws Exception {
        logger.info("Initializing Kafka for topic {}...", currentTopic);
        String consumerGroupName = consumerConfig.consumerGroupName;
        if (consumerGroupName.isEmpty()) {
            consumerGroupName = "es_indexer";
        }
        String kafkaClientId = consumerGroupName + "_" + currentTopic;
        logger.info("kafkaClientId={} for topic {}", kafkaClientId, currentTopic);
        kafkaConsumerClient = new KafkaClient(consumerConfig, kafkaClientId, currentTopic, this);
        logger.info("Kafka client created and intialized OK for topic {}", currentTopic);
    }

    private void initElasticSearch() throws Exception {
        String[] esHostPortList = consumerConfig.esHostPortList.trim().split(",");
        logger.info("Initializing ElasticSearch... hostPortList={}, esClusterName={}",
                consumerConfig.esHostPortList, consumerConfig.esClusterName);

        // TODO add validation of host:port syntax - to avoid Runtime exceptions
        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", consumerConfig.esClusterName)
                    .build();
            esClient = TransportClient.builder().settings(settings).build();
            for (String eachHostPort : esHostPortList) {
                logger.info("adding [{}] to TransportClient for topic {}... ", eachHostPort, currentTopic);
                esClient.addTransportAddress(new InetSocketTransportAddress(
                        new InetSocketAddress(InetAddress.getByName(eachHostPort.split(":")[0].trim()), Integer.parseInt(eachHostPort.split(":")[1].trim()))

                ));
            }
            logger.info("ElasticSearch Client created and intialized OK for topic {}", currentTopic);
        } catch (Exception e) {
            logger.error("Exception when trying to connect and create ElasticSearch Client. Throwing the error. Error Message is::"
                    + e.getMessage());
            throw e;
        }
    }


    private void createMessageHandler() throws Exception {
        try {
            logger.info("MessageHandler Class given in config is {} for topic {}", consumerConfig.messageHandlerClass, currentTopic);
            msgHandler = (MessageHandler) Class
                    .forName(consumerConfig.messageHandlerClass)
                    .getConstructor(TransportClient.class, ConsumerConfig.class)
                    .newInstance(esClient, consumerConfig);
            logger.debug("Created and initialized MessageHandler: {} for topic {}", consumerConfig.messageHandlerClass, currentTopic);
        } catch (Exception e) {
            logger.error("Exception creating MessageHandler class for topic {}: ", currentTopic, e);
            throw e;
        }
    }

    private void indexIntoESWithRetries() throws IndexerESException, Exception {
        try {
            logger.info("posting the messages to ElasticSearch for topic {}...", currentTopic);
            msgHandler.postToElasticSearch();
        } catch (NoNodeAvailableException e) {
            // ES cluster is unreachable or down. Re-try up to the configured number of times
            // if fails even after then - shutdown the current IndexerJob
            for (int i = 1; i <= numberOfEsIndexingRetryAttempts; i++) {
                Thread.sleep(esIndexingRetrySleepTimeMs);
                logger.info("Retrying indexing into ES after a connection failure, CurrentTopic {}, try# {}",
                        currentTopic, i);
                try {
                    msgHandler.postToElasticSearch();
                    // we succeeded - get out of the loop
                    break;
                } catch (NoNodeAvailableException e2) {
                    if (i < numberOfEsIndexingRetryAttempts) {
                        // do not fail yet - will re-try again
                        logger.error("Retrying indexing into ES after a connection failure, topic {}, try# {} - failed again",
                                currentTopic, i);
                    } else {
                        //we've exhausted the number of retries - throw a IndexerESException to stop the IndexerJob thread
                        logger.error("Retrying indexing into ES after a connection failure, topic {}, "
                                        + "try# {} - failed after the last retry; Will shot down the job",
                                currentTopic, i);
                        throw new IndexerESException("Indexing into ES failed due to connectivity issue to ES, topic: " +
                                currentTopic);
                    }
                }
            }
        } catch (ElasticsearchException e) {
            // we are assuming that other exceptions are data-specific
            // -  continue and commit the offset,
            // but be aware that ALL messages from this batch are NOT indexed into ES
            logger.error(e.getMessage());
        }

    }

    public void stopClients() {
        logger.info("About to stop ES client for topic {}",
                currentTopic);
        if (esClient != null)
            esClient.close();

    }

    public IndexerJobStatus getIndexerJobStatus() {
        return indexerJobStatus;
    }

    @Override
    public void run() {

        indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Started);
        try {
            // check if there was a request to stop this thread - stop processing if so
            if (Thread.currentThread().isInterrupted()) {
                // preserve interruption state of the thread
                Thread.currentThread().interrupt();
                throw new InterruptedException(
                        "Cought interrupted event in IndexerJob for topic=" + currentTopic + " - stopping");
            }
            logger.debug("******* Starting a new batch of events from Kafka for topic {} ...", currentTopic);
            indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);
            //开始获取数据
            kafkaConsumerClient.getMessagesFromKafka();
            // sleep for configured time
            // TODO improve sleep pattern
            logger.debug("Completed a round of indexing into ES for topic {}", currentTopic);
        } catch (InterruptedException e) {
            indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Stopped);
            stopClients();
        }
        logger.warn("******* Indexing job was stopped, indexerJobStatus={} - exiting", indexerJobStatus);

    }

    public void prepareMessage(){

        msgHandler.prepareForPostToElasticSearch(messageList);
        messageList.clear();

        try {
            indexIntoESWithRetries();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public void stopJob(){
        kafkaConsumerClient.close();
        esClient.close();
        prepareMessage();
    }




}

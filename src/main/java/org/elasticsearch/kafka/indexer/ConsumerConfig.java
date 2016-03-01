package org.elasticsearch.kafka.indexer;

import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerConfig {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerConfig.class);
    private Properties prop = new Properties();

    // Kafka ZooKeeper's IP Address/HostName : port list
    public final String kafkaZookeeperList;

    // Kafka Broker's IP Address/HostName : port list
    public final String kafkaBrokersList;

    // Name of the Kafka Consumer Group
    public final String consumerGroupName;

    // Kafka Topics from which the message has to be processed
    public final String topicList;

    //timeout in seconds before force-stopping Indexer app and all indexer jobs
    public final int appStopTimeoutSeconds;

    // Name of the ElasticSearch Host Port List
    public final String esHostPortList;

    // Name of the ElasticSearch Cluster
    public final String esClusterName;

    // Full class path and name for the concrete message handler class
    public final String messageHandlerClass;

    // Preferred Message Encoding to process the message before posting it to ElasticSearch
    public final String messageEncoding;

    // number of times to try to index data into ES if ES cluster is not reachable
    public final int numberOfEsIndexingRetryAttempts;

    // sleep time in ms between attempts to index data into ES again
    public final int esIndexingRetrySleepTimeMs;

    // flag to enable/disable performance metrics reporting
    public boolean isPerfReportingEnabled;

    public final int numOfWriteToES;

    // Log property file for the consumer instance
    public final String logPropertyFile;


    public ConsumerConfig(String configFile) throws Exception {
        try {
            logger.info("configFile : " + configFile);
            prop.load(new FileInputStream(configFile));

            logger.info("Properties : " + prop);
        } catch (Exception e) {
            logger.error("Error reading/loading configFile: " + e.getMessage(), e);
            throw e;
        }


        kafkaZookeeperList = prop.getProperty("kafkaZookeeperList", "localhost:2181");

        kafkaBrokersList = prop.getProperty("kafkaBrokersList", "localhost:9092");

        consumerGroupName = prop.getProperty("consumerGroupName", "ESKafkaConsumerClient");

        topicList = prop.getProperty("topicList", "");

        appStopTimeoutSeconds = Integer.parseInt(prop.getProperty("appStopTimeoutSeconds", "10"));

        esHostPortList = prop.getProperty("esHostPortList", "localhost:9300");

        esClusterName = prop.getProperty("esClusterName", "");

        messageHandlerClass = prop.getProperty("messageHandlerClass",
                "org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler");

        messageEncoding = prop.getProperty("messageEncoding", "UTF-8");

        numberOfEsIndexingRetryAttempts = Integer.parseInt(prop.getProperty(
                "numberOfEsIndexingRetryAttempts", "2"));

        esIndexingRetrySleepTimeMs = Integer.parseInt(prop.getProperty(
                "esIndexingRetrySleepTimeMs", "1000"));

        numOfWriteToES = Integer.parseInt(prop.getProperty("numOfWriteToES", "5000"));

        isPerfReportingEnabled = Boolean.getBoolean(prop.getProperty(
                "isPerfReportingEnabled", "false"));

        logPropertyFile = prop.getProperty("logPropertyFile",
                "log4j.properties");

        logger.info("Config reading complete !");
    }

    public boolean isPerfReportingEnabled() {
        return isPerfReportingEnabled;
    }

    public Properties getProperties() {
        return prop;
    }

    public int getEsIndexingRetrySleepTimeMs() {
        return esIndexingRetrySleepTimeMs;
    }

    public int getNumberOfEsIndexingRetryAttempts() {
        return numberOfEsIndexingRetryAttempts;
    }

}

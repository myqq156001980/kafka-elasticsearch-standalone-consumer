package org.elasticsearch.kafka.indexer.jobs;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.elasticsearch.kafka.indexer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class IndexerJobManager {

    private static final Logger logger = LoggerFactory.getLogger(IndexerJobManager.class);
    private static final String KAFKA_CONSUMER_STREAM_POOL_NAME_FORMAT = "kafka-indexer-consumer-thread-%d";
    private ConsumerConfig consumerConfig;
    private ExecutorService executorService;
    //topic数量
    private int numOfTopics;
    private String[] topics;

    // Map of <partitionNumber, IndexerJob> of indexer jobs for all partitions
    private ConcurrentHashMap<String, IndexerJob> indexerJobs;
    // List of <Future<IndexerJobStatus>> futures of all submitted indexer jobs for all partitions
    private List<Future<IndexerJobStatus>> indexerJobFutures;

    public IndexerJobManager(ConsumerConfig config) throws Exception {
        this.consumerConfig = config;
        //获取 topic 数量
        topics = config.topicList.trim().split(",");
        this.numOfTopics = topics.length;

        logger.info("ConsumerJobManager is starting, servicing topic number is: [{}]", numOfTopics);
    }

    public void startAll() throws Exception {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(KAFKA_CONSUMER_STREAM_POOL_NAME_FORMAT).build();
        //初始化线程池
//        executorService = Executors.newFixedThreadPool(numOfTopics, threadFactory);
        executorService = Executors.newCachedThreadPool(threadFactory);

        //线程安全的 hashmap
        indexerJobs = new ConcurrentHashMap<>();
        // create as many IndexerJobs as there are partitions in the events topic
        // first create all jobs without starting them - to make sure they can init all resources OK
        try {

            for (int i = 0; i < numOfTopics; i++) {
                logger.info("Creating IndexerJob for topic={}", topics[i]);
                IndexerJob pIndexJob = new IndexerJob(consumerConfig, topics[i]);
                indexerJobs.put(topics[i], pIndexJob);
                executorService.execute(pIndexJob);
            }

        } catch (Exception e) {
            logger.error("ERROR: Failure creating a consumer job, exiting: ", e);
            // if any job startup fails - abort;
            throw e;
        }
    }

    public List<IndexerJobStatus> getJobStatuses() {
        List<IndexerJobStatus> indexerJobStatuses = new ArrayList<IndexerJobStatus>();
        for (IndexerJob indexerJob : indexerJobs.values()) {
            indexerJobStatuses.add(indexerJob.getIndexerJobStatus());
        }
        return indexerJobStatuses;
    }

    public void stop() {
        logger.info("About to stop all consumer jobs ...");
        if (executorService != null && !executorService.isTerminated()) {
            try {
                executorService.awaitTermination(consumerConfig.appStopTimeoutSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("ERROR: failed to stop all consumer jobs due to InterruptedException: ", e);
            }
        }
        logger.info("Stop() finished OK");
    }

    public void startSinleThread(IndexerJob indexerJob){

        executorService.execute(indexerJob);
    }

    public ConcurrentHashMap<String, IndexerJob> getIndexerJobs() {
        return indexerJobs;
    }

    public void setIndexerJobs(ConcurrentHashMap<String, IndexerJob> indexerJobs) {
        this.indexerJobs = indexerJobs;
    }

    public void startNewJob(){


    }
}

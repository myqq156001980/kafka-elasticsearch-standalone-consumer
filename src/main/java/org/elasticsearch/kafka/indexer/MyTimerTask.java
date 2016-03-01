package org.elasticsearch.kafka.indexer;

import org.elasticsearch.kafka.indexer.jobs.IndexerJob;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by fpschina on 16/2/16.
 */
public class MyTimerTask extends TimerTask {

    Logger logger = LoggerFactory.getLogger(MyTimerTask.class);
    String topics;

    private String path;
    private ConsumerConfig consumerConfig;
    private IndexerJobManager indexerJobManager;
    private Properties prop = new Properties();

    public MyTimerTask(String path, ConsumerConfig consumerConfig, IndexerJobManager indexerJobManager) {
        this.path = path;
        this.consumerConfig = consumerConfig;
        this.indexerJobManager = indexerJobManager;
        try {
            prop.load(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

    }

    @Override
    public void run() {
        topics = prop.getProperty("topicList", "");
        String[] topicList = topics.trim().split(",");
        ConcurrentHashMap<String, IndexerJob> indexerJobs = indexerJobManager.getIndexerJobs();
        ArrayList<String> list = new ArrayList<>();
        Set keySet = indexerJobs.keySet();

        Iterator iterator = keySet.iterator();

        while (iterator.hasNext()){
            String key = (String)iterator.next();
            if(!list.contains(key)){
                IndexerJob i = indexerJobs.get(key);
                i.stopJob();
                iterator.remove();
            }
        }

        for (int i = 0; i < topicList.length; i++) {
            String tmpKey = topicList[i];
            list.add(tmpKey);
            if(!keySet.contains(tmpKey)){
                try {
                    IndexerJob pIndexJob = new IndexerJob(consumerConfig, tmpKey);
                    indexerJobs.put(tmpKey, pIndexJob);
                    indexerJobManager.startSinleThread(pIndexJob);
                } catch (Exception e) {
                    logger.error(e.getMessage() + e);
                }
            }

        }

    }
}

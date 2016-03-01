package org.elasticsearch.kafka.indexer;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by fpschina on 16/2/16.
 */
public class MessageList extends ArrayList {
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public synchronized void clear() {
        super.clear();
    }

    @Override
    public synchronized boolean add(Object o) {
        return super.add(o);
    }

    @Override
    public synchronized Object get(int index) {
        return super.get(index);
    }

    @Override
    public synchronized int size() {
        return super.size();
    }


}

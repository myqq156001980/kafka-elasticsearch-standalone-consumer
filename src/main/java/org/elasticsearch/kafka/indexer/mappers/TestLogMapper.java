package org.elasticsearch.kafka.indexer.mappers;

/**
 * Created by fpschina on 16-1-25.
 */
public class TestLogMapper {

//    private KafkaMetaDataMapper kafkaMetaDataMapper = new KafkaMetaDataMapper();
    private String name;
    private String age;

//    public KafkaMetaDataMapper getKafkaMetaDataMapper() {
//        return kafkaMetaDataMapper;
//    }
//
//    public void setKafkaMetaDataMapper(KafkaMetaDataMapper kafkaMetaDataMapper) {
//        this.kafkaMetaDataMapper = kafkaMetaDataMapper;
//    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }
}

package org.hps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Collections;
import java.util.Properties;

public class MetaDataConsumer  {
    private KafkaConsumer metadataConsumer;
    private static final Logger log = LogManager.getLogger(Scaler.class);

    public void createDirectConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        metadataConsumer = new KafkaConsumer<String, String>(props);
        metadataConsumer.subscribe(Collections.singletonList("testtopic2"));
    }


    public void consumerEnforceRebalance() {
        log.info("Trying to enforce Rebalance");
        metadataConsumer.enforceRebalance();
        log.info("Finalizing  enforce Rebalance");
    }
}

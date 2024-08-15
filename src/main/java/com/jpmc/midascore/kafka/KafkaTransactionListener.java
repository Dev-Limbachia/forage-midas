package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    @Value("${general.kafka-topic}")
    private String topicName;

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        // Later tasks will implement further processing of the transaction
        // For now, we're just receiving and logging it
    }
}
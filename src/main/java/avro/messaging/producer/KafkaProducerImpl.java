package avro.messaging.producer;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.function.BiConsumer;

@Component
public class KafkaProducerImpl<K extends Serializable, V extends SpecificRecordBase> implements KafkaProducer<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerImpl.class);

    private final KafkaTemplate<K, V> kafkaTemplate;

    public KafkaProducerImpl(KafkaTemplate<K, V> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, K key, V message, BiConsumer<SendResult<K, V>, Throwable> callback) {
        kafkaTemplate.send(topicName, key, message).addCallback(
                success -> {
                    if (LOG.isDebugEnabled()) {
                        ProducerRecord<K, V> record = success.getProducerRecord();
                        LOG.debug("Sent to topic {} with key {} and value {}", record.topic(), record.key(), record.value());
                    }
                },
                failure -> {
                    LOG.error("Something went wrong while sending message", failure);
                }
        );
    }
}
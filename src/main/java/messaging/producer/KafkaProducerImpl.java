package messaging.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@Slf4j
@Component
public class KafkaProducerImpl<K extends Serializable, V extends SpecificRecordBase> implements KafkaProducer<K, V> {

    private final KafkaTemplate<K, V> kafkaTemplate;

    public KafkaProducerImpl(KafkaTemplate<K, V> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, K key, V message, BiConsumer<SendResult<K, V>, Throwable> callback) {
        log.info("Sending message='{}' to topic='{}'", message, topicName);
        CompletableFuture<SendResult<K, V>> future = kafkaTemplate.send(topicName, key, message).whenComplete(
                (result, ex) -> {
                    if (ex == null) {
                        log.info("Message sent successfully='{}'", result);
                    } else {
                        log.error("Error sending message='{}'", message, ex);
                        throw new KafkaProducerException("Error sending message", ex);
                    }

                    if (callback != null) {
                        callback.accept(result, ex);
                    }
                }
        );
    }
}
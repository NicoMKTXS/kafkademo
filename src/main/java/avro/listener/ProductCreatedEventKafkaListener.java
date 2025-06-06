package avro.listener;

import avro.messaging.consumer.KafkaConsumer;
import avro.messaging.dto.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class ProductCreatedEventKafkaListener implements KafkaConsumer<Product> {

    /**
     * This method is used to receive the messages from the Kafka topic.
     *
     * @param messages   List of messages received from the Kafka topic.
     * @param keys       List of keys received from the Kafka topic.
     * @param partitions List of partitions received from the Kafka topic.
     * @param offsets    List of offsets received from the Kafka topic.
     */
    @Override
    @KafkaListener(id = "${kafka-consumer-config.product-created-consumer-group-id}", topics = "${catalog-service.product-created-event-topic-name}")
    public void receive(@Payload List<Product> messages,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<String> keys,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                        @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
        log.info("[Listener]: Received messages='{}' with keys='{}' from partitions='{}' with offsets='{}'", messages.size(), keys.toString(), partitions.toString(), offsets.toString());
    }
}

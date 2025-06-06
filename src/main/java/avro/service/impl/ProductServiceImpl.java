package avro.service.impl;

import avro.messaging.dto.Product;
import avro.messaging.producer.KafkaProducer;
import avro.service.ProductService;
import avro.service.helper.ProductServiceHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.function.BiConsumer;

@Slf4j
@Service
public class ProductServiceImpl implements ProductService {


    private final ProductServiceHelper productServiceHelper;

    private final KafkaProducer<String, Product> kafkaProducer;

    @Value("${catalog-service.product-created-event-topic-name}")
    private String productCreatedEventTopic;

    /**
     * Callback to handle the result of sending the product to the Kafka topic
     */
    BiConsumer<SendResult<String, Product>, Throwable> productCreatedEventCallback = (result, ex) -> {
        if (ex != null) {
            // Audit the error
            log.error("Error sending message='{}'", result.getProducerRecord().value(), ex);
        } else {
            // Audit the success
            log.info("Success handling: Sent to topic {}", result.getRecordMetadata().topic());
        }
    };

    public ProductServiceImpl(ProductServiceHelper productServiceHelper, KafkaProducer<String, Product> kafkaProducer) {
        this.productServiceHelper = productServiceHelper;
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * Create a product
     *
     * @param product the product to create
     * @return the created product
     */
    @Override
    public Optional<Product> createProduct(Product product) {
        log.info("Creating product='{}'", product);
        // Persist the product
        Optional<Product> persistedProduct = productServiceHelper.createProduct(product);

        // Send the product to the Kafka topic
        kafkaProducer.send(productCreatedEventTopic, product.getProductName(), product, productCreatedEventCallback);
        return persistedProduct;
    }
}
package avro.service.helper;

import avro.messaging.dto.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class ProductServiceHelper {

    public Optional<Product> createProduct(Product product) {
        log.info("Creating product='{}'", product);
        return Optional.of(product);
    }
}
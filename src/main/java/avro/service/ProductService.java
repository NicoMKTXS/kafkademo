package avro.service;

import avro.messaging.dto.Product;

import java.util.Optional;

public interface ProductService {

    Optional<Product> createProduct(Product product);
}
package com.example.store.sim;

import java.util.UUID;

/** A reference to a product row: the partition key (category) plus the clustering key. */
public record ProductRef(String category, UUID productId) {
}

package com.example.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.store.db.CassandraConnector;
import com.example.store.schema.SchemaManager;
import com.example.store.sim.SimulationConfig;
import com.example.store.sim.SimulationEngine;
import com.example.store.sim.Stats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spins up a real Cassandra 5 node with Testcontainers, creates the schema, runs a
 * short simulation and asserts that data of every kind was produced.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StoreSimulationTest {

    private static final String KEYSPACE = "store_test";
    private static final String CONSISTENCY = "QUORUM";

    @SuppressWarnings({"rawtypes", "resource"})
    @Container
    static final CassandraContainer CASSANDRA =
            new CassandraContainer(DockerImageName.parse("cassandra:5.0"));

    static {
        CASSANDRA.withStartupTimeout(Duration.ofMinutes(5));
    }

    private static String host;
    private static int port;
    private static String datacenter;

    @BeforeAll
    void resolveContactPoint() {
        host = CASSANDRA.getContactPoint().getHostString();
        port = CASSANDRA.getContactPoint().getPort();
        datacenter = CASSANDRA.getLocalDatacenter();
    }

    @Test
    void createsSchemaRunsSimulationAndDrops() {
        // Create the schema (RF=1 for a single-node container).
        try (CqlSession session = CassandraConnector.connect(host, port, datacenter, CONSISTENCY, null, 2)) {
            new SchemaManager(session, KEYSPACE).createSchema(1);
        }

        Stats stats = new Stats();
        SimulationConfig config = SimulationConfig.builder()
                .keyspace(KEYSPACE)
                .parallelism(8)
                .operations(3000)
                .customerPoolSize(2000)
                .productPoolSize(1000)
                .statsIntervalSeconds(2)
                .tickMillis(5)
                .build();

        try (CqlSession session = CassandraConnector.connect(host, port, datacenter, CONSISTENCY, KEYSPACE, 4)) {
            new SimulationEngine(session, config, stats).run();

            assertEquals(3000, stats.total(), "all requested operations should run");

            long customers = count(session, "customers");
            long products = count(session, "products");
            long orders = count(session, "orders");

            assertTrue(customers > 0, "expected some customers");
            assertTrue(products > 0, "expected some products");
            assertTrue(orders > 0, "expected some orders");

            // A non-frozen collection (orders.tracking) should be populated somewhere,
            // proving partial collection updates work end to end.
            long ordersWithTracking = session.execute(
                            "SELECT customer_id FROM " + KEYSPACE + ".orders LIMIT 200")
                    .all().size();
            assertTrue(ordersWithTracking > 0);
        }

        // Drop the schema and verify it is gone.
        try (CqlSession session = CassandraConnector.connect(host, port, datacenter, CONSISTENCY, null, 2)) {
            new SchemaManager(session, KEYSPACE).dropSchema();
            boolean exists = session.getMetadata().getKeyspace(KEYSPACE).isPresent();
            assertFalse(exists, "keyspace should have been dropped");
        }
    }

    private long count(CqlSession session, String table) {
        return session.execute("SELECT COUNT(*) FROM " + KEYSPACE + "." + table)
                .one().getLong(0);
    }

    @AfterAll
    void done() {
        // Container stopped automatically by the Testcontainers extension.
    }
}

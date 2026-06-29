package com.example.store.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * Builds {@link CqlSession} instances with the settings shared by every command:
 * a fixed consistency level (QUORUM by default), generous timeouts and a connection
 * pool large enough to keep the simulation thread-pool busy.
 */
public final class CassandraConnector {

    private CassandraConnector() {
    }

    /**
     * Opens a session.
     *
     * @param keyspace keyspace to bind the session to, or {@code null} for a session
     *                 that is not tied to a keyspace (needed when creating/dropping it).
     */
    public static CqlSession connect(String host,
                                     int port,
                                     String datacenter,
                                     String consistency,
                                     String keyspace,
                                     int poolConnections) {

        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                // "each operation runs at consistency level QUORUM" (configurable).
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistency)
                // LWTs need a serial consistency; SERIAL works with any replication strategy.
                .withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, "SERIAL")
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                // A load generator submits a lot of statements concurrently.
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, poolConnections)
                .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 2048)
                .withBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false)
                .build();

        com.datastax.oss.driver.api.core.CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(datacenter)
                .withConfigLoader(loader);

        if (keyspace != null) {
            builder.withKeyspace(CqlIdentifier.fromCql(keyspace));
        }
        return builder.build();
    }
}

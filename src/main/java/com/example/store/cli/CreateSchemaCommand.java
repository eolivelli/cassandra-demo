package com.example.store.cli;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.store.db.CassandraConnector;
import com.example.store.schema.SchemaManager;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "create-schema",
        description = "Create the keyspace, user defined types and tables for the store.")
public class CreateSchemaCommand implements Callable<Integer> {

    @Mixin
    ConnectionOptions conn = new ConnectionOptions();

    @Option(names = {"-r", "--replication-factor"}, defaultValue = "1",
            description = "Replication factor for the keyspace (default: ${DEFAULT-VALUE}).")
    int replicationFactor;

    @Override
    public Integer call() {
        // Connect without binding to the keyspace, since we are about to create it.
        try (CqlSession session = CassandraConnector.connect(
                conn.host, conn.port, conn.datacenter, conn.consistency, null, conn.poolConnections)) {
            new SchemaManager(session, conn.keyspace).createSchema(replicationFactor);
        }
        return 0;
    }
}

package com.example.store.cli;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.store.db.CassandraConnector;
import com.example.store.schema.SchemaManager;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import java.util.concurrent.Callable;

@Command(name = "drop-schema",
        description = "Drop the entire keyspace and everything in it.")
public class DropSchemaCommand implements Callable<Integer> {

    @Mixin
    ConnectionOptions conn = new ConnectionOptions();

    @Override
    public Integer call() {
        try (CqlSession session = CassandraConnector.connect(
                conn.host, conn.port, conn.datacenter, conn.consistency, null, conn.poolConnections)) {
            new SchemaManager(session, conn.keyspace).dropSchema();
        }
        return 0;
    }
}

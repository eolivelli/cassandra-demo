package com.example.store.cli;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.store.db.CassandraConnector;
import com.example.store.schema.SchemaManager;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "create-indexes",
        description = "Create one index per table from a template. The template must contain "
                + "exactly three '%%s' placeholders, filled with the index name (index_<table>), "
                + "the keyspace and the table name, in that order.")
public class CreateIndexesCommand implements Callable<Integer> {

    @Mixin
    ConnectionOptions conn = new ConnectionOptions();

    @Option(names = {"-t", "--index-template"}, required = true,
            description = "CREATE INDEX template with three '%%s' placeholders (index, keyspace, table). "
                    + "Escape any literal percent as '%%%%'. Example: "
                    + "\"CREATE CUSTOM INDEX %%s ON %%s.%%s (email) USING 'StorageAttachedIndex'\".")
    String indexTemplate;

    @Override
    public Integer call() {
        // The template carries the fully-qualified keyspace.table, so the session does
        // not need to be bound to a keyspace.
        try (CqlSession session = CassandraConnector.connect(
                conn.host, conn.port, conn.datacenter, conn.consistency, null, conn.poolConnections)) {
            new SchemaManager(session, conn.keyspace).createIndexes(indexTemplate);
        }
        return 0;
    }
}

package com.example.store.cli;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.store.db.CassandraConnector;
import com.example.store.schema.SchemaManager;
import com.example.store.sim.SimulationConfig;
import com.example.store.sim.SimulationEngine;
import com.example.store.sim.Stats;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "simulate",
        description = "Run the online-store workload simulation against Cassandra.")
public class SimulateCommand implements Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Mixin
    ConnectionOptions conn = new ConnectionOptions();

    @Option(names = {"-p", "--parallelism"}, defaultValue = "64",
            description = "Number of worker threads executing events (default: ${DEFAULT-VALUE}).")
    int parallelism;

    @Option(names = {"-n", "--operations"}, defaultValue = "100000000",
            description = "Total operations to run; 0 means unlimited (default: ${DEFAULT-VALUE}).")
    long operations;

    @Option(names = {"--duration-seconds"}, defaultValue = "0",
            description = "Stop after this many seconds; 0 means no time limit (default: ${DEFAULT-VALUE}).")
    long durationSeconds;

    @Option(names = {"--rate"}, defaultValue = "0",
            description = "Target operations per second; 0 means as fast as possible (default: ${DEFAULT-VALUE}).")
    long rate;

    @Option(names = {"--tick-millis"}, defaultValue = "10",
            description = "Producer tick interval in milliseconds (default: ${DEFAULT-VALUE}).")
    long tickMillis;

    @Option(names = {"--stats-interval-seconds"}, defaultValue = "5",
            description = "How often to print progress (default: ${DEFAULT-VALUE}).")
    long statsIntervalSeconds;

    @Option(names = {"--customer-pool"}, defaultValue = "100000",
            description = "Recent customer ids kept for reuse (default: ${DEFAULT-VALUE}).")
    int customerPool;

    @Option(names = {"--product-pool"}, defaultValue = "50000",
            description = "Recent product refs kept for reuse (default: ${DEFAULT-VALUE}).")
    int productPool;

    @Option(names = {"--categories"}, split = ",",
            description = "Product categories (comma-separated). Defaults to a built-in set.")
    List<String> categories;

    @Option(names = {"--seed"}, defaultValue = "42",
            description = "RNG seed (default: ${DEFAULT-VALUE}).")
    long seed;

    @Option(names = {"--create-schema"}, defaultValue = "false",
            description = "Create the schema before running the simulation.")
    boolean createSchema;

    @Option(names = {"--replication-factor"}, defaultValue = "1",
            description = "RF used only when --create-schema is set (default: ${DEFAULT-VALUE}).")
    int replicationFactor;

    @Option(names = {"--create-indexes"}, defaultValue = "false",
            description = "Create one index per table (from --index-template) before running.")
    boolean createIndexes;

    @Option(names = {"-t", "--index-template"},
            description = "CREATE INDEX template with three '%%s' placeholders (index, keyspace, table). "
                    + "Required when --create-indexes is set.")
    String indexTemplate;

    @Override
    public Integer call() {
        if (createSchema || createIndexes) {
            if (createIndexes && (indexTemplate == null || indexTemplate.isBlank())) {
                throw new picocli.CommandLine.ParameterException(spec.commandLine(),
                        "--create-indexes requires --index-template");
            }
            try (CqlSession s = CassandraConnector.connect(
                    conn.host, conn.port, conn.datacenter, conn.consistency, null, conn.poolConnections)) {
                SchemaManager schema = new SchemaManager(s, conn.keyspace);
                if (createSchema) {
                    schema.createSchema(replicationFactor);
                }
                if (createIndexes) {
                    schema.createIndexes(indexTemplate);
                }
            }
        }

        SimulationConfig.Builder cfg = SimulationConfig.builder()
                .keyspace(conn.keyspace)
                .parallelism(parallelism)
                .operations(operations)
                .durationSeconds(durationSeconds)
                .rate(rate)
                .tickMillis(tickMillis)
                .statsIntervalSeconds(statsIntervalSeconds)
                .customerPoolSize(customerPool)
                .productPoolSize(productPool)
                .seed(seed);
        if (categories != null && !categories.isEmpty()) {
            cfg.categories(categories);
        }
        SimulationConfig config = cfg.build();

        Stats stats = new Stats();
        try (CqlSession session = CassandraConnector.connect(
                conn.host, conn.port, conn.datacenter, conn.consistency, conn.keyspace, conn.poolConnections)) {

            SimulationEngine engine = new SimulationEngine(session, config, stats);
            Runtime.getRuntime().addShutdownHook(new Thread(engine::stop, "shutdown-hook"));
            engine.run();
        }
        return 0;
    }
}

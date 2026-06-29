package com.example.store;

import com.example.store.cli.CreateSchemaCommand;
import com.example.store.cli.DropSchemaCommand;
import com.example.store.cli.SimulateCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Entry point. A small online-computer-store workload generator for Apache Cassandra 5.
 *
 * <pre>
 *   store create-schema --replication-factor 3
 *   store simulate --parallelism 64 --operations 100000000
 *   store drop-schema
 * </pre>
 */
@Command(name = "store",
        mixinStandardHelpOptions = true,
        version = "cassandra-store-sim 1.0.0",
        description = "Simulate a real-world online computer store workload on Apache Cassandra 5.",
        subcommands = {
                CreateSchemaCommand.class,
                DropSchemaCommand.class,
                SimulateCommand.class
        })
public class Main {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}

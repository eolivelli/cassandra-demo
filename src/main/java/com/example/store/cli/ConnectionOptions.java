package com.example.store.cli;

import picocli.CommandLine.Option;

/** Connection options shared by every sub-command (mixed in via {@code @Mixin}). */
public class ConnectionOptions {

    @Option(names = {"-H", "--host"}, defaultValue = "127.0.0.1",
            description = "Cassandra contact point host (default: ${DEFAULT-VALUE}).")
    public String host;

    @Option(names = {"-P", "--port"}, defaultValue = "9042",
            description = "Cassandra native protocol port (default: ${DEFAULT-VALUE}).")
    public int port;

    @Option(names = {"-d", "--datacenter"}, defaultValue = "datacenter1",
            description = "Local datacenter name (default: ${DEFAULT-VALUE}).")
    public String datacenter;

    @Option(names = {"-k", "--keyspace"}, defaultValue = "store",
            description = "Keyspace name (default: ${DEFAULT-VALUE}).")
    public String keyspace;

    @Option(names = {"-c", "--consistency"}, defaultValue = "QUORUM",
            description = "Consistency level for every operation (default: ${DEFAULT-VALUE}).")
    public String consistency;

    @Option(names = {"--pool-connections"}, defaultValue = "4",
            description = "Driver connections per node (default: ${DEFAULT-VALUE}).")
    public int poolConnections;
}

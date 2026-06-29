package com.example.store.sim;

import java.util.List;

/** Immutable bag of tunables that drive a simulation run. */
public final class SimulationConfig {

    public final String keyspace;

    /** Number of worker threads executing events. */
    public final int parallelism;

    /** Stop after this many operations; {@code <= 0} means "unlimited". */
    public final long operations;

    /** Stop after this many seconds; {@code <= 0} means "no time limit". */
    public final long durationSeconds;

    /** Target operations per second; {@code <= 0} means "as fast as possible". */
    public final long rate;

    /** How often the producer wakes up to enqueue work. */
    public final long tickMillis;

    /** How often progress statistics are printed. */
    public final long statsIntervalSeconds;

    /** Bounded number of recently created customer ids kept for reuse. */
    public final int customerPoolSize;

    /** Bounded number of recently created product references kept for reuse. */
    public final int productPoolSize;

    /** Product categories used by the store. */
    public final List<String> categories;

    /** RNG seed used to make data generation reproducible. */
    public final long seed;

    private SimulationConfig(Builder b) {
        this.keyspace = b.keyspace;
        this.parallelism = b.parallelism;
        this.operations = b.operations;
        this.durationSeconds = b.durationSeconds;
        this.rate = b.rate;
        this.tickMillis = b.tickMillis;
        this.statsIntervalSeconds = b.statsIntervalSeconds;
        this.customerPoolSize = b.customerPoolSize;
        this.productPoolSize = b.productPoolSize;
        this.categories = List.copyOf(b.categories);
        this.seed = b.seed;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String keyspace = "store";
        private int parallelism = 64;
        private long operations = 100_000_000L;
        private long durationSeconds = 0;
        private long rate = 0;
        private long tickMillis = 10;
        private long statsIntervalSeconds = 5;
        private int customerPoolSize = 100_000;
        private int productPoolSize = 50_000;
        private List<String> categories = List.of(
                "laptops", "monitors", "mice", "keyboards", "gpus",
                "cpus", "storage", "memory", "headsets", "webcams");
        private long seed = 42L;

        public Builder keyspace(String v) { this.keyspace = v; return this; }
        public Builder parallelism(int v) { this.parallelism = v; return this; }
        public Builder operations(long v) { this.operations = v; return this; }
        public Builder durationSeconds(long v) { this.durationSeconds = v; return this; }
        public Builder rate(long v) { this.rate = v; return this; }
        public Builder tickMillis(long v) { this.tickMillis = v; return this; }
        public Builder statsIntervalSeconds(long v) { this.statsIntervalSeconds = v; return this; }
        public Builder customerPoolSize(int v) { this.customerPoolSize = v; return this; }
        public Builder productPoolSize(int v) { this.productPoolSize = v; return this; }
        public Builder categories(List<String> v) { this.categories = v; return this; }
        public Builder seed(long v) { this.seed = v; return this; }

        public SimulationConfig build() {
            return new SimulationConfig(this);
        }
    }
}

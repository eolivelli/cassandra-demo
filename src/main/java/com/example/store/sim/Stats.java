package com.example.store.sim;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Thread-safe counters for the simulation, plus a periodic console report. */
public final class Stats {

    private final Map<String, LongAdder> opCounts = new ConcurrentHashMap<>();
    private final LongAdder total = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LongAdder reads = new LongAdder();
    private final LongAdder lwtApplied = new LongAdder();
    private final LongAdder lwtRejected = new LongAdder();

    private final long startNanos = System.nanoTime();
    private volatile long lastTotal = 0;
    private volatile long lastNanos = startNanos;

    public void record(String op) {
        opCounts.computeIfAbsent(op, k -> new LongAdder()).increment();
        total.increment();
    }

    public void recordError() {
        errors.increment();
    }

    public void recordRead() {
        reads.increment();
    }

    public void recordLwt(boolean applied) {
        if (applied) {
            lwtApplied.increment();
        } else {
            lwtRejected.increment();
        }
    }

    public long total() {
        return total.sum();
    }

    /** Renders a one-line snapshot and resets the interval-rate window. */
    public String snapshot() {
        long now = System.nanoTime();
        long t = total.sum();
        double overallRate = t / Math.max(1e-9, (now - startNanos) / 1e9);
        double intervalRate = (t - lastTotal) / Math.max(1e-9, (now - lastNanos) / 1e9);
        lastTotal = t;
        lastNanos = now;
        return String.format(
                "ops=%,d (%,.0f op/s now, %,.0f op/s avg) errors=%,d reads=%,d lwt[ok=%,d,reject=%,d]",
                t, intervalRate, overallRate, errors.sum(), reads.sum(),
                lwtApplied.sum(), lwtRejected.sum());
    }

    /** A multi-line breakdown printed once the run finishes. */
    public String finalReport() {
        StringBuilder sb = new StringBuilder();
        double elapsed = (System.nanoTime() - startNanos) / 1e9;
        long t = total.sum();
        sb.append(String.format("Total operations : %,d%n", t));
        sb.append(String.format("Elapsed          : %.1f s%n", elapsed));
        sb.append(String.format("Throughput       : %,.0f op/s%n", t / Math.max(1e-9, elapsed)));
        sb.append(String.format("Errors           : %,d%n", errors.sum()));
        sb.append(String.format("Reads            : %,d%n", reads.sum()));
        sb.append(String.format("LWT applied      : %,d%n", lwtApplied.sum()));
        sb.append(String.format("LWT rejected     : %,d%n", lwtRejected.sum()));
        sb.append("By operation:").append(System.lineSeparator());
        opCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> sb.append(String.format("  %-22s %,d%n", e.getKey(), e.getValue().sum())));
        return sb.toString();
    }
}

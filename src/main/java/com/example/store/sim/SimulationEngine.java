package com.example.store.sim;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives the simulation: a timer ("producer") periodically enqueues events that are
 * executed by a fixed-size worker pool. Back-pressure is provided by a bounded queue
 * with a caller-runs rejection policy, so the producer naturally slows down when the
 * workers cannot keep up.
 */
public final class SimulationEngine {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationEngine.class);

    private final SimulationConfig config;
    private final StoreOperations operations;
    private final Stats stats;

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public SimulationEngine(CqlSession session, SimulationConfig config, Stats stats) {
        this.config = config;
        this.stats = stats;
        this.operations = new StoreOperations(session, config, stats);
    }

    public void run() {
        running.set(true);

        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                config.parallelism,
                config.parallelism,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.parallelism * 8),
                namedFactory("worker"),
                new ThreadPoolExecutor.CallerRunsPolicy());

        ScheduledExecutorService producer = Executors.newSingleThreadScheduledExecutor(namedFactory("producer"));
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(namedFactory("reporter"));

        long target = config.operations;
        long deadlineNanos = config.durationSeconds > 0
                ? System.nanoTime() + TimeUnit.SECONDS.toNanos(config.durationSeconds)
                : Long.MAX_VALUE;

        // How many events to enqueue per producer tick.
        long perTick = config.rate > 0
                ? Math.max(1, Math.round(config.rate * (config.tickMillis / 1000.0)))
                : (long) config.parallelism * 2;

        logPlan(target, perTick);

        reporter.scheduleAtFixedRate(
                () -> LOG.info(stats.snapshot()),
                config.statsIntervalSeconds, config.statsIntervalSeconds, TimeUnit.SECONDS);

        producer.scheduleAtFixedRate(() -> {
            if (!running.get()) {
                return;
            }
            for (long i = 0; i < perTick; i++) {
                long seq = submitted.get();
                if ((target > 0 && seq >= target) || System.nanoTime() >= deadlineNanos) {
                    running.set(false);
                    return;
                }
                submitted.incrementAndGet();
                workers.execute(operations::executeRandom);
            }
        }, 0, config.tickMillis, TimeUnit.MILLISECONDS);

        // Wait for completion (operation target, time limit, or external stop).
        try {
            while (running.get()) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted, stopping simulation");
        } finally {
            running.set(false);
            shutdown(producer, "producer");
            drain(workers);
            shutdown(reporter, "reporter");
        }

        LOG.info("Simulation finished.\n{}", stats.finalReport());
    }

    /** Requests an early, graceful stop (e.g. from a shutdown hook). */
    public void stop() {
        running.set(false);
    }

    private void logPlan(long target, long perTick) {
        LOG.info("Starting simulation: parallelism={} target-ops={} duration={}s rate={} op/s tick={}ms (per-tick={})",
                config.parallelism,
                target > 0 ? String.format("%,d", target) : "unlimited",
                config.durationSeconds > 0 ? config.durationSeconds : "unlimited",
                config.rate > 0 ? config.rate : "unlimited",
                config.tickMillis,
                perTick);
    }

    private void drain(ThreadPoolExecutor workers) {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.warn("Workers did not finish within 60s; forcing shutdown");
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    private void shutdown(ScheduledExecutorService svc, String name) {
        svc.shutdownNow();
        try {
            if (!svc.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("{} did not stop cleanly", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory namedFactory(String prefix) {
        AtomicLong n = new AtomicLong();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}

package com.example.store.sim;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A fixed-capacity, thread-safe pool of recently created entity ids.
 *
 * <p>The simulation generates hundreds of millions of operations, so we cannot keep
 * every id ever created. Instead we keep the most recent {@code capacity} ids in a
 * ring buffer and sample from them at random. This is what lets later events
 * (updates, deletes, orders, tracking) target rows that really exist without an
 * unbounded memory footprint.
 */
public final class BoundedIdPool<T> {

    private final AtomicReferenceArray<T> slots;
    private final int capacity;
    private final AtomicLong writes = new AtomicLong();

    public BoundedIdPool(int capacity) {
        this.capacity = Math.max(1, capacity);
        this.slots = new AtomicReferenceArray<>(this.capacity);
    }

    /** Records a freshly created id, possibly overwriting the oldest slot. */
    public void add(T value) {
        long w = writes.getAndIncrement();
        slots.set((int) (w % capacity), value);
    }

    /** @return a random id currently held, or {@code null} if the pool is still empty. */
    public T sample() {
        long w = writes.get();
        if (w == 0) {
            return null;
        }
        int filled = (int) Math.min(w, capacity);
        return slots.get(ThreadLocalRandom.current().nextInt(filled));
    }

    public boolean isEmpty() {
        return writes.get() == 0;
    }

    public int size() {
        return (int) Math.min(writes.get(), capacity);
    }
}

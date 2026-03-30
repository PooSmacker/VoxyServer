package com.dripps.voxyserver.server;

import it.unimi.dsi.fastutil.HashCommon;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

final class StoredSectionPresenceIndex {
    private static final int HASH_FUNCTIONS = 3;
    private static final int FILTER_BITS = 1 << 26;
    private static final int FILTER_MASK = FILTER_BITS - 1;
    private static final int FILTER_WORDS = FILTER_BITS >>> 6;
    private static final long HASH_SEED = 0x9E3779B97F4A7C15L;

    private final AtomicBoolean buildScheduled = new AtomicBoolean();
    private volatile AtomicLongArray filter;
    private volatile boolean ready;

    boolean isReady() {
        return ready;
    }

    boolean mayContain(long sectionKey) {
        AtomicLongArray current = filter;
        if (!ready || current == null) {
            return true;
        }

        long baseHash = HashCommon.mix(sectionKey);
        long stepHash = HashCommon.mix(sectionKey ^ HASH_SEED) | 1L;
        for (int hashIndex = 0; hashIndex < HASH_FUNCTIONS; hashIndex++) {
            int bitIndex = (int) ((baseHash + (stepHash * hashIndex)) & FILTER_MASK);
            long word = current.get(bitIndex >>> 6);
            long bitMask = 1L << (bitIndex & 63);
            if ((word & bitMask) == 0L) {
                return false;
            }
        }
        return true;
    }

    boolean tryScheduleBuild() {
        return buildScheduled.compareAndSet(false, true);
    }

    AtomicLongArray createBuildFilter() {
        AtomicLongArray buildingFilter = new AtomicLongArray(FILTER_WORDS);
        filter = buildingFilter;
        ready = false;
        return buildingFilter;
    }

    void addTo(AtomicLongArray target, long sectionKey) {
        long baseHash = HashCommon.mix(sectionKey);
        long stepHash = HashCommon.mix(sectionKey ^ HASH_SEED) | 1L;
        for (int hashIndex = 0; hashIndex < HASH_FUNCTIONS; hashIndex++) {
            int bitIndex = (int) ((baseHash + (stepHash * hashIndex)) & FILTER_MASK);
            int wordIndex = bitIndex >>> 6;
            long bitMask = 1L << (bitIndex & 63);

            while (true) {
                long current = target.get(wordIndex);
                long updated = current | bitMask;
                if (current == updated || target.compareAndSet(wordIndex, current, updated)) {
                    break;
                }
            }
        }
    }

    void add(long sectionKey) {
        AtomicLongArray current = filter;
        if (current == null) {
            return;
        }
        addTo(current, sectionKey);
    }

    void completeBuild(AtomicLongArray builtFilter) {
        filter = builtFilter;
        ready = true;
        buildScheduled.set(false);
    }

    void failBuild() {
        ready = false;
        buildScheduled.set(false);
    }
}
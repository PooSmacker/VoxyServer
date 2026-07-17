package com.dripps.voxyserver.server;

import it.unimi.dsi.fastutil.HashCommon;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

final class StoredSectionPresenceIndex {
    enum BuildState {
        UNBUILT,
        BUILDING,
        READY,
        FAILED
    }

    private static final int HASH_FUNCTIONS = 3;
    private static final int FILTER_BITS = 1 << 26;
    private static final int FILTER_MASK = FILTER_BITS - 1;
    private static final int FILTER_WORDS = FILTER_BITS >>> 6;
    private static final long HASH_SEED = 0x9E3779B97F4A7C15L;

    private final AtomicLongArray filter = new AtomicLongArray(FILTER_WORDS);
    private final AtomicReference<BuildState> buildState = new AtomicReference<>(BuildState.UNBUILT);

    BuildState getBuildState() {
        return buildState.get();
    }

    boolean tryBeginBuild() {
        return buildState.compareAndSet(BuildState.UNBUILT, BuildState.BUILDING);
    }

    void completeBuild() {
        buildState.set(BuildState.READY);
    }

    void failBuild() {
        buildState.set(BuildState.FAILED);
    }

    boolean mayContain(long sectionKey) {
        if (buildState.get() != BuildState.READY) {
            return true;
        }

        long baseHash = HashCommon.mix(sectionKey);
        long stepHash = HashCommon.mix(sectionKey ^ HASH_SEED) | 1L;
        for (int hashIndex = 0; hashIndex < HASH_FUNCTIONS; hashIndex++) {
            int bitIndex = (int) ((baseHash + stepHash * hashIndex) & FILTER_MASK);
            long word = filter.get(bitIndex >>> 6);
            long bitMask = 1L << (bitIndex & 63);
            if ((word & bitMask) == 0L) {
                return false;
            }
        }
        return true;
    }

    void add(long sectionKey) {
        long baseHash = HashCommon.mix(sectionKey);
        long stepHash = HashCommon.mix(sectionKey ^ HASH_SEED) | 1L;
        for (int hashIndex = 0; hashIndex < HASH_FUNCTIONS; hashIndex++) {
            int bitIndex = (int) ((baseHash + stepHash * hashIndex) & FILTER_MASK);
            int wordIndex = bitIndex >>> 6;
            long bitMask = 1L << (bitIndex & 63);

            while (true) {
                long current = filter.get(wordIndex);
                long updated = current | bitMask;
                if (current == updated || filter.compareAndSet(wordIndex, current, updated)) {
                    break;
                }
            }
        }
    }
}

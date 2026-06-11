package com.dripps.voxyserver.util;

import com.dripps.voxyserver.Voxyserver;
import com.dripps.voxyserver.config.VoxyServerConfig;
import net.minecraft.server.MinecraftServer;

import java.util.concurrent.atomic.LongAdder;

public class ServerStatsTracker {
    public static ServerStatsTracker INSTANCE;

    public record Snapshot(long chunksVoxelized, long sectionsStreamed, long engineActions) {}

    private final LongAdder chunksVoxelized = new LongAdder();
    private final LongAdder sectionsStreamed = new LongAdder();
    private final LongAdder engineActions = new LongAdder();
    private volatile int tickInterval;
    private int ticks;

    private long prevChunks;
    private long prevSections;
    private long prevEngine;

    public ServerStatsTracker(int interval) {
        this.tickInterval = interval;
    }

    public void updateTickInterval(int tickInterval) {
        this.tickInterval = tickInterval;
    }

    public void markVoxelized() {
        this.chunksVoxelized.increment();
    }

    public void markStreamed() {
        this.sectionsStreamed.increment();
    }

    public void markEngineAction() {
        this.engineActions.increment();
    }

    public Snapshot snapshot() {
        return new Snapshot(chunksVoxelized.sum(), sectionsStreamed.sum(), engineActions.sum());
    }

    public void tick(MinecraftServer server) {
        if (++this.ticks < this.tickInterval) return;
        this.ticks = 0;

        long c = chunksVoxelized.sum();
        long s = sectionsStreamed.sum();
        long e = engineActions.sum();

        VoxyServerConfig cfg = Voxyserver.getConfig();
        if (cfg != null && cfg.debugTrackingEnabled) {
            Voxyserver.LOGGER.info(
                    "stats: chunks voxelized {} | sections streamed {} | engine actions {}",
                    c - prevChunks, s - prevSections, e - prevEngine);
        }

        prevChunks = c;
        prevSections = s;
        prevEngine = e;
    }
}

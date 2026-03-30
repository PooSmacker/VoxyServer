package com.dripps.voxyserver.util;

import com.dripps.voxyserver.Voxyserver;
import net.minecraft.server.MinecraftServer;

import java.util.concurrent.atomic.LongAdder;

public class ServerStatsTracker {
    public static ServerStatsTracker INSTANCE;

    private final LongAdder chunksVoxelized = new LongAdder();
    private final LongAdder sectionsStreamed = new LongAdder();
    private final LongAdder engineActions = new LongAdder();
    private final int tickInterval;
    private int ticks;

    public ServerStatsTracker(int interval) {
        this.tickInterval = interval;
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

    public void tick(MinecraftServer server) {
        if (++this.ticks >= this.tickInterval) {
            this.ticks = 0;
            Voxyserver.LOGGER.info(
                    "stats: chunks voxelized {} | sections streamed {} | engine actions {}",
                    this.chunksVoxelized.sumThenReset(),
                    this.sectionsStreamed.sumThenReset(),
                    this.engineActions.sumThenReset()
            );
        }
    }
}

package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import me.cortex.voxy.common.world.WorldEngine;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerChunkEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.minecraft.resources.Identifier;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.chunk.LevelChunk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkVoxelizer {
    private static final long INITIAL_RETRY_DELAY_TICKS = 20L;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final int MAX_RETRIES_PER_TICK = 16;

    private final ServerLodEngine engine;
    private final LodStreamingService streamingService;
    private final boolean generateOnChunkLoad;
    private final boolean ingestOnChunkUnload;
    private final ConcurrentHashMap<PendingChunk, RetryState> pendingChunkRetries = new ConcurrentHashMap<>();
    private long currentTick;

    private record PendingChunk(Identifier dimension, int chunkX, int chunkZ) {}
    private record RetryState(long nextAttemptTick, int completedAttempts) {}

    private enum IngestOutcome {
        ACCEPTED,
        RETRYABLE
    }

    public ChunkVoxelizer(ServerLodEngine engine, LodStreamingService streamingService,
                          com.dripps.voxyserver.config.VoxyServerConfig config) {
        this.engine = engine;
        this.streamingService = streamingService;
        this.generateOnChunkLoad = config.generateOnChunkLoad;
        this.ingestOnChunkUnload = !config.dirtyTrackingEnabled;
    }

    public void register() {
        if (generateOnChunkLoad) {
            ServerChunkEvents.CHUNK_LOAD.register(this::onChunkLoad);
        }
        ServerTickEvents.END_SERVER_TICK.register(this::onServerTick);
        ServerChunkEvents.CHUNK_UNLOAD.register(this::onChunkUnload);
    }

    private void onChunkLoad(ServerLevel level, LevelChunk chunk, boolean generateOnChunkLoad) {
        streamingService.onChunkLoadStateChanged(level.dimension().identifier(), chunk.getPos().x(), chunk.getPos().z(), true);
        if (ingestChunk(level, chunk, true) == IngestOutcome.ACCEPTED) {
            pendingChunkRetries.remove(new PendingChunk(level.dimension().identifier(), chunk.getPos().x(), chunk.getPos().z()));
            return;
        }

        scheduleRetry(level, chunk);
    }

    private void onChunkUnload(ServerLevel level, LevelChunk chunk) {
        streamingService.onChunkLoadStateChanged(level.dimension().identifier(), chunk.getPos().x(), chunk.getPos().z(), false);
        pendingChunkRetries.remove(new PendingChunk(level.dimension().identifier(), chunk.getPos().x(), chunk.getPos().z()));
        if (ingestOnChunkUnload) {
            ingestChunk(level, chunk, false);
        }
    }

    private IngestOutcome ingestChunk(ServerLevel level, LevelChunk chunk, boolean markPendingResend) {
        WorldEngine world = engine.getOrCreate(level);
        if (world == null) return IngestOutcome.RETRYABLE;

        IntList pendingSectionYs = markPendingResend ? markPendingChunkSections(level, chunk) : IntList.of();
        boolean entirelyEmpty = isEntirelyEmpty(chunk);
        boolean enqueued = engine.enqueueIngest(world, chunk);
        if (!enqueued && !entirelyEmpty && !pendingSectionYs.isEmpty()) {
            clearPendingChunkSections(level.dimension().identifier(), chunk, pendingSectionYs);
        }

        if (!enqueued && !entirelyEmpty) {
            return IngestOutcome.RETRYABLE;
        }

        if (com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE != null) {
            com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE.markVoxelized();
        }
        return IngestOutcome.ACCEPTED;
    }

    public boolean revoxelizeChunk(ServerLevel level, LevelChunk chunk) {
        return ingestChunk(level, chunk, false) == IngestOutcome.ACCEPTED;
    }

    private static boolean isEntirelyEmpty(LevelChunk chunk) {
        for (var section : chunk.getSections()) {
            if (section != null && !section.hasOnlyAir()) {
                return false;
            }
        }
        return true;
    }

    private IntList markPendingChunkSections(ServerLevel level, LevelChunk chunk) {
        Identifier dimension = level.dimension().identifier();
        IntList pendingSectionYs = new IntArrayList();
        int chunkSectionY = chunk.getMinSectionY() - 1;
        int lastWorldSecY = Integer.MIN_VALUE;
        for (var ignored : chunk.getSections()) {
            chunkSectionY++;
            int worldSecY = chunkSectionY >> 1;
            if (worldSecY == lastWorldSecY) {
                continue;
            }

            lastWorldSecY = worldSecY;
            pendingSectionYs.add(worldSecY);
            streamingService.markChunkPendingInitialLoad(dimension, chunk.getPos().x(), worldSecY, chunk.getPos().z());
        }
        return pendingSectionYs;
    }

    private void clearPendingChunkSections(Identifier dimension, LevelChunk chunk, IntList pendingSectionYs) {
        for (int worldSecY : pendingSectionYs) {
            streamingService.clearChunkPendingDirty(dimension, chunk.getPos().x(), worldSecY, chunk.getPos().z());
        }
    }

    private void scheduleRetry(ServerLevel level, LevelChunk chunk) {
        pendingChunkRetries.putIfAbsent(
                new PendingChunk(level.dimension().identifier(), chunk.getPos().x(), chunk.getPos().z()),
                new RetryState(currentTick + INITIAL_RETRY_DELAY_TICKS, 0)
        );
    }

    private void onServerTick(MinecraftServer server) {
        currentTick++;
        if (!pendingChunkRetries.isEmpty()) {
            int retriesProcessed = 0;
            for (Map.Entry<PendingChunk, RetryState> entry : pendingChunkRetries.entrySet()) {
                if (retriesProcessed >= MAX_RETRIES_PER_TICK) {
                    break;
                }

                RetryState retryState = entry.getValue();
                if (retryState.nextAttemptTick() > currentTick) {
                    continue;
                }
                retriesProcessed++;

                PendingChunk pendingChunk = entry.getKey();
                ServerLevel level = findLevel(server, pendingChunk.dimension());
                if (level == null) {
                    pendingChunkRetries.remove(pendingChunk, retryState);
                    continue;
                }

                LevelChunk chunk = level.getChunkSource().getChunkNow(pendingChunk.chunkX(), pendingChunk.chunkZ());
                if (chunk == null) {
                    pendingChunkRetries.remove(pendingChunk, retryState);
                    continue;
                }

                if (ingestChunk(level, chunk, true) == IngestOutcome.ACCEPTED) {
                    pendingChunkRetries.remove(pendingChunk, retryState);
                    continue;
                }

                int completedAttempts = retryState.completedAttempts() + 1;
                if (completedAttempts >= MAX_RETRY_ATTEMPTS) {
                    pendingChunkRetries.remove(pendingChunk, retryState);
                    continue;
                }

                long retryDelay = INITIAL_RETRY_DELAY_TICKS << completedAttempts;
                pendingChunkRetries.replace(
                        pendingChunk,
                        retryState,
                        new RetryState(currentTick + retryDelay, completedAttempts)
                );
            }
        }
        engine.releaseIdleIngestReferences();
    }

    public void shutdown() {
        pendingChunkRetries.clear();
        engine.releaseIngestReferences();
    }

    private static ServerLevel findLevel(MinecraftServer server, Identifier dimension) {
        for (ServerLevel level : server.getAllLevels()) {
            if (level.dimension().identifier().equals(dimension)) {
                return level;
            }
        }
        return null;
    }
}

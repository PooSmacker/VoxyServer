package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import me.cortex.voxy.common.world.WorldEngine;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerChunkEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.chunk.LevelChunk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkVoxelizer {
    private static final long RETRY_INTERVAL_TICKS = 2L;

    private final ServerLodEngine engine;
    private final LodStreamingService streamingService;
    private final boolean generateOnChunkLoad;
    private final boolean ingestOnChunkUnload;
    private final ConcurrentHashMap<PendingChunk, Long> pendingChunkRetries = new ConcurrentHashMap<>();
    private long currentTick;

    private record PendingChunk(ResourceLocation dimension, int chunkX, int chunkZ) {}

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
            ServerTickEvents.END_SERVER_TICK.register(this::onServerTick);
        }
        ServerChunkEvents.CHUNK_UNLOAD.register(this::onChunkUnload);
    }

    private void onChunkLoad(ServerLevel level, LevelChunk chunk) {
        streamingService.onChunkLoadStateChanged(level.dimension().location(), chunk.getPos().x, chunk.getPos().z, true);
        if (ingestChunk(level, chunk, true)) {
            pendingChunkRetries.remove(new PendingChunk(level.dimension().location(), chunk.getPos().x, chunk.getPos().z));
            return;
        }

        scheduleRetry(level, chunk);
    }

    private void onChunkUnload(ServerLevel level, LevelChunk chunk) {
        streamingService.onChunkLoadStateChanged(level.dimension().location(), chunk.getPos().x, chunk.getPos().z, false);
        pendingChunkRetries.remove(new PendingChunk(level.dimension().location(), chunk.getPos().x, chunk.getPos().z));
        if (ingestOnChunkUnload) {
            ingestChunk(level, chunk, false);
        }
    }

    private boolean ingestChunk(ServerLevel level, LevelChunk chunk, boolean markPendingResend) {
        WorldEngine world = engine.getOrCreate(level);
        if (world == null) return false;

        List<Integer> pendingSectionYs = markPendingResend ? markPendingChunkSections(level, chunk) : List.of();

        engine.markChunkPossiblyPresent(level, chunk);

        if (com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE != null) {
            com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE.markVoxelized();
        }

        boolean enqueued = engine.getIngestService().enqueueIngest(world, chunk);
        if (!enqueued && !pendingSectionYs.isEmpty()) {
            clearPendingChunkSections(level.dimension().location(), chunk, pendingSectionYs);
        }
        return enqueued;
    }

    public boolean revoxelizeChunk(ServerLevel level, LevelChunk chunk) {
        return ingestChunk(level, chunk, false);
    }

    private List<Integer> markPendingChunkSections(ServerLevel level, LevelChunk chunk) {
        ResourceLocation dimension = level.dimension().location();
        List<Integer> pendingSectionYs = new ArrayList<>();
        int chunkSectionY = chunk.getMinSection() - 1;
        int lastWorldSecY = Integer.MIN_VALUE;
        for (var ignored : chunk.getSections()) {
            chunkSectionY++;
            int worldSecY = chunkSectionY >> 1;
            if (worldSecY == lastWorldSecY) {
                continue;
            }

            lastWorldSecY = worldSecY;
            pendingSectionYs.add(worldSecY);
            streamingService.markChunkPendingInitialLoad(dimension, chunk.getPos().x, worldSecY, chunk.getPos().z);
        }
        return pendingSectionYs;
    }

    private void clearPendingChunkSections(ResourceLocation dimension, LevelChunk chunk, List<Integer> pendingSectionYs) {
        for (int worldSecY : pendingSectionYs) {
            streamingService.clearChunkPendingDirty(dimension, chunk.getPos().x, worldSecY, chunk.getPos().z);
        }
    }

    private void scheduleRetry(ServerLevel level, LevelChunk chunk) {
        pendingChunkRetries.put(
                new PendingChunk(level.dimension().location(), chunk.getPos().x, chunk.getPos().z),
                currentTick + RETRY_INTERVAL_TICKS
        );
    }

    private void onServerTick(MinecraftServer server) {
        currentTick++;
        if (pendingChunkRetries.isEmpty()) {
            return;
        }

        for (Map.Entry<PendingChunk, Long> entry : pendingChunkRetries.entrySet()) {
            if (entry.getValue() > currentTick) {
                continue;
            }

            PendingChunk pendingChunk = entry.getKey();
            ServerLevel level = findLevel(server, pendingChunk.dimension());
            if (level == null) {
                pendingChunkRetries.remove(pendingChunk, entry.getValue());
                continue;
            }

            LevelChunk chunk = level.getChunkSource().getChunkNow(pendingChunk.chunkX(), pendingChunk.chunkZ());
            if (chunk == null) {
                pendingChunkRetries.remove(pendingChunk, entry.getValue());
                continue;
            }

            if (ingestChunk(level, chunk, true)) {
                pendingChunkRetries.remove(pendingChunk, entry.getValue());
                continue;
            }

            pendingChunkRetries.replace(pendingChunk, entry.getValue(), currentTick + RETRY_INTERVAL_TICKS);
        }
    }

    private static ServerLevel findLevel(MinecraftServer server, ResourceLocation dimension) {
        for (ServerLevel level : server.getAllLevels()) {
            if (level.dimension().location().equals(dimension)) {
                return level;
            }
        }
        return null;
    }
}

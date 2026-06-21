package com.dripps.voxyserver.server;

import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.chunk.LevelChunk;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// waits a few ticks before pushing dirty updates bc was causing issues
public class DirtyTracker {
    public static volatile DirtyTracker INSTANCE;

    private final ConcurrentHashMap<DirtySection, Boolean> dirtySections = new ConcurrentHashMap<>();
    private final ChunkVoxelizer voxelizer;
    private final LodStreamingService streamingService;
    private final int flushInterval;
    private int tickCounter = 0;

    private record DirtySection(ResourceLocation dimension, int chunkX, int sectionY, int chunkZ) {}
    private record ChunkPosDim(ResourceLocation dimension, int chunkX, int chunkZ) {}

    public DirtyTracker(ChunkVoxelizer voxelizer, LodStreamingService streamingService, int flushInterval) {
        this.voxelizer = voxelizer;
        this.streamingService = streamingService;
        this.flushInterval = flushInterval;
    }

    public void markDirty(ServerLevel level, int chunkX, int blockY, int chunkZ) {
        ResourceLocation dim = level.dimension().location();
        int sectionY = blockY >> 5;
        DirtySection dirtySection = new DirtySection(dim, chunkX, sectionY, chunkZ);
        dirtySections.put(dirtySection, Boolean.TRUE);
    }

    public void tick(MinecraftServer server) {
        if (++tickCounter < flushInterval) {
            return;
        }
        tickCounter = 0;

        if (dirtySections.isEmpty()) {
            return;
        }

        Set<DirtySection> toProcess = ConcurrentHashMap.newKeySet();
        var iter = dirtySections.keySet().iterator();
        while (iter.hasNext()) {
            toProcess.add(iter.next());
            iter.remove();
        }

        java.util.HashMap<ChunkPosDim, java.util.Set<Integer>> sectionsByChunk = new java.util.HashMap<>();
        for (DirtySection ds : toProcess) {
            ChunkPosDim cpd = new ChunkPosDim(ds.dimension, ds.chunkX, ds.chunkZ);
            sectionsByChunk.computeIfAbsent(cpd, ignored -> new java.util.HashSet<>()).add(ds.sectionY);
        }

        for (var entry : sectionsByChunk.entrySet()) {
            ChunkPosDim chunkPos = entry.getKey();
            ServerLevel level = findLevel(server, chunkPos.dimension);
            if (level == null) continue;

            LevelChunk chunk = level.getChunkSource().getChunkNow(chunkPos.chunkX, chunkPos.chunkZ);
            if (chunk == null) continue;

            for (int sectionY : entry.getValue()) {
                streamingService.markChunkPendingDirty(chunkPos.dimension, chunkPos.chunkX, sectionY, chunkPos.chunkZ);
            }

            if (!voxelizer.revoxelizeChunk(level, chunk)) {
                for (int sectionY : entry.getValue()) {
                    streamingService.clearChunkPendingDirty(chunkPos.dimension, chunkPos.chunkX, sectionY, chunkPos.chunkZ);
                }
            }
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

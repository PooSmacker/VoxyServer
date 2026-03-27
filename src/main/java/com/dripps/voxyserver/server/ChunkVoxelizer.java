package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerChunkEvents;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.chunk.LevelChunk;

public class ChunkVoxelizer {
    private final ServerLodEngine engine;
    private final boolean generateOnChunkLoad;

    public ChunkVoxelizer(ServerLodEngine engine, com.dripps.voxyserver.config.VoxyServerConfig config) {
        this.engine = engine;
        this.generateOnChunkLoad = config.generateOnChunkLoad;
    }

    public void register() {
        if (generateOnChunkLoad) {
            ServerChunkEvents.CHUNK_LOAD.register(this::onChunkLoad);
        }
        ServerChunkEvents.CHUNK_UNLOAD.register(this::onChunkUnload);
    }

    private void onChunkLoad(ServerLevel level, LevelChunk chunk, boolean generated) {
        ingestChunk(level, chunk);
    }

    private void onChunkUnload(ServerLevel level, LevelChunk chunk) {
        ingestChunk(level, chunk);
    }

    private void ingestChunk(ServerLevel level, LevelChunk chunk) {
        WorldIdentifier worldId = WorldIdentifier.of(level);
        if (worldId == null) return;

        WorldEngine world = engine.getOrCreate(worldId);
        if (world == null) return;

        if (com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE != null) {
            com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE.markVoxelized();
        }

        engine.getIngestService().enqueueIngest(world, chunk);
    }

    // revoxelize an already loaded chunk (dirty tracking)
    public void revoxelizeChunk(ServerLevel level, LevelChunk chunk) {
        ingestChunk(level, chunk);
    }
}

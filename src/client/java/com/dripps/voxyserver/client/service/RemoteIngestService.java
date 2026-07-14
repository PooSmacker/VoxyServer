package com.dripps.voxyserver.client.service;

import me.cortex.voxy.common.Logger;
import com.dripps.voxyserver.client.ClientLodHashStore;
import com.dripps.voxyserver.network.LODBulkPayload;
import com.dripps.voxyserver.network.LODSectionPayload;
import com.dripps.voxyserver.network.PreSerializedLodPayload;
import me.cortex.voxy.common.thread.Service;
import me.cortex.voxy.common.thread.ServiceManager;
import me.cortex.voxy.common.voxelization.VoxelizedSection;
import me.cortex.voxy.common.voxelization.WorldVoxilizedSectionMipper;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldUpdater;
import me.cortex.voxy.common.world.other.Mapper;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.minecraft.core.Holder;
import net.minecraft.core.Registry;
import net.minecraft.core.RegistryAccess;
import net.minecraft.core.registries.Registries;
import net.minecraft.resources.Identifier;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteIngestService {

    private record IngestTask(WorldIdentifier worldId, Identifier dimension,
                              PreSerializedLodPayload raw, RegistryAccess registryAccess,
                              long generation) {}

    private final Service service;
    private final ConcurrentLinkedDeque<IngestTask> ingestQueue = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<Runnable> manifestQueue = new ConcurrentLinkedDeque<>();
    private final AtomicLong dimensionGeneration = new AtomicLong();
    private volatile Identifier activeDimension;

    public RemoteIngestService(ServiceManager pool) {
        this.service = pool.createServiceNoCleanup(
                () -> this::processJob,
                5000L,
                "VoxyServer-RemoteIngestService"
        );
    }

    private void processJob() {
        Runnable manifestJob = this.manifestQueue.poll();
        if (manifestJob != null) {
            manifestJob.run();
            return;
        }

        IngestTask task = this.ingestQueue.poll();
        if (task == null) return;
        if (task.generation() != this.dimensionGeneration.get()) return;
        if (!task.dimension().equals(this.activeDimension)) return;

        LODBulkPayload bulk = task.raw().decodeBulk(task.registryAccess());
        if (!task.dimension().equals(bulk.dimension())) return;
        if (task.generation() != this.dimensionGeneration.get()) return;
        if (!task.dimension().equals(this.activeDimension)) return;

        WorldEngine engine = task.worldId().getOrCreateEngine();
        if (engine == null || !engine.isLive()) return;
        engine.markActive();

        String worldIdHash = task.worldId().getWorldId();
        Mapper mapper = engine.getMapper();

        for (LODSectionPayload section : bulk.sections()) {
            if (task.generation() != this.dimensionGeneration.get()) return;
            if (!task.dimension().equals(this.activeDimension)) return;
            long[] remappedLut = remapLut(
                    section.lutBlockStateIds(),
                    section.lutBiomeIds(),
                    section.lutLight(),
                    mapper,
                    task.registryAccess()
            );

            int secX = WorldEngine.getX(section.sectionKey());
            int secY = WorldEngine.getY(section.sectionKey());
            int secZ = WorldEngine.getZ(section.sectionKey());
            short[] indexArray = section.indexArray();

            for (int oy = 0; oy < 2; oy++) {
                for (int oz = 0; oz < 2; oz++) {
                    for (int ox = 0; ox < 2; ox++) {
                        VoxelizedSection vs = VoxelizedSection.createEmpty();
                        vs.setPosition(secX * 2 + ox, secY * 2 + oy, secZ * 2 + oz);

                        int nonAirCount = 0;
                        for (int vy = 0; vy < 16; vy++) {
                            for (int vz = 0; vz < 16; vz++) {
                                for (int vx = 0; vx < 16; vx++) {
                                    int wsIdx = ((oy * 16 + vy) << 10) | ((oz * 16 + vz) << 5) | (ox * 16 + vx);
                                    int vsIdx = (vy << 8) | (vz << 4) | vx;
                                    long id = remappedLut[indexArray[wsIdx] & 0xFFFF];
                                    vs.section[vsIdx] = id;
                                    if (!Mapper.isAir(id)) nonAirCount++;
                                }
                            }
                        }
                        vs.lvl0NonAirCount = nonAirCount;

                        WorldVoxilizedSectionMipper.mipSection(vs, mapper);
                        WorldUpdater.insertUpdate(engine, vs);
                    }
                }
            }
            ClientLodHashStore.get().put(worldIdHash, section.sectionKey(), section.contentHash());
        }
    }

    private static long[] remapLut(int[] blockStateIds, int[] biomeIds, byte[] light,
                                   Mapper mapper, RegistryAccess registryAccess) {
        Registry<Biome> biomeRegistry = registryAccess.lookupOrThrow(Registries.BIOME);
        long[] remapped = new long[blockStateIds.length];

        for (int i = 0; i < blockStateIds.length; i++) {
            BlockState state = Block.BLOCK_STATE_REGISTRY.byId(blockStateIds[i]);
            int clientBlockId = (state != null) ? mapper.getIdForBlockState(state) : 0;

            Optional<Holder.Reference<Biome>> biomeHolder = biomeRegistry.get(biomeIds[i]);
            int clientBiomeId = biomeHolder.map(mapper::getIdForBiome).orElse(0);

            remapped[i] = Mapper.composeMappingId(light[i], clientBlockId, clientBiomeId);
        }

        return remapped;
    }

    public void enqueueIngest(WorldIdentifier worldId, Identifier dimension,
                              PreSerializedLodPayload raw, RegistryAccess registryAccess) {
        if (!this.service.isLive()) return;

        this.ingestQueue.add(new IngestTask(
                worldId,
                dimension,
                raw,
                registryAccess,
                this.dimensionGeneration.get()
        ));

        try {
            this.service.execute();
        } catch (Exception e) {
            Logger.error("exception enqueuing remote ingest task", e);
        }
    }

    public void enqueueManifestBuild(Runnable job) {
        if (!this.service.isLive()) return;

        this.manifestQueue.add(job);

        try {
            this.service.execute();
        } catch (Exception e) {
            Logger.error("exception enqueuing manifest build task", e);
        }
    }

    public boolean isLive() {
        return this.service.isLive();
    }

    public void setActiveDimension(Identifier dimension) {
        if (java.util.Objects.equals(this.activeDimension, dimension)) return;
        this.activeDimension = dimension;
        this.dimensionGeneration.incrementAndGet();
    }

    public void shutdown() {
        this.activeDimension = null;
        this.ingestQueue.clear();
        this.manifestQueue.clear();
        this.service.shutdown();
    }
}

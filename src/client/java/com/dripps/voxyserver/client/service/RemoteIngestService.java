package com.dripps.voxyserver.client.service;

import me.cortex.voxy.common.Logger;
import com.dripps.voxyserver.client.ClientLodHashStore;
import com.dripps.voxyserver.network.LODSectionPayload;
import com.dripps.voxyserver.network.PreSerializedLodPayload;
import me.cortex.voxy.common.thread.Service;
import me.cortex.voxy.common.thread.ServiceManager;
import me.cortex.voxy.common.voxelization.VoxelizedSection;
import me.cortex.voxy.common.voxelization.WorldVoxilizedSectionMipper;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldUpdater;
import me.cortex.voxy.common.world.other.Mapper;
import net.minecraft.core.Holder;
import net.minecraft.core.Registry;
import net.minecraft.core.RegistryAccess;
import net.minecraft.core.registries.Registries;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

public class RemoteIngestService {

    private record IngestTask(WorldEngine engine, PreSerializedLodPayload raw, RegistryAccess registryAccess, String worldId) {}

    private final Service service;
    private final ConcurrentLinkedDeque<IngestTask> ingestQueue = new ConcurrentLinkedDeque<>();

    public RemoteIngestService(ServiceManager pool) {
        this.service = pool.createServiceNoCleanup(
                () -> this::processJob,
                5000L,
                "VoxyServer-RemoteIngestService"
        );
    }

    private void processJob() {
        IngestTask task = this.ingestQueue.poll();
        if (task == null) return;

        if (!task.engine().isLive()) return;
        task.engine().markActive();

        Mapper mapper = task.engine().getMapper();

        for (LODSectionPayload section : task.raw().decodeBulk(task.registryAccess()).sections()) {
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
                        WorldUpdater.insertUpdate(task.engine(), vs);
                    }
                }
            }
            ClientLodHashStore.get().put(task.worldId(), section.sectionKey(), section.contentHash());
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

    public void enqueueIngest(WorldEngine engine, PreSerializedLodPayload raw, RegistryAccess registryAccess, String worldId) {
        if (!this.service.isLive()) return;

        if (!engine.isLive()) {
            Logger.error("tried enqueuing remote ingest into a WorldEngine that is not alive, skipping");
            return;
        }

        this.ingestQueue.add(new IngestTask(engine, raw, registryAccess, worldId));

        try {
            this.service.execute();
        } catch (Exception e) {
            Logger.error("exception enqueuing remote ingest task", e);
        }
    }

    public boolean isLive() {
        return this.service.isLive();
    }

    public void shutdown() {
        this.service.shutdown();
    }
}
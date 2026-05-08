package com.dripps.voxyserver.client;

import com.dripps.voxyserver.network.LODBulkPayload;
import com.dripps.voxyserver.network.LODClearPayload;
import com.dripps.voxyserver.network.LODReadyPayload;
import com.dripps.voxyserver.network.LODSectionPayload;
import com.dripps.voxyserver.network.LODServerSettingsPayload;
import com.dripps.voxyserver.network.PreSerializedLodPayload;
import me.cortex.voxy.common.Logger;
import me.cortex.voxy.common.voxelization.VoxelizedSection;
import me.cortex.voxy.common.voxelization.WorldConversionFactory;
import me.cortex.voxy.common.voxelization.WorldVoxilizedSectionMipper;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldUpdater;
import me.cortex.voxy.common.world.other.Mapper;
import me.cortex.voxy.commonImpl.VoxyCommon;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientLifecycleEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayConnectionEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayNetworking;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.core.Holder;
import net.minecraft.core.Registry;
import net.minecraft.core.RegistryAccess;
import net.minecraft.core.registries.Registries;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ClientLodReceiver {

    private static final ExecutorService LOD_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "voxy-lod-receiver");
        t.setDaemon(true);
        return t;
    });

    public static void register() {
        // send ready handshake when joining a server
        ClientPlayConnectionEvents.JOIN.register((handler, sender, client) -> {
            ClientLodSettings.prepareForCurrentConnection();
            ClientPlayNetworking.send(new LODReadyPayload());
        });

        ClientPlayConnectionEvents.DISCONNECT.register((handler, client) -> {
            ClientLodSettings.reset();
            if (LOD_EXECUTOR instanceof ThreadPoolExecutor threadPool) {
                threadPool.getQueue().clear();
            }
        });

        ClientLifecycleEvents.CLIENT_STOPPING.register(client -> {
            LOD_EXECUTOR.shutdownNow();
        });

        ClientPlayNetworking.registerGlobalReceiver(LODServerSettingsPayload.TYPE, (payload, context) -> {
            context.client().execute(() -> ClientLodSettings.applyServerSettings(
                    payload.maxLodStreamRadius(), payload.maxSectionsPerTick()));
        });

        ClientPlayNetworking.registerGlobalReceiver(PreSerializedLodPayload.TYPE, (payload, context) -> {
            context.client().execute(() -> {
                ClientLevel level = context.client().level;
                if (level == null) return;

                RegistryAccess registryAccess = level.registryAccess();
                WorldIdentifier worldId = WorldIdentifier.of(level);
                if (worldId == null) return;

                LOD_EXECUTOR.execute(() -> {
                    var instance = VoxyCommon.getInstance();
                    if (instance == null) return;

                    WorldEngine engine = instance.getOrCreate(worldId);
                    if (engine == null || !engine.isLive()) return;

                    Mapper mapper = engine.getMapper();

                    try {
                        LODBulkPayload bulk = payload.decodeBulk(registryAccess);

                        List<VoxelizedSection> ready = new ArrayList<>();
                        for (LODSectionPayload sec : bulk.sections()) {
                            buildVoxelizedSections(sec, mapper, registryAccess, ready);
                        }

                        for (VoxelizedSection vs : ready) {
                            WorldVoxilizedSectionMipper.mipSection(vs, mapper);
                        }

                        context.client().execute(() -> {
                            if (!engine.isLive()) return;

                            for (VoxelizedSection vs : ready) {
                                WorldUpdater.insertUpdate(engine, vs);
                            }
                        });
                    } catch (Exception e) {
                        Logger.error(new Object[]{"Exception processing LOD payload", e});
                    }
                });
            });
        });

        ClientPlayNetworking.registerGlobalReceiver(LODClearPayload.TYPE, (payload, context) -> {
            context.client().execute(() -> handleClear(payload));
        });
    }

    private static void buildVoxelizedSections(LODSectionPayload payload, Mapper mapper, RegistryAccess registryAccess, List<VoxelizedSection> ready) {
        long[] remappedLut = remapLut(payload.lutBlockStateIds(), payload.lutBiomeIds(),
                payload.lutLight(), mapper, registryAccess);

        int secX = WorldEngine.getX(payload.sectionKey());
        int secY = WorldEngine.getY(payload.sectionKey());
        int secZ = WorldEngine.getZ(payload.sectionKey());

        short[] indexArray = payload.indexArray();

        // split 32x32x32 world section into 8 VoxelizedSections (16x16x16 each)
        for (int oy = 0; oy < 2; oy++) {
            for (int oz = 0; oz < 2; oz++) {
                for (int ox = 0; ox < 2; ox++) {
                    VoxelizedSection vs = VoxelizedSection.createEmpty();
                    vs.setPosition(secX * 2 + ox, secY * 2 + oy, secZ * 2 + oz);

                    int nonAirCount = 0;
                    for (int vy = 0; vy < 16; vy++) {
                        for (int vz = 0; vz < 16; vz++) {
                            for (int vx = 0; vx < 16; vx++) {
                                // world section index: (y<<10)|(z<<5)|x
                                int wsIdx = ((oy * 16 + vy) << 10) | ((oz * 16 + vz) << 5) | (ox * 16 + vx);
                                // voxelized section level 0 index: (y<<8)|(z<<4)|x
                                int vsIdx = (vy << 8) | (vz << 4) | vx;
                                long id = remappedLut[indexArray[wsIdx] & 0xFFFF];
                                vs.section[vsIdx] = id;
                                if (!Mapper.isAir(id)) nonAirCount++;
                            }
                        }
                    }
                    vs.lvl0NonAirCount = nonAirCount;
                    ready.add(vs);
                }
            }
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

    private static void handleClear(LODClearPayload payload) {
        // dimension change clear is handled by voxy itself when the world changes
        // this is a signal from the server to reset any cached state
    }
}
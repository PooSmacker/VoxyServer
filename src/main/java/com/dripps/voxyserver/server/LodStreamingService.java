package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import com.dripps.voxyserver.network.LODBulkPayload;
import com.dripps.voxyserver.network.LODClearPayload;
import com.dripps.voxyserver.network.LODReadyPayload;
import com.dripps.voxyserver.network.LODSectionPayload;
import com.dripps.voxyserver.util.IdRemapper;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldSection;
import me.cortex.voxy.common.world.other.Mapper;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerChunkEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayNetworking;
import net.minecraft.core.Registry;
import net.minecraft.core.registries.Registries;
import net.minecraft.resources.Identifier;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.biome.Biome;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LodStreamingService {
    private final ServerLodEngine engine;
    private final int lodStreamRadius;
    private final int maxSectionsPerTick;
    private final int tickInterval;
    private final ConcurrentHashMap<UUID, PlayerLodTracker> trackers = new ConcurrentHashMap<>();
    private final ExecutorService streamExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "VoxyServer Streaming");
        t.setDaemon(true);
        return t;
    });
    private int tickCounter = 0;

    public LodStreamingService(ServerLodEngine engine, com.dripps.voxyserver.config.VoxyServerConfig config) {
        this.engine = engine;
        this.lodStreamRadius = config.lodStreamRadius;
        this.maxSectionsPerTick = config.maxSectionsPerTickPerPlayer;
        this.tickInterval = config.tickInterval;
    }

    public void register() {
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            var tracker = new PlayerLodTracker();
            trackers.put(handler.getPlayer().getUUID(), tracker);
        });

        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            trackers.remove(handler.getPlayer().getUUID());
        });

        ServerPlayNetworking.registerGlobalReceiver(LODReadyPayload.TYPE, (payload, context) -> {
            var tracker = trackers.get(context.player().getUUID());
            if (tracker != null) {
                tracker.setReady(true);
                Voxyserver.LOGGER.info("player {} is ready for LOD streaming", context.player().getName().getString());
            }
        });

        ServerTickEvents.END_SERVER_TICK.register(this::onServerTick);

        // when a chunk is (re)voxelized, invalidate the sent flag so the updated section gets restreamed
        ServerChunkEvents.CHUNK_LOAD.register((level, chunk, generated) -> invalidateForChunk(level, chunk.getPos().x(), chunk.getPos().z()));
        ServerChunkEvents.CHUNK_UNLOAD.register((level, chunk) -> invalidateForChunk(level, chunk.getPos().x(), chunk.getPos().z()));
    }

    private void invalidateForChunk(ServerLevel level, int chunkX, int chunkZ) {
        int worldSecX = chunkX >> 1;
        int worldSecZ = chunkZ >> 1;
        int minY = level.getMinSectionY() >> 1;
        int maxY = (level.getMaxSectionY() >> 1) + 1;
        for (int secY = minY; secY < maxY; secY++) {
            long key = WorldEngine.getWorldSectionId(0, worldSecX, secY, worldSecZ);
            for (var tracker : trackers.values()) {
                tracker.invalidate(key);
            }
        }
    }

    public void shutdown() {
        streamExecutor.shutdownNow();
        try {
            streamExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    // snapshot player state on tick thread, do heavy shtuff async
    private record PlayerSnapshot(UUID uuid, int chunkX, int chunkZ,
                                   WorldIdentifier worldId, Identifier dimension,
                                   int minY, int maxY,
                                   Registry<Biome> biomeRegistry) {}

    private void onServerTick(MinecraftServer server) {
        if (++tickCounter < tickInterval) return;
        tickCounter = 0;

        List<PlayerSnapshot> snapshots = new ArrayList<>();
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            var tracker = trackers.get(player.getUUID());
            if (tracker == null || !tracker.isReady()) continue;

            tracker.updatePosition(player);
            ServerLevel level = player.level();
            WorldIdentifier worldId = WorldIdentifier.of(level);
            if (worldId == null) continue;

            snapshots.add(new PlayerSnapshot(
                    player.getUUID(),
                    tracker.getLastChunkX(),
                    tracker.getLastChunkZ(),
                    worldId,
                    level.dimension().identifier(),
                    level.getMinSectionY() >> 1,
                    (level.getMaxSectionY() >> 1) + 1,
                    level.registryAccess().lookupOrThrow(Registries.BIOME)
            ));
        }

        if (!snapshots.isEmpty()) {
            streamExecutor.execute(() -> processSnapshots(server, snapshots));
        }
    }

    private void processSnapshots(MinecraftServer server, List<PlayerSnapshot> snapshots) {
        for (PlayerSnapshot snap : snapshots) {
            var tracker = trackers.get(snap.uuid);
            if (tracker == null || !tracker.isReady()) continue;

            try {
                if (com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE != null) {
                    com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE.markStreamed();
                }
                streamForSnapshot(server, snap, tracker);
            } catch (Exception e) {
                Voxyserver.LOGGER.error("error streaming LODs for player {}", snap.uuid, e);
            }
        }
    }

    // push updated sections for a dirty chunk to all players who already have it
    public void onSectionDirty(MinecraftServer server, ServerLevel level, int chunkX, int sectionY, int chunkZ) {
        int worldSecX = chunkX >> 1;
        int worldSecZ = chunkZ >> 1;

        WorldIdentifier worldId = WorldIdentifier.of(level);
        if (worldId == null) return;

        Identifier dimension = level.dimension().identifier();
        Registry<Biome> biomeRegistry = level.registryAccess().lookupOrThrow(Registries.BIOME);

        streamExecutor.execute(() -> {
            WorldEngine world = engine.getOrCreate(worldId);
            if (world == null) return;
            Mapper mapper = world.getMapper();

            long key = WorldEngine.getWorldSectionId(0, worldSecX, sectionY, worldSecZ);
            WorldSection section = world.acquireIfExists(0, worldSecX, sectionY, worldSecZ);
            if (section == null) return;

            LODSectionPayload payload;
            try {
                payload = serializeSection(section, dimension, mapper, biomeRegistry);
            } finally {
                section.release();
            }
            if (payload == null) return;

            LODSectionPayload finalPayload = payload;
            long sectionKey = key;
            // send to all players who already received this section
            for (var entry : trackers.entrySet()) {
                PlayerLodTracker tracker = entry.getValue();
                if (!tracker.isReady() || !tracker.hasSent(sectionKey)) continue;

                UUID playerId = entry.getKey();
                if (com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE != null) {
                    com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE.markStreamed();
                }
                server.execute(() -> {
                    ServerPlayer player = server.getPlayerList().getPlayer(playerId);
                    if (player != null && player.level() == level) {
                        ServerPlayNetworking.send(player, finalPayload);
                    }
                });
            }
        });
    }

    public void onDimensionChange(ServerPlayer player, ServerLevel newLevel) {
        var tracker = trackers.get(player.getUUID());
        if (tracker == null || !tracker.isReady()) return;

        tracker.reset();
        Identifier dim = newLevel.dimension().identifier();
        ServerPlayNetworking.send(player, LODClearPayload.clearDimension(dim));
    }

    public void clearDimensionForReadyPlayers(ServerLevel level) {
        Identifier dim = level.dimension().identifier();
        for (var entry : trackers.entrySet()) {
            PlayerLodTracker tracker = entry.getValue();
            if (tracker == null || !tracker.isReady()) continue;

            ServerPlayer player = level.getServer().getPlayerList().getPlayer(entry.getKey());
            if (player == null || player.level() != level) continue;

            tracker.reset();
            ServerPlayNetworking.send(player, LODClearPayload.clearDimension(dim));
        }
    }

    private void streamForSnapshot(MinecraftServer server, PlayerSnapshot snap, PlayerLodTracker tracker) {
        WorldEngine world = engine.getOrCreate(snap.worldId);
        if (world == null) return;

        int playerWorldSecX = snap.chunkX >> 1;
        int playerWorldSecZ = snap.chunkZ >> 1;

        int radiusSections = lodStreamRadius >> 1;
        Mapper mapper = world.getMapper();

        List<LODSectionPayload> batch = new ArrayList<>();
        int sent = 0;

        for (int dist = 0; dist <= radiusSections && sent < maxSectionsPerTick; dist++) {
            for (int dx = -dist; dx <= dist && sent < maxSectionsPerTick; dx++) {
                for (int dz = -dist; dz <= dist && sent < maxSectionsPerTick; dz++) {
                    if (Math.abs(dx) != dist && Math.abs(dz) != dist) continue;

                    int secX = playerWorldSecX + dx;
                    int secZ = playerWorldSecZ + dz;

                    for (int secY = snap.minY; secY < snap.maxY && sent < maxSectionsPerTick; secY++) {
                        long key = WorldEngine.getWorldSectionId(0, secX, secY, secZ);
                        if (tracker.hasSent(key)) continue;

                        WorldSection section = world.acquireIfExists(0, secX, secY, secZ);
                        if (section == null) continue;

                        try {
                            LODSectionPayload payload = serializeSection(section, snap.dimension, mapper, snap.biomeRegistry);
                            if (payload != null) {
                                batch.add(payload);
                                sent++;
                            }
                            tracker.markSent(key);
                        } finally {
                            section.release();
                        }
                    }
                }
            }
        }

        if (!batch.isEmpty()) {
            List<LODSectionPayload> toSend = List.copyOf(batch);
            Identifier dim = snap.dimension;
            UUID playerId = snap.uuid;
            server.execute(() -> {
                ServerPlayer player = server.getPlayerList().getPlayer(playerId);
                if (player == null) return;
                if (!player.level().dimension().identifier().equals(dim)) return;
                if (toSend.size() == 1) {
                    ServerPlayNetworking.send(player, toSend.getFirst());
                } else {
                    ServerPlayNetworking.send(player, new LODBulkPayload(dim, toSend));
                }
            });
        }
    }

    private LODSectionPayload serializeSection(WorldSection section, Identifier dimension,
                                                Mapper mapper, Registry<Biome> biomeRegistry) {
        long[] data = section.copyData();

        // build LUT of unique mapping ids
        Long2ShortOpenHashMap lutMap = new Long2ShortOpenHashMap();
        lutMap.defaultReturnValue((short) -1);
        short lutIndex = 0;

        short[] indexArray = new short[data.length];
        for (int i = 0; i < data.length; i++) {
            long id = data[i];
            short idx = lutMap.putIfAbsent(id, lutIndex);
            if (idx == -1) {
                idx = lutIndex++;
            }
            indexArray[i] = idx;
        }

        // convert LUT from voxy mapper ids to vanilla registry ids
        int[] lutBlockStateIds = new int[lutIndex];
        int[] lutBiomeIds = new int[lutIndex];
        byte[] lutLight = new byte[lutIndex];

        for (var entry : lutMap.long2ShortEntrySet()) {
            long mappingId = entry.getLongKey();
            short idx = entry.getShortValue();
            lutBlockStateIds[idx] = IdRemapper.toVanillaBlockStateId(mapper, mappingId);
            lutBiomeIds[idx] = IdRemapper.toVanillaBiomeIdFromMapper(mapper, mappingId, biomeRegistry);
            lutLight[idx] = (byte) IdRemapper.getLightFromMapping(mappingId);
        }

        return new LODSectionPayload(dimension, section.key, lutBlockStateIds, lutBiomeIds, lutLight, indexArray);
    }
}

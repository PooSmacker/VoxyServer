package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import com.dripps.voxyserver.network.LODBulkPayload;
import com.dripps.voxyserver.network.LODClearPayload;
import com.dripps.voxyserver.network.LODPreferencesPayload;
import com.dripps.voxyserver.network.LODReadyPayload;
import com.dripps.voxyserver.network.LODSectionPayload;
import com.dripps.voxyserver.network.LODServerSettingsPayload;
import com.dripps.voxyserver.util.IdRemapper;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldSection;
import me.cortex.voxy.common.world.other.Mapper;
import me.cortex.voxy.commonImpl.WorldIdentifier;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LodStreamingService {
    private static final long IDLE_SCAN_RESTART_TICKS = 100L;
    private static final long INITIAL_LOAD_GRACE_TICKS = 20L;
    private static final int INITIAL_LOAD_MIN_CHUNKS_AT_DEADLINE = 3;
    private static final int MAX_DIRTY_SECTIONS_PER_DRAIN = 64;

    private final ServerLodEngine engine;
    private final int lodStreamRadius;
    private final int maxSectionsPerTick;
    private final int sectionsPerPacket;
    private final int tickInterval;
    private final long pendingDirtyTimeoutTicks;
    private final ConcurrentHashMap<UUID, PlayerLodTracker> trackers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SectionRef, Integer> sectionVersions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SectionRef, Long> pendingDirtySections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SectionRef, Long> initialLoadSections = new ConcurrentHashMap<>();
    private final Set<ChunkRef> loadedChunks = ConcurrentHashMap.newKeySet();
    private final Set<SectionRef> queuedDirtySections = ConcurrentHashMap.newKeySet();
    private final AtomicReference<SnapshotBatch> pendingSnapshotBatch = new AtomicReference<>();
    private final AtomicBoolean streamWorkerScheduled = new AtomicBoolean();
    private final ExecutorService streamExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "VoxyServer Streaming");
        t.setDaemon(true);
        return t;
    });
    private volatile MinecraftServer server;
    private int tickCounter = 0;
    private volatile long currentTick = 0L;

    private record SectionRef(Identifier dimension, long sectionKey) {}
    private record ChunkRef(Identifier dimension, int chunkX, int chunkZ) {}
    private record SnapshotBatch(MinecraftServer server, List<PlayerSnapshot> snapshots) {}

    public LodStreamingService(ServerLodEngine engine, com.dripps.voxyserver.config.VoxyServerConfig config) {
        this.engine = engine;
        this.lodStreamRadius = config.lodStreamRadius;
        this.maxSectionsPerTick = config.maxSectionsPerTickPerPlayer;
        this.sectionsPerPacket = config.sectionsPerPacket;
        this.tickInterval = config.tickInterval;
        this.pendingDirtyTimeoutTicks = Math.max(config.dirtyTrackingInterval * 2L, 40L);
        this.engine.setDirtySectionListener(this::onWorldSectionDirty);
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
                ServerPlayNetworking.send(context.player(),
                        new LODServerSettingsPayload(lodStreamRadius, maxSectionsPerTick));
            }
        });

        ServerPlayNetworking.registerGlobalReceiver(LODPreferencesPayload.TYPE, (payload, context) -> {
            var tracker = trackers.get(context.player().getUUID());
            if (tracker == null) return;
            tracker.setLodEnabled(payload.enabled());
            tracker.setPreferredRadius(payload.lodStreamRadius());
            tracker.setPreferredMaxSections(payload.maxSectionsPerTick());
            if (!payload.enabled()) {
                tracker.reset();
            }
            Voxyserver.LOGGER.info("player {} updated LOD preferences: enabled={}, radius={}, maxSections={}",
                    context.player().getName().getString(), payload.enabled(),
                    payload.lodStreamRadius(), payload.maxSectionsPerTick());
        });

        ServerTickEvents.END_SERVER_TICK.register(this::onServerTick);
    }

    public void markChunkPendingDirty(Identifier dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        long blockUntilTick = currentTick + pendingDirtyTimeoutTicks;
        pendingDirtySections.put(new SectionRef(dimension, key), blockUntilTick);
    }

    public void markChunkPendingInitialLoad(Identifier dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        SectionRef ref = new SectionRef(dimension, key);
        long blockUntilTick = currentTick + pendingDirtyTimeoutTicks;
        pendingDirtySections.put(ref, blockUntilTick);
        long graceUntilTick = currentTick + INITIAL_LOAD_GRACE_TICKS;
        initialLoadSections.compute(ref, (ignored, currentDeadline) ->
                currentDeadline == null ? graceUntilTick : Math.max(currentDeadline, graceUntilTick));
    }

    public void clearChunkPendingDirty(Identifier dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        SectionRef ref = new SectionRef(dimension, key);
        pendingDirtySections.remove(ref);
        initialLoadSections.remove(ref);
    }

    public void onChunkLoadStateChanged(Identifier dimension, int chunkX, int chunkZ, boolean loaded) {
        ChunkRef ref = new ChunkRef(dimension, chunkX, chunkZ);
        if (loaded) {
            loadedChunks.add(ref);
        } else {
            loadedChunks.remove(ref);
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

    // snapshot player state on the tick thread for async processing
    private record PlayerSnapshot(UUID uuid, int chunkX, int chunkZ,
                                   WorldIdentifier worldId, Identifier dimension,
                                   int minY, int maxY,
                                   Registry<Biome> biomeRegistry) {}

    private void onServerTick(MinecraftServer server) {
        this.server = server;
        currentTick++;
        flushReadyInitialLoadSections();
        expirePendingDirtySections();

        if (++tickCounter < tickInterval) return;
        tickCounter = 0;

        List<PlayerSnapshot> snapshots = new ArrayList<>();
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            var tracker = trackers.get(player.getUUID());
            if (tracker == null || !tracker.isReady() || !tracker.isLodEnabled()) continue;

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
            pendingSnapshotBatch.set(new SnapshotBatch(server, List.copyOf(snapshots)));
            scheduleStreamWorker();
        }
    }

    private void processSnapshots(MinecraftServer server, List<PlayerSnapshot> snapshots) {
        for (PlayerSnapshot snap : snapshots) {
            drainQueuedDirtySections(server, MAX_DIRTY_SECTIONS_PER_DRAIN / 2);

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

        drainQueuedDirtySections(server, MAX_DIRTY_SECTIONS_PER_DRAIN / 2);
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
        WorldEngine world = engine.getOrCreate(snap.worldId, snap.dimension);
        if (world == null) return;

        int playerWorldSecX = snap.chunkX >> 1;
        int playerWorldSecZ = snap.chunkZ >> 1;

        int effectiveRadius = tracker.getEffectiveRadius(lodStreamRadius);
        int effectiveMaxSections = tracker.getEffectiveMaxSections(maxSectionsPerTick);
        int radiusSections = effectiveRadius >> 1;
        Mapper mapper = world.getMapper();
        long scanTick = currentTick;

        if (!tracker.prepareScan(
                playerWorldSecX,
                playerWorldSecZ,
                radiusSections,
                snap.minY,
                snap.maxY,
                scanTick,
                IDLE_SCAN_RESTART_TICKS
        )) {
            return;
        }

        List<LODSectionPayload> batch = new ArrayList<>();
        int sent = 0;

        while (sent < effectiveMaxSections) {
            long key = tracker.nextSectionKeyToScan(scanTick, IDLE_SCAN_RESTART_TICKS);
            if (key == PlayerLodTracker.NO_SECTION_KEY) {
                break;
            }

            int version = getSectionVersion(snap.dimension, key);
            if (tracker.hasSent(key, version)) continue;
            if (isSectionPendingDirty(snap.dimension, key)) continue;

            WorldSection section = world.acquireIfExists(key);
            if (section == null) continue;

            try {
                LODSectionPayload payload = serializeSection(section, snap.dimension, mapper, snap.biomeRegistry);
                if (payload != null) {
                    batch.add(payload);
                    sent++;
                }
                tracker.markSent(key, version);
            } finally {
                section.release();
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
                for (int i = 0; i < toSend.size(); i += sectionsPerPacket) {
                    List<LODSectionPayload> chunk = toSend.subList(i, Math.min(toSend.size(), i + sectionsPerPacket));
                    if (chunk.size() == 1) {
                        ServerPlayNetworking.send(player, chunk.getFirst());
                    } else {
                        ServerPlayNetworking.send(player, new LODBulkPayload(dim, chunk));
                    }
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

    private void onWorldSectionDirty(Identifier dimension, long sectionKey) {
        if (WorldEngine.getLevel(sectionKey) != 0) {
            return;
        }

        SectionRef ref = new SectionRef(dimension, sectionKey);
        if (!pendingDirtySections.containsKey(ref)) {
            return;
        }

        MinecraftServer currentServer = this.server;
        if (currentServer == null) {
            return;
        }

        Long initialLoadDeadline = initialLoadSections.get(ref);
        if (initialLoadDeadline != null && !isInitialLoadReady(ref, initialLoadDeadline)) {
            return;
        }

        try {
            queuedDirtySections.add(ref);
            scheduleStreamWorker();
        } catch (RejectedExecutionException ignored) {
        }
    }

    private void processDirtySection(MinecraftServer server, SectionRef ref) {
        if (shouldDeferInitialLoad(ref)) {
            return;
        }

        if (pendingDirtySections.remove(ref) == null) {
            return;
        }

        initialLoadSections.remove(ref);

        Identifier dimension = ref.dimension();
        long sectionKey = ref.sectionKey();

        int version = sectionVersions.compute(ref, (ignored, currentVersion) -> {
            if (currentVersion == null || currentVersion == Integer.MAX_VALUE) {
                return 1;
            }
            return currentVersion + 1;
        });

        ServerLevel level = findLevel(server, dimension);
        if (level == null) return;

        pushDirtySection(server, level, dimension, sectionKey, version);
    }

    private void pushDirtySection(MinecraftServer server, ServerLevel level, Identifier dimension, long sectionKey, int version) {
        WorldIdentifier worldId = WorldIdentifier.of(level);
        if (worldId == null) {
            return;
        }

        WorldEngine world = engine.getOrCreate(worldId, dimension);
        if (world == null) {
            return;
        }

        Mapper mapper = world.getMapper();
        WorldSection section = world.acquireIfExists(sectionKey);
        if (section == null) return;

        Registry<Biome> biomeRegistry = level.registryAccess().lookupOrThrow(Registries.BIOME);
        LODSectionPayload payload;
        try {
            payload = serializeSection(section, dimension, mapper, biomeRegistry);
        } finally {
            section.release();
        }
        if (payload == null) {
            return;
        }

        int worldSecX = WorldEngine.getX(sectionKey);
        int worldSecZ = WorldEngine.getZ(sectionKey);
        LODSectionPayload finalPayload = payload;
        for (var entry : trackers.entrySet()) {
            PlayerLodTracker tracker = entry.getValue();
            if (!tracker.isReady() || !tracker.isLodEnabled()) {
                continue;
            }

            int playerWorldSecX = tracker.getLastChunkX() >> 1;
            int playerWorldSecZ = tracker.getLastChunkZ() >> 1;
            int effectiveRadius = tracker.getEffectiveRadius(lodStreamRadius);
            int radiusSections = effectiveRadius >> 1;
            if (Math.abs(worldSecX - playerWorldSecX) > radiusSections
                    || Math.abs(worldSecZ - playerWorldSecZ) > radiusSections) {
                continue;
            }

            tracker.markSent(sectionKey, version);

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
    }

    private boolean isSectionPendingDirty(Identifier dimension, long sectionKey) {
        Long blockUntilTick = pendingDirtySections.get(new SectionRef(dimension, sectionKey));
        return blockUntilTick != null && blockUntilTick > currentTick;
    }

    private int getSectionVersion(Identifier dimension, long sectionKey) {
        return sectionVersions.getOrDefault(new SectionRef(dimension, sectionKey), 0);
    }

    private void expirePendingDirtySections() {
        if (pendingDirtySections.isEmpty()) {
            return;
        }

        for (var entry : pendingDirtySections.entrySet()) {
            if (entry.getValue() > currentTick) {
                continue;
            }

            if (!pendingDirtySections.remove(entry.getKey(), entry.getValue())) {
                continue;
            }

            initialLoadSections.remove(entry.getKey());

            sectionVersions.compute(entry.getKey(), (ignored, currentVersion) -> {
                if (currentVersion == null || currentVersion == Integer.MAX_VALUE) {
                    return 1;
                }
                return currentVersion + 1;
            });
        }
    }

    private static ServerLevel findLevel(MinecraftServer server, Identifier dimension) {
        for (ServerLevel level : server.getAllLevels()) {
            if (level.dimension().identifier().equals(dimension)) {
                return level;
            }
        }
        return null;
    }

    private void flushReadyInitialLoadSections() {
        if (initialLoadSections.isEmpty() || pendingDirtySections.isEmpty()) {
            return;
        }

        MinecraftServer currentServer = this.server;
        if (currentServer == null) {
            return;
        }

        for (var entry : initialLoadSections.entrySet()) {
            SectionRef ref = entry.getKey();
            long deadline = entry.getValue();
            int loadedChunkCount = loadedChunkCountForSection(ref.dimension(), ref.sectionKey());
            boolean readyByFootprint = loadedChunkCount == 4;
            boolean readyByDeadline = currentTick >= deadline && loadedChunkCount >= INITIAL_LOAD_MIN_CHUNKS_AT_DEADLINE;
            if (!readyByDeadline && !readyByFootprint) {
                continue;
            }

            if (!initialLoadSections.remove(ref, deadline)) {
                continue;
            }

            // remove pending gate immediately so the snapshot scan can reach this section
            // without waiting for the dirty queue (which may be backed up behind thousands
            // of dirty callbacks and cause the pending entry to expire before processing)
            if (pendingDirtySections.remove(ref) == null) {
                continue;
            }

            // bump version so hasSent() returns false and the scanner resends
            sectionVersions.compute(ref, (ignored, currentVersion) -> {
                if (currentVersion == null || currentVersion == Integer.MAX_VALUE) {
                    return 1;
                }
                return currentVersion + 1;
            });
        }
    }

    private void scheduleStreamWorker() {
        if (!streamWorkerScheduled.compareAndSet(false, true)) {
            return;
        }

        try {
            streamExecutor.execute(this::runStreamWorker);
        } catch (RejectedExecutionException ignored) {
            streamWorkerScheduled.set(false);
        }
    }

    private void runStreamWorker() {
        try {
            while (true) {
                boolean didWork = drainQueuedDirtySections(server, MAX_DIRTY_SECTIONS_PER_DRAIN) > 0;

                SnapshotBatch snapshotBatch = pendingSnapshotBatch.getAndSet(null);
                if (snapshotBatch != null) {
                    didWork = true;
                    processSnapshots(snapshotBatch.server(), snapshotBatch.snapshots());
                }

                if (!didWork && queuedDirtySections.isEmpty() && pendingSnapshotBatch.get() == null) {
                    return;
                }
            }
        } finally {
            streamWorkerScheduled.set(false);
            if (!queuedDirtySections.isEmpty() || pendingSnapshotBatch.get() != null) {
                scheduleStreamWorker();
            }
        }
    }

    private int drainQueuedDirtySections(MinecraftServer server, int maxSections) {
        if (server == null || queuedDirtySections.isEmpty()) {
            return 0;
        }

        int drained = 0;
        while (!queuedDirtySections.isEmpty() && drained < maxSections) {
            Iterator<SectionRef> iterator = queuedDirtySections.iterator();
            if (!iterator.hasNext()) {
                return drained;
            }

            SectionRef ref = iterator.next();
            if (!queuedDirtySections.remove(ref)) {
                continue;
            }

            processDirtySection(server, ref);
            drained++;
        }
        return drained;
    }

    private boolean shouldDeferInitialLoad(SectionRef ref) {
        Long deadline = initialLoadSections.get(ref);
        if (deadline == null) {
            return false;
        }

        int loadedChunkCount = loadedChunkCountForSection(ref.dimension(), ref.sectionKey());
        if (isInitialLoadReady(deadline, loadedChunkCount)) {
            return false;
        }

        return true;
    }

    private boolean isInitialLoadReady(SectionRef ref, long deadline) {
        return isInitialLoadReady(deadline, loadedChunkCountForSection(ref.dimension(), ref.sectionKey()));
    }

    private boolean isInitialLoadReady(long deadline, int loadedChunkCount) {
        return loadedChunkCount == 4
                || (currentTick >= deadline && loadedChunkCount >= INITIAL_LOAD_MIN_CHUNKS_AT_DEADLINE);
    }

    private int loadedChunkCountForSection(Identifier dimension, long sectionKey) {
        int baseChunkX = WorldEngine.getX(sectionKey) << 1;
        int baseChunkZ = WorldEngine.getZ(sectionKey) << 1;
        int loaded = 0;
        if (loadedChunks.contains(new ChunkRef(dimension, baseChunkX, baseChunkZ))) {
            loaded++;
        }
        if (loadedChunks.contains(new ChunkRef(dimension, baseChunkX + 1, baseChunkZ))) {
            loaded++;
        }
        if (loadedChunks.contains(new ChunkRef(dimension, baseChunkX, baseChunkZ + 1))) {
            loaded++;
        }
        if (loadedChunks.contains(new ChunkRef(dimension, baseChunkX + 1, baseChunkZ + 1))) {
            loaded++;
        }
        return loaded;
    }
}

package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import com.dripps.voxyserver.network.LODBulkPayload;
import com.dripps.voxyserver.network.LODClearPayload;
import com.dripps.voxyserver.network.LODPreferencesPayload;
import com.dripps.voxyserver.network.LODReadyPayload;
import com.dripps.voxyserver.network.LODSectionPayload;
import com.dripps.voxyserver.network.LODServerSettingsPayload;
import com.dripps.voxyserver.network.PreSerializedLodPayload;
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
import net.minecraft.resources.ResourceLocation;
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
    private final DimensionOrdinals dimOrdinals = new DimensionOrdinals();
    private final ConcurrentHashMap<UUID, PlayerLodTracker> trackers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> sectionVersions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> pendingDirtySections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> initialLoadSections = new ConcurrentHashMap<>();
    private final Set<Long> loadedChunks = ConcurrentHashMap.newKeySet();
    private final Set<Long> queuedDirtySections = ConcurrentHashMap.newKeySet();
    private final AtomicReference<SnapshotBatch> pendingSnapshotBatch = new AtomicReference<>();
    private final AtomicBoolean streamWorkerScheduled = new AtomicBoolean();
    private volatile ExecutorService streamExecutor = createStreamExecutor();
    private volatile MinecraftServer server;
    private int tickCounter = 0;
    private volatile long currentTick = 0L;

    // biome id -> vanilla registry id cache per mapper, only accessed from stream thread
    private final IdentityHashMap<Mapper, int[]> biomeIdCaches = new IdentityHashMap<>();

    // we try to recover from voxy state corruption as best we can but the fix is rlly in voxy, all i can do for now.
    private final Set<ResourceLocation> corruptedDimensions = ConcurrentHashMap.newKeySet();
    private volatile long lastStreamHeartbeat = System.nanoTime();
    private volatile ResourceLocation currentStreamDimension;
    private static final long STREAM_WORKER_STUCK_NANOS = TimeUnit.SECONDS.toNanos(30);

    private record SnapshotBatch(MinecraftServer server, List<PlayerSnapshot> snapshots) {}

    private static final class DimensionOrdinals {
        private final ConcurrentHashMap<ResourceLocation, Integer> dimToOrdinal = new ConcurrentHashMap<>();
        private volatile ResourceLocation[] ordinalToDim = new ResourceLocation[0];

        int getOrdinal(ResourceLocation dim) {
            Integer ord = dimToOrdinal.get(dim);
            if (ord != null) return ord;
            return register(dim);
        }

        private synchronized int register(ResourceLocation dim) {
            Integer ord = dimToOrdinal.get(dim);
            if (ord != null) return ord;
            int o = ordinalToDim.length;
            if (o >= 16) throw new IllegalStateException("too many dimensions for key encoding (max 16)");
            ResourceLocation[] newArr = Arrays.copyOf(ordinalToDim, o + 1);
            newArr[o] = dim;
            ordinalToDim = newArr;
            dimToOrdinal.put(dim, o);
            return o;
        }

        ResourceLocation getDimension(int ordinal) {
            return ordinalToDim[ordinal];
        }
    }

    // packs dimension ordinal into the unused level bits of a level 0 section key
    private static long composeSectionKey(int dimOrdinal, long sectionKey) {
        return sectionKey | ((long)(dimOrdinal & 0xF) << 60);
    }

    private static long extractSectionKey(long compositeKey) {
        return compositeKey & 0x0FFFFFFFFFFFFFFFL;
    }

    private static int extractSectionDimOrdinal(long compositeKey) {
        return (int)(compositeKey >>> 60) & 0xF;
    }

    private static long composeChunkKey(int dimOrdinal, int chunkX, int chunkZ) {
        return ((long)(dimOrdinal & 0xFF) << 56) | ((long)(chunkX & 0x0FFFFFFF) << 28) | (chunkZ & 0x0FFFFFFFL);
    }

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

    public void markChunkPendingDirty(ResourceLocation dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        long blockUntilTick = currentTick + pendingDirtyTimeoutTicks;
        pendingDirtySections.put(composeSectionKey(dimOrdinals.getOrdinal(dimension), key), blockUntilTick);
    }

    public void markChunkPendingInitialLoad(ResourceLocation dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        long compositeKey = composeSectionKey(dimOrdinals.getOrdinal(dimension), key);
        long blockUntilTick = currentTick + pendingDirtyTimeoutTicks;
        pendingDirtySections.put(compositeKey, blockUntilTick);
        long graceUntilTick = currentTick + INITIAL_LOAD_GRACE_TICKS;
        initialLoadSections.compute(compositeKey, (ignored, currentDeadline) ->
                currentDeadline == null ? graceUntilTick : Math.max(currentDeadline, graceUntilTick));
    }

    public void clearChunkPendingDirty(ResourceLocation dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        long compositeKey = composeSectionKey(dimOrdinals.getOrdinal(dimension), key);
        pendingDirtySections.remove(compositeKey);
        initialLoadSections.remove(compositeKey);
    }

    public void onChunkLoadStateChanged(ResourceLocation dimension, int chunkX, int chunkZ, boolean loaded) {
        long chunkKey = composeChunkKey(dimOrdinals.getOrdinal(dimension), chunkX, chunkZ);
        if (loaded) {
            loadedChunks.add(chunkKey);
        } else {
            loadedChunks.remove(chunkKey);
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
                                   WorldIdentifier worldId, ResourceLocation dimension,
                                   int minY, int maxY,
                                   Registry<Biome> biomeRegistry) {}

    private void onServerTick(MinecraftServer server) {
        this.server = server;
        currentTick++;
        flushReadyInitialLoadSections();
        expirePendingDirtySections();
        checkStreamWorkerHealth();

        if (++tickCounter < tickInterval) return;
        tickCounter = 0;

        List<PlayerSnapshot> snapshots = new ArrayList<>();
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            var tracker = trackers.get(player.getUUID());
            if (tracker == null || !tracker.isReady() || !tracker.isLodEnabled()) continue;

            tracker.updatePosition(player);
            ServerLevel level = player.serverLevel();
            WorldIdentifier worldId = WorldIdentifier.of(level);
            if (worldId == null) continue;

            snapshots.add(new PlayerSnapshot(
                    player.getUUID(),
                    tracker.getLastChunkX(),
                    tracker.getLastChunkZ(),
                    worldId,
                    level.dimension().location(),
                    level.getMinSection() >> 1,
                    (level.getMaxSection() >> 1) + 1,
                    level.registryAccess().registryOrThrow(Registries.BIOME)
            ));
        }

        if (!snapshots.isEmpty()) {
            pendingSnapshotBatch.set(new SnapshotBatch(server, List.copyOf(snapshots)));
            scheduleStreamWorker();
        }
    }

    private void processSnapshots(MinecraftServer server, List<PlayerSnapshot> snapshots) {
        for (PlayerSnapshot snap : snapshots) {
            lastStreamHeartbeat = System.nanoTime();
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
        ResourceLocation dim = newLevel.dimension().location();
        ServerPlayNetworking.send(player, LODClearPayload.clearDimension(dim));
    }

    public void clearDimensionForReadyPlayers(ServerLevel level) {
        ResourceLocation dim = level.dimension().location();
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
        if (corruptedDimensions.contains(snap.dimension)) return;
        currentStreamDimension = snap.dimension;
        WorldEngine world = engine.getOrCreate(snap.worldId, snap.dimension);
        if (world == null) return;

        int playerWorldSecX = snap.chunkX >> 1;
        int playerWorldSecZ = snap.chunkZ >> 1;

        int effectiveRadius = tracker.getEffectiveRadius(lodStreamRadius);
        int effectiveMaxSections = tracker.getEffectiveMaxSections(maxSectionsPerTick);
        int radiusSections = effectiveRadius >> 1;
        Mapper mapper = world.getMapper();
        long scanTick = currentTick;
        int dimOrd = dimOrdinals.getOrdinal(snap.dimension);

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

            int version = getSectionVersion(dimOrd, key);
            if (tracker.hasSent(key, version)) continue;
            if (isSectionPendingDirty(dimOrd, key)) continue;

            WorldSection section = world.acquireIfExists(key);
            if (section == null) continue;

            boolean sectionCorrupted = false;
            try {
                LODSectionPayload payload = serializeSection(section, snap.dimension, mapper, snap.biomeRegistry);
                if (payload != null) {
                    batch.add(payload);
                    sent++;
                }
                tracker.markSent(key, version);
            } finally {
                try {
                    section.release();
                } catch (IllegalStateException e) {
                    handleVoxyCorruption(snap.dimension, "streamForSnapshot section.release()", e);
                    sectionCorrupted = true;
                }
            }
            if (sectionCorrupted) break;
        }

        if (!batch.isEmpty()) {
            List<LODSectionPayload> toSend = List.copyOf(batch);
            ResourceLocation dim = snap.dimension;
            UUID playerId = snap.uuid;

            // preserialize on stream thread so no heavy encoding on tick thread
            List<PreSerializedLodPayload> packets = new ArrayList<>();
            for (int i = 0; i < toSend.size(); i += sectionsPerPacket) {
                List<LODSectionPayload> chunk = toSend.subList(i, Math.min(toSend.size(), i + sectionsPerPacket));
                packets.add(PreSerializedLodPayload.fromBulk(new LODBulkPayload(dim, chunk), server.registryAccess()));
            }
            List<PreSerializedLodPayload> preEncoded = List.copyOf(packets);

            server.execute(() -> {
                ServerPlayer player = server.getPlayerList().getPlayer(playerId);
                if (player == null) return;
                if (!player.level().dimension().location().equals(dim)) return;
                for (PreSerializedLodPayload pkt : preEncoded) {
                    ServerPlayNetworking.send(player, pkt);
                }
            });
        }
    }

    private LODSectionPayload serializeSection(WorldSection section, ResourceLocation dimension,
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
            lutBiomeIds[idx] = getCachedBiomeId(mapper, mappingId, biomeRegistry);
            lutLight[idx] = (byte) IdRemapper.getLightFromMapping(mappingId);
        }

        return new LODSectionPayload(dimension, section.key, lutBlockStateIds, lutBiomeIds, lutLight, indexArray);
    }

    private int getCachedBiomeId(Mapper mapper, long mappingId, Registry<Biome> biomeRegistry) {
        int biomeId = Mapper.getBiomeId(mappingId);
        int[] cache = biomeIdCaches.get(mapper);
        if (cache != null && biomeId < cache.length && cache[biomeId] != -1) {
            return cache[biomeId];
        }
        int vanillaId = IdRemapper.toVanillaBiomeIdFromMapper(mapper, mappingId, biomeRegistry);
        if (cache == null || biomeId >= cache.length) {
            int newLen = Math.max(biomeId + 1, cache == null ? 16 : cache.length * 2);
            int[] newCache = new int[newLen];
            Arrays.fill(newCache, -1);
            if (cache != null) System.arraycopy(cache, 0, newCache, 0, cache.length);
            cache = newCache;
            biomeIdCaches.put(mapper, cache);
        }
        cache[biomeId] = vanillaId;
        return vanillaId;
    }

    private void onWorldSectionDirty(ResourceLocation dimension, long sectionKey) {
        if (WorldEngine.getLevel(sectionKey) != 0) {
            return;
        }

        long compositeKey = composeSectionKey(dimOrdinals.getOrdinal(dimension), sectionKey);
        if (!pendingDirtySections.containsKey(compositeKey)) {
            return;
        }

        MinecraftServer currentServer = this.server;
        if (currentServer == null) {
            return;
        }

        Long initialLoadDeadline = initialLoadSections.get(compositeKey);
        if (initialLoadDeadline != null && !isInitialLoadReady(compositeKey, initialLoadDeadline)) {
            return;
        }

        try {
            queuedDirtySections.add(compositeKey);
            scheduleStreamWorker();
        } catch (RejectedExecutionException ignored) {
        }
    }

    private void processDirtySection(MinecraftServer server, long compositeKey) {
        if (shouldDeferInitialLoad(compositeKey)) {
            return;
        }

        if (pendingDirtySections.remove(compositeKey) == null) {
            return;
        }

        initialLoadSections.remove(compositeKey);

        ResourceLocation dimension = dimOrdinals.getDimension(extractSectionDimOrdinal(compositeKey));
        long sectionKey = extractSectionKey(compositeKey);

        int version = sectionVersions.compute(compositeKey, (ignored, currentVersion) -> {
            if (currentVersion == null || currentVersion == Integer.MAX_VALUE) {
                return 1;
            }
            return currentVersion + 1;
        });

        ServerLevel level = findLevel(server, dimension);
        if (level == null) return;

        pushDirtySection(server, level, dimension, sectionKey, version);
    }

    private void pushDirtySection(MinecraftServer server, ServerLevel level, ResourceLocation dimension, long sectionKey, int version) {
        if (corruptedDimensions.contains(dimension)) return;
        currentStreamDimension = dimension;
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

        Registry<Biome> biomeRegistry = level.registryAccess().registryOrThrow(Registries.BIOME);
        boolean sectionCorrupted = false;
        LODSectionPayload payload;
        try {
            payload = serializeSection(section, dimension, mapper, biomeRegistry);
        } finally {
            try {
                section.release();
            } catch (IllegalStateException e) {
                handleVoxyCorruption(dimension, "pushDirtySection section.release()", e);
                sectionCorrupted = true;
            }
        }
        if (sectionCorrupted || payload == null) {
            return;
        }

        int worldSecX = WorldEngine.getX(sectionKey);
        int worldSecZ = WorldEngine.getZ(sectionKey);

        PreSerializedLodPayload preSerialized = PreSerializedLodPayload.fromBulk(
                new LODBulkPayload(dimension, List.of(payload)), level.registryAccess());

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
                    ServerPlayNetworking.send(player, preSerialized);
                }
            });
        }
    }

    private boolean isSectionPendingDirty(int dimOrdinal, long sectionKey) {
        Long blockUntilTick = pendingDirtySections.get(composeSectionKey(dimOrdinal, sectionKey));
        return blockUntilTick != null && blockUntilTick > currentTick;
    }

    private int getSectionVersion(int dimOrdinal, long sectionKey) {
        return sectionVersions.getOrDefault(composeSectionKey(dimOrdinal, sectionKey), 0);
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

    private static ServerLevel findLevel(MinecraftServer server, ResourceLocation dimension) {
        for (ServerLevel level : server.getAllLevels()) {
            if (level.dimension().location().equals(dimension)) {
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
            Long compositeKey = entry.getKey();
            long deadline = entry.getValue();
            int dimOrd = extractSectionDimOrdinal(compositeKey);
            long sectionKey = extractSectionKey(compositeKey);
            int loadedChunkCount = loadedChunkCountForSection(dimOrd, sectionKey);
            boolean readyByFootprint = loadedChunkCount == 4;
            boolean readyByDeadline = currentTick >= deadline && loadedChunkCount >= INITIAL_LOAD_MIN_CHUNKS_AT_DEADLINE;
            if (!readyByDeadline && !readyByFootprint) {
                continue;
            }

            if (!initialLoadSections.remove(compositeKey, deadline)) {
                continue;
            }

            // remove pending gate immediately so the snapshot scan can reach this section
            // without waiting for the dirty queue (which may be backed up behind thousands
            // of dirty callbacks and cause the pending entry to expire before processing)
            if (pendingDirtySections.remove(compositeKey) == null) {
                continue;
            }

            // bump version so hasSent() returns false and the scanner resends
            sectionVersions.compute(compositeKey, (ignored, currentVersion) -> {
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
                lastStreamHeartbeat = System.nanoTime();
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
            lastStreamHeartbeat = System.nanoTime();
            currentStreamDimension = null;
            streamWorkerScheduled.set(false);
            if (!queuedDirtySections.isEmpty() || pendingSnapshotBatch.get() != null) {
                scheduleStreamWorker();
            }
        }
    }

    private static ExecutorService createStreamExecutor() {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "VoxyServer Streaming");
            t.setDaemon(true);
            return t;
        });
    }

    private void checkStreamWorkerHealth() {
        if (!streamWorkerScheduled.get()) {
            lastStreamHeartbeat = System.nanoTime();
            return;
        }
        long elapsed = System.nanoTime() - lastStreamHeartbeat;
        if (elapsed > STREAM_WORKER_STUCK_NANOS) {
            ResourceLocation dim = currentStreamDimension;
            if (dim != null) {
                handleVoxyCorruption(dim, "stuck stream worker (no heartbeat for "
                        + TimeUnit.NANOSECONDS.toSeconds(elapsed) + "s)", null);
            }
            Voxyserver.LOGGER.error(
                    "[VoxyServer] stream worker unresponsive for {}s, prolly blocked on a leaked Voxy StampedLock. "
                            + "replacing stream executor. a daemon thread has been leaked.",
                    TimeUnit.NANOSECONDS.toSeconds(elapsed));

            ExecutorService oldExecutor = streamExecutor;
            streamExecutor = createStreamExecutor();
            lastStreamHeartbeat = System.nanoTime();
            currentStreamDimension = null;
            streamWorkerScheduled.set(false);
            oldExecutor.shutdownNow();

            if (!queuedDirtySections.isEmpty() || pendingSnapshotBatch.get() != null) {
                scheduleStreamWorker();
            }
        }
    }

    private void handleVoxyCorruption(ResourceLocation dimension, String context, Exception e) {
        if (!corruptedDimensions.add(dimension)) return;
        if (e != null) {
            Voxyserver.LOGGER.error(
                    "[VoxyServer] Voxy state corruption :/ ({}) for dimension '{}'. "
                            + "lod streaming disabled for this dimension until server restart.",
                    context, dimension, e);
        } else {
            Voxyserver.LOGGER.error(
                    "[VoxyServer] Voxy state corruption :/ ({}) for dimension '{}'. "
                            + "lod streaming disabled for this dimension until server restart.",
                    context, dimension);
        }
    }

    private int drainQueuedDirtySections(MinecraftServer server, int maxSections) {
        if (server == null || queuedDirtySections.isEmpty()) {
            return 0;
        }

        int drained = 0;
        while (!queuedDirtySections.isEmpty() && drained < maxSections) {
            Iterator<Long> iterator = queuedDirtySections.iterator();
            if (!iterator.hasNext()) {
                return drained;
            }

            Long compositeKey = iterator.next();
            if (!queuedDirtySections.remove(compositeKey)) {
                continue;
            }

            processDirtySection(server, compositeKey);
            drained++;
        }
        return drained;
    }

    private boolean shouldDeferInitialLoad(long compositeKey) {
        Long deadline = initialLoadSections.get(compositeKey);
        if (deadline == null) {
            return false;
        }

        int dimOrd = extractSectionDimOrdinal(compositeKey);
        long sectionKey = extractSectionKey(compositeKey);
        int loadedChunkCount = loadedChunkCountForSection(dimOrd, sectionKey);
        if (isInitialLoadReady(deadline, loadedChunkCount)) {
            return false;
        }

        return true;
    }

    private boolean isInitialLoadReady(long compositeKey, long deadline) {
        int dimOrd = extractSectionDimOrdinal(compositeKey);
        long sectionKey = extractSectionKey(compositeKey);
        return isInitialLoadReady(deadline, loadedChunkCountForSection(dimOrd, sectionKey));
    }

    private boolean isInitialLoadReady(long deadline, int loadedChunkCount) {
        return loadedChunkCount == 4
                || (currentTick >= deadline && loadedChunkCount >= INITIAL_LOAD_MIN_CHUNKS_AT_DEADLINE);
    }

    private int loadedChunkCountForSection(int dimOrdinal, long sectionKey) {
        int baseChunkX = WorldEngine.getX(sectionKey) << 1;
        int baseChunkZ = WorldEngine.getZ(sectionKey) << 1;
        int loaded = 0;
        if (loadedChunks.contains(composeChunkKey(dimOrdinal, baseChunkX, baseChunkZ))) loaded++;
        if (loadedChunks.contains(composeChunkKey(dimOrdinal, baseChunkX + 1, baseChunkZ))) loaded++;
        if (loadedChunks.contains(composeChunkKey(dimOrdinal, baseChunkX, baseChunkZ + 1))) loaded++;
        if (loadedChunks.contains(composeChunkKey(dimOrdinal, baseChunkX + 1, baseChunkZ + 1))) loaded++;
        return loaded;
    }
}

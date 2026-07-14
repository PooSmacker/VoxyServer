package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import com.dripps.voxyserver.network.LODBulkPayload;
import com.dripps.voxyserver.network.LODClearPayload;
import com.dripps.voxyserver.network.LODHandshakePayload;
import com.dripps.voxyserver.network.LODManifestPayload;
import com.dripps.voxyserver.network.LODPreferencesPayload;
import com.dripps.voxyserver.network.LODProtocolPayload;
import com.dripps.voxyserver.network.LODSectionPayload;
import com.dripps.voxyserver.network.LODServerSettingsPayload;
import com.dripps.voxyserver.network.PreSerializedLodPayload;
import com.dripps.voxyserver.network.VoxyServerNetworking;
import com.dripps.voxyserver.util.IdRemapper;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldSection;
import me.cortex.voxy.common.world.other.Mapper;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayNetworking;
import net.minecraft.ChatFormatting;
import net.minecraft.core.Registry;
import net.minecraft.core.registries.Registries;
import net.minecraft.network.chat.Component;
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
    private static final int MAX_MANIFEST_BATCHES_PER_DRAIN = 4;
    // we have a grace window for the client manifest to arrive before the scan falls back to a full send
    private static final long MANIFEST_TIMEOUT_TICKS = 60L;

    private final ServerLodEngine engine;
    private volatile int lodStreamRadius;
    private volatile int maxSectionsPerTick;
    private volatile int sectionsPerPacket;
    private volatile int tickInterval;
    private volatile long pendingDirtyTimeoutTicks;
    private volatile boolean hashSyncEnabled;
    private final DimensionOrdinals dimOrdinals = new DimensionOrdinals();
    private final ConcurrentHashMap<UUID, PlayerLodTracker> trackers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> sectionVersions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, long[]> hashCacheByKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> pendingDirtySections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> initialLoadSections = new ConcurrentHashMap<>();
    private final Set<Long> loadedChunks = ConcurrentHashMap.newKeySet();
    private final Set<Long> queuedDirtySections = ConcurrentHashMap.newKeySet();
    private final Queue<ManifestBatch> pendingManifestBatches = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final AtomicReference<SnapshotBatch> pendingSnapshotBatch = new AtomicReference<>();
    private final AtomicBoolean streamWorkerScheduled = new AtomicBoolean();
    private volatile ExecutorService streamExecutor = createStreamExecutor();
    private volatile MinecraftServer server;
    private int tickCounter = 0;
    private volatile long currentTick = 0L;

    // biome id -> vanilla registry id cache per mapper, only accessed from stream thread
    private final IdentityHashMap<Mapper, int[]> biomeIdCaches = new IdentityHashMap<>();

    // we try to recover from voxy state corruption as best we can but the fix is rlly in voxy, all i can do for now.
    private final Set<Identifier> corruptedDimensions = ConcurrentHashMap.newKeySet();
    private volatile long lastStreamHeartbeat = System.nanoTime();
    private volatile Identifier currentStreamDimension;
    private static final long STREAM_WORKER_STUCK_NANOS = TimeUnit.SECONDS.toNanos(30);

    private record SnapshotBatch(MinecraftServer server, List<PlayerSnapshot> snapshots) {}
    private record ManifestBatch(UUID playerId, Identifier dimension,
                                 long[] keys, long[] hashes, boolean complete) {}

    private static final class DimensionOrdinals {
        private final ConcurrentHashMap<Identifier, Integer> dimToOrdinal = new ConcurrentHashMap<>();
        private volatile Identifier[] ordinalToDim = new Identifier[0];

        int getOrdinal(Identifier dim) {
            Integer ord = dimToOrdinal.get(dim);
            if (ord != null) return ord;
            return register(dim);
        }

        private synchronized int register(Identifier dim) {
            Integer ord = dimToOrdinal.get(dim);
            if (ord != null) return ord;
            int o = ordinalToDim.length;
            if (o >= 16) throw new IllegalStateException("too many dimensions for key encoding (max 16)");
            Identifier[] newArr = Arrays.copyOf(ordinalToDim, o + 1);
            newArr[o] = dim;
            ordinalToDim = newArr;
            dimToOrdinal.put(dim, o);
            return o;
        }

        Identifier getDimension(int ordinal) {
            return ordinalToDim[ordinal];
        }
    }

    // packs dimension ordinal into the unused level bits of a level 0 section key
    static long composeSectionKey(int dimOrdinal, long sectionKey) {
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

    private static Component clientOutOfDateMessage() {
        return Component.literal("[VoxyServer] ").withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD)
                .append(Component.literal("your VoxyServer client is out of date. update the mod to use LODs from this server.")
                        .withStyle(ChatFormatting.RED));
    }

    public LodStreamingService(ServerLodEngine engine, com.dripps.voxyserver.config.VoxyServerConfig config) {
        this.engine = engine;
        this.lodStreamRadius = config.lodStreamRadius;
        this.maxSectionsPerTick = config.maxSectionsPerTickPerPlayer;
        this.sectionsPerPacket = config.sectionsPerPacket;
        this.tickInterval = config.tickInterval;
        this.pendingDirtyTimeoutTicks = Math.max(config.dirtyTrackingInterval * 2L, 40L);
        this.hashSyncEnabled = config.hashSyncEnabled;
        this.engine.setDirtySectionListener(this::onWorldSectionDirty);
    }

    public void register() {
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
            var tracker = new PlayerLodTracker();
            trackers.put(handler.getPlayer().getUUID(), tracker);
        });

        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            UUID playerId = handler.getPlayer().getUUID();
            trackers.remove(playerId);
        });

        ServerPlayNetworking.registerGlobalReceiver(LODHandshakePayload.TYPE, (payload, context) -> {
            var tracker = trackers.get(context.player().getUUID());
            if (tracker == null) return;
            ServerPlayer player = context.player();
            int clientProto = payload.protocol();
            int serverProto = VoxyServerNetworking.PROTOCOL_VERSION;
            tracker.setReady(true);
            if (clientProto == serverProto) {
                tracker.setProtocolOk(true);
                tracker.setActiveDimension(player.level().dimension().identifier());
                ServerPlayNetworking.send(player, new LODProtocolPayload(serverProto));
                ServerPlayNetworking.send(player, new LODServerSettingsPayload(lodStreamRadius, maxSectionsPerTick));
                if (hashSyncEnabled) {
                    Identifier dim = player.level().dimension().identifier();
                    tracker.beginManifestWait(dim, currentTick + MANIFEST_TIMEOUT_TICKS);
                }
                Voxyserver.LOGGER.info("player {} ready for LOD streaming, protocol {}", player.getName().getString(), serverProto);
            } else if (clientProto < serverProto) {
                tracker.setProtocolOk(false);
                Voxyserver.LOGGER.warn("player {} VoxyServer client out of date (client protocol {}, server {}), LOD streaming disabled",
                        player.getName().getString(), clientProto, serverProto);
                player.sendSystemMessage(clientOutOfDateMessage());
            } else {
                tracker.setProtocolOk(false);
                Voxyserver.LOGGER.warn("this server's VoxyServer is out of date (server protocol {}, client {}) for player {}, LOD streaming disabled",
                        serverProto, clientProto, player.getName().getString());
            }
        });

        ServerPlayNetworking.registerGlobalReceiver(LODManifestPayload.TYPE, (payload, context) -> {
            if (!hashSyncEnabled) return;
            var tracker = trackers.get(context.player().getUUID());
            if (tracker == null) return;
            Identifier currentDimension = context.player().level().dimension().identifier();
            if (!currentDimension.equals(payload.dimension())) return;
            pendingManifestBatches.add(new ManifestBatch(
                    context.player().getUUID(),
                    payload.dimension(),
                    payload.keys(),
                    payload.hashes(),
                    payload.complete()
            ));
            scheduleStreamWorker();
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
        pendingDirtySections.put(composeSectionKey(dimOrdinals.getOrdinal(dimension), key), blockUntilTick);
    }

    public void markChunkPendingInitialLoad(Identifier dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        long compositeKey = composeSectionKey(dimOrdinals.getOrdinal(dimension), key);
        long blockUntilTick = currentTick + pendingDirtyTimeoutTicks;
        pendingDirtySections.put(compositeKey, blockUntilTick);
        long graceUntilTick = currentTick + INITIAL_LOAD_GRACE_TICKS;
        initialLoadSections.compute(compositeKey, (ignored, currentDeadline) ->
                currentDeadline == null ? graceUntilTick : Math.max(currentDeadline, graceUntilTick));
    }

    public void clearChunkPendingDirty(Identifier dimension, int chunkX, int sectionY, int chunkZ) {
        long key = WorldEngine.getWorldSectionId(0, chunkX >> 1, sectionY, chunkZ >> 1);
        long compositeKey = composeSectionKey(dimOrdinals.getOrdinal(dimension), key);
        pendingDirtySections.remove(compositeKey);
        initialLoadSections.remove(compositeKey);
    }

    public void onChunkLoadStateChanged(Identifier dimension, int chunkX, int chunkZ, boolean loaded) {
        long chunkKey = composeChunkKey(dimOrdinals.getOrdinal(dimension), chunkX, chunkZ);
        if (loaded) {
            loadedChunks.add(chunkKey);
        } else {
            loadedChunks.remove(chunkKey);
        }
    }

    public void updateConfig(int lodStreamRadius, int maxSectionsPerTick,
                             int sectionsPerPacket, int tickInterval,
                             int dirtyTrackingInterval, boolean hashSyncEnabled) {
        this.lodStreamRadius = lodStreamRadius;
        this.maxSectionsPerTick = maxSectionsPerTick;
        this.sectionsPerPacket = sectionsPerPacket;
        this.tickInterval = tickInterval;
        this.pendingDirtyTimeoutTicks = Math.max(dirtyTrackingInterval * 2L, 40L);
        this.hashSyncEnabled = hashSyncEnabled;
        if (!hashSyncEnabled) {
            pendingManifestBatches.clear();
            for (PlayerLodTracker tracker : trackers.values()) {
                tracker.clearManifestWait();
            }
        }
    }

    public void shutdown() {
        pendingManifestBatches.clear();
        streamExecutor.shutdownNow();
        try {
            streamExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public record PlayerStat(String name, int sentCount, int chunkX, int chunkZ) {}

    public record StreamingStats(int players, int trackedSections, int hashCacheSize,
                                 int pendingDirty, int queuedDirty, int loadedChunks,
                                 List<PlayerStat> perPlayer) {}

    public StreamingStats snapshotStats(MinecraftServer mcServer) {
        List<PlayerStat> perPlayer = new ArrayList<>();
        for (var entry : trackers.entrySet()) {
            UUID uuid = entry.getKey();
            PlayerLodTracker tracker = entry.getValue();
            String name = uuid.toString();
            if (mcServer != null) {
                ServerPlayer player = mcServer.getPlayerList().getPlayer(uuid);
                if (player != null) name = player.getName().getString();
            }
            perPlayer.add(new PlayerStat(name, tracker.sentCount(),
                    tracker.getLastChunkX(), tracker.getLastChunkZ()));
        }
        return new StreamingStats(
                trackers.size(),
                sectionVersions.size(),
                hashCacheByKey.size(),
                pendingDirtySections.size(),
                queuedDirtySections.size(),
                loadedChunks.size(),
                perPlayer);
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
        checkStreamWorkerHealth();

        if (++tickCounter < tickInterval) return;
        tickCounter = 0;

        List<PlayerSnapshot> snapshots = new ArrayList<>();
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            var tracker = trackers.get(player.getUUID());
            if (tracker == null || !tracker.isReady() || !tracker.isProtocolOk() || !tracker.isLodEnabled()) continue;

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

        tracker.resetScanState();
        Identifier dim = newLevel.dimension().identifier();
        tracker.setActiveDimension(dim);
        if (hashSyncEnabled && tracker.isProtocolOk()) {
            tracker.beginManifestWait(dim, currentTick + MANIFEST_TIMEOUT_TICKS);
        }
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
        if (corruptedDimensions.contains(snap.dimension)) return;
        if (!tracker.isActiveDimension(snap.dimension)) return;
        if (hashSyncEnabled && tracker.isManifestGated(snap.dimension, currentTick)) return;
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
            lastStreamHeartbeat = System.nanoTime();

            long key = tracker.nextSectionKeyToScan(scanTick, IDLE_SCAN_RESTART_TICKS);
            if (key == PlayerLodTracker.NO_SECTION_KEY) {
                break;
            }

            long composite = composeSectionKey(dimOrd, key);
            int version = getSectionVersion(dimOrd, key);
            if (isSectionPendingDirty(dimOrd, key)) continue;

            // we cn js skip without acquiring when the cached hash is current and the client already has it
            long[] cached = hashCacheByKey.get(composite);
            if (cached != null && cached[0] == version && tracker.hasSent(composite, cached[1])) continue;

            WorldSection section = world.acquireIfExists(key);
            if (section == null) continue;

            try {
                LODSectionPayload payload = serializeSection(section, snap.dimension, mapper, snap.biomeRegistry);
                if (payload != null) {
                    long hash = payload.contentHash();
                    hashCacheByKey.put(composite, new long[]{version, hash});
                    if (!tracker.hasSent(composite, hash)) {
                        batch.add(payload);
                        sent++;
                        tracker.markSent(composite, hash);
                    }
                }
            } finally {
                section.release();
            }
        }

        if (!batch.isEmpty()) {
            List<LODSectionPayload> toSend = List.copyOf(batch);
            Identifier dim = snap.dimension;
            UUID playerId = snap.uuid;

            // preserialize on stream thread so no heavy encoding on tick thread
            List<PreSerializedLodPayload> packets = new ArrayList<>();
            for (int i = 0; i < toSend.size(); i += sectionsPerPacket) {
                List<LODSectionPayload> chunk = toSend.subList(i, Math.min(toSend.size(), i + sectionsPerPacket));
                packets.add(PreSerializedLodPayload.fromBulk(new LODBulkPayload(dim, chunk), server.registryAccess()));
            }
            List<PreSerializedLodPayload> preEncoded = List.copyOf(packets);

            if (Voxyserver.LOGGER.isDebugEnabled()) {
                int totalBytes = 0;
                for (PreSerializedLodPayload pkt : preEncoded) totalBytes += pkt.data().length;
                Voxyserver.LOGGER.debug("hashsync stream player {} sections {} bytes {}", playerId, toSend.size(), totalBytes);
            }

            server.execute(() -> {
                ServerPlayer player = server.getPlayerList().getPlayer(playerId);
                if (player == null) return;
                if (!player.level().dimension().identifier().equals(dim)) return;
                for (PreSerializedLodPayload pkt : preEncoded) {
                    ServerPlayNetworking.send(player, pkt);
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
            lutBiomeIds[idx] = getCachedBiomeId(mapper, mappingId, biomeRegistry);
            lutLight[idx] = (byte) IdRemapper.getLightFromMapping(mappingId);
        }

        long contentHash = computeContentHash(lutBlockStateIds, lutBiomeIds, lutLight, indexArray);
        return new LODSectionPayload(dimension, section.key, lutBlockStateIds, lutBiomeIds, lutLight, indexArray, contentHash);
    }

    // canonical content fingerprint independent of lut assembly order
    // sort lut slots by blockstate biome light, then hash the sorted table plus per voxel sorted slot
    private static long computeContentHash(int[] lutBlockStateIds, int[] lutBiomeIds, byte[] lutLight, short[] indexArray) {
        int lutLen = lutBlockStateIds.length;
        Integer[] order = new Integer[lutLen];
        for (int i = 0; i < lutLen; i++) order[i] = i;
        Arrays.sort(order, (a, b) -> {
            if (lutBlockStateIds[a] != lutBlockStateIds[b]) return Integer.compare(lutBlockStateIds[a], lutBlockStateIds[b]);
            if (lutBiomeIds[a] != lutBiomeIds[b]) return Integer.compare(lutBiomeIds[a], lutBiomeIds[b]);
            return Integer.compare(lutLight[a] & 0xFF, lutLight[b] & 0xFF);
        });
        int[] remap = new int[lutLen];
        for (int p = 0; p < lutLen; p++) remap[order[p]] = p;

        long h = lutLen;
        for (int p = 0; p < lutLen; p++) {
            int s = order[p];
            h = mix(h, lutBlockStateIds[s] & 0xFFFFFFFFL);
            h = mix(h, lutBiomeIds[s] & 0xFFFFFFFFL);
            h = mix(h, lutLight[s] & 0xFFL);
        }
        for (short slot : indexArray) {
            h = mix(h, remap[slot & 0xFFFF]);
        }
        return fmix64(h ^ ((long) indexArray.length));
    }

    private static long mix(long h, long v) {
        h ^= fmix64(v);
        h = Long.rotateLeft(h, 27) * 0x9E3779B97F4A7C15L + 0x165667B19E3779F9L;
        return h;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
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

    private void onWorldSectionDirty(Identifier dimension, long sectionKey) {
        if (WorldEngine.getLevel(sectionKey) != 0) {
            return;
        }

        long compositeKey = composeSectionKey(dimOrdinals.getOrdinal(dimension), sectionKey);
        hashCacheByKey.remove(compositeKey);
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

        Identifier dimension = dimOrdinals.getDimension(extractSectionDimOrdinal(compositeKey));
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

    private void pushDirtySection(MinecraftServer server, ServerLevel level, Identifier dimension, long sectionKey, int version) {
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

        long composite = composeSectionKey(dimOrdinals.getOrdinal(dimension), sectionKey);
        long hash = payload.contentHash();
        hashCacheByKey.put(composite, new long[]{version, hash});

        PreSerializedLodPayload preSerialized = PreSerializedLodPayload.fromBulk(
                new LODBulkPayload(dimension, List.of(payload)), level.registryAccess());

        for (var entry : trackers.entrySet()) {
            PlayerLodTracker tracker = entry.getValue();
            if (!tracker.isReady() || !tracker.isProtocolOk() || !tracker.isLodEnabled()) {
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

            tracker.markSent(composite, hash);

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
                boolean didWork = drainManifestBatches(MAX_MANIFEST_BATCHES_PER_DRAIN) > 0;
                didWork |= drainQueuedDirtySections(server, MAX_DIRTY_SECTIONS_PER_DRAIN) > 0;

                SnapshotBatch snapshotBatch = pendingSnapshotBatch.getAndSet(null);
                if (snapshotBatch != null) {
                    didWork = true;
                    processSnapshots(snapshotBatch.server(), snapshotBatch.snapshots());
                }

                if (!didWork && pendingManifestBatches.isEmpty()
                        && queuedDirtySections.isEmpty() && pendingSnapshotBatch.get() == null) {
                    return;
                }
            }
        } finally {
            lastStreamHeartbeat = System.nanoTime();
            currentStreamDimension = null;
            streamWorkerScheduled.set(false);
            if (!pendingManifestBatches.isEmpty()
                    || !queuedDirtySections.isEmpty() || pendingSnapshotBatch.get() != null) {
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
            Identifier dim = currentStreamDimension;
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

            if (!pendingManifestBatches.isEmpty()
                    || !queuedDirtySections.isEmpty() || pendingSnapshotBatch.get() != null) {
                scheduleStreamWorker();
            }
        }
    }

    private int drainManifestBatches(int maxBatches) {
        int drained = 0;
        while (drained < maxBatches) {
            ManifestBatch batch = pendingManifestBatches.poll();
            if (batch == null) break;
            drained++;
            lastStreamHeartbeat = System.nanoTime();

            PlayerLodTracker tracker = trackers.get(batch.playerId());
            if (tracker == null || !hashSyncEnabled || !tracker.isActiveDimension(batch.dimension())) continue;

            int dimOrd = dimOrdinals.getOrdinal(batch.dimension());
            tracker.applyManifestBatch(dimOrd, batch.keys(), batch.hashes());
            if (batch.complete()) {
                tracker.completeManifestWait(batch.dimension());
            }
        }
        return drained;
    }

    private void handleVoxyCorruption(Identifier dimension, String context, Exception e) {
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

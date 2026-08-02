package com.dripps.voxyserver.client;

import com.dripps.voxyserver.client.service.IVoxyServerIngestAccess;
import com.dripps.voxyserver.client.service.RemoteIngestService;
import com.dripps.voxyserver.network.LODManifestPayload;
import com.dripps.voxyserver.network.LODPreferencesPayload;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.commonImpl.VoxyCommon;
import me.cortex.voxy.commonImpl.VoxyInstance;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayNetworking;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.multiplayer.ServerData;
import net.minecraft.client.multiplayer.resolver.ServerAddress;
import net.minecraft.resources.Identifier;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ClientLodSettings {
    private static final ClientLodConfig CONFIG = ClientLodConfig.load();

    private static int serverMaxRadius = -1;
    private static int serverMaxSections = -1;
    private static volatile boolean protocolOk = false;
    private static volatile boolean serverHashSyncEnabled = true;

    private static int lastManifestCenterSecX;
    private static int lastManifestCenterSecZ;
    private static Identifier lastManifestDim;
    private static boolean manifestSent = false;

    private static String activeServerKey;
    private static ClientLodConfig.Preferences activePreferences = CONFIG.getPreferencesForServer(null);

    private static final long[] EMPTY_LONG = new long[0];
    private static final AtomicReference<ManifestRequest> pendingManifestRequest = new AtomicReference<>();
    private static final AtomicReference<ManifestDispatch> pendingManifestDispatch = new AtomicReference<>();
    private static final AtomicBoolean manifestBuildScheduled = new AtomicBoolean(false);
    private static final AtomicLong connectionGeneration = new AtomicLong();
    private static final AtomicLong manifestRequestGeneration = new AtomicLong();

    private record ManifestRequest(WorldIdentifier worldId, Identifier dim,
                                   int playerSecX, int playerSecZ, int radiusSections,
                                   long connectionGeneration, long requestGeneration) {}

    private static final class ManifestSupersededException extends RuntimeException {}

    private static final class ManifestDispatch {
        private final Identifier dimension;
        private final LongArrayList keys;
        private final LongArrayList hashes;
        private final long connectionGeneration;
        private final long requestGeneration;
        private int cursor;

        private ManifestDispatch(Identifier dimension, LongArrayList keys, LongArrayList hashes,
                                 long connectionGeneration, long requestGeneration) {
            this.dimension = dimension;
            this.keys = keys;
            this.hashes = hashes;
            this.connectionGeneration = connectionGeneration;
            this.requestGeneration = requestGeneration;
        }
    }

    public static void prepareForCurrentConnection() {
        serverMaxRadius = -1;
        serverMaxSections = -1;
        protocolOk = false;
        serverHashSyncEnabled = true;
        lastManifestDim = null;
        manifestSent = false;
        pendingManifestRequest.set(null);
        pendingManifestDispatch.set(null);
        manifestBuildScheduled.set(false);
        connectionGeneration.incrementAndGet();
        manifestRequestGeneration.incrementAndGet();
        activeServerKey = resolveCurrentServerKey();
        activePreferences = CONFIG.getPreferencesForServer(activeServerKey);
    }

    public static boolean isProtocolOk() {
        return protocolOk;
    }

    public static void setProtocolOk(boolean value) {
        protocolOk = value;
        if (value && serverHashSyncEnabled && serverMaxRadius >= 0) {
            requestManifestBuild();
        }
    }

    public static void applyServerSettings(int maxRadius, int maxSections) {
        boolean radiusChanged = serverMaxRadius != maxRadius;
        serverMaxRadius = maxRadius;
        serverMaxSections = maxSections;
        sendPreferences();
        if (protocolOk && serverHashSyncEnabled && radiusChanged) {
            requestManifestBuild();
        }
    }

    public static void applyHashSyncSettings(boolean enabled) {
        boolean wasEnabled = serverHashSyncEnabled;
        serverHashSyncEnabled = enabled;
        if (!enabled) {
            pendingManifestRequest.set(null);
            pendingManifestDispatch.set(null);
            manifestRequestGeneration.incrementAndGet();
            manifestSent = false;
            lastManifestDim = null;
        } else if (!wasEnabled && protocolOk && serverMaxRadius >= 0) {
            requestManifestBuild();
        }
    }

    public static void onClientTick() {
        if (!protocolOk || !serverHashSyncEnabled || !isEnabled() || serverMaxRadius < 0) return;

        Minecraft mc = Minecraft.getInstance();
        if (mc.getConnection() == null) return;
        ClientLevel level = mc.level;
        var player = mc.player;
        if (level == null || player == null) return;

        Identifier dim = level.dimension().identifier();
        int playerSecX = (player.getBlockX() >> 4) >> 1;
        int playerSecZ = (player.getBlockZ() >> 4) >> 1;

        boolean needRebuild;
        if (!manifestSent) {
            needRebuild = false;
        } else if (lastManifestDim == null || !lastManifestDim.equals(dim)) {
            needRebuild = true;
        } else {
            int radiusSections = Math.max(0, effectiveRadius()) >> 1;
            int threshold = Math.max(4, radiusSections >> 1);
            needRebuild = Math.abs(playerSecX - lastManifestCenterSecX) >= threshold
                    || Math.abs(playerSecZ - lastManifestCenterSecZ) >= threshold;
        }

        if (needRebuild) {
            requestManifestBuild();
        }

        sendPendingManifestChunk(dim);
    }

    // client preferred radius clamped to the server max, mirrors the server side computation
    private static int effectiveRadius() {
        int preferred = activePreferences.preferredRadius;
        return (preferred <= 0) ? serverMaxRadius : Math.min(preferred, serverMaxRadius);
    }

    // tells the server which sections we already store so it can skip resending them
    // captures player state on the client thread then hands the heavy build to the ingest worker
    private static void requestManifestBuild() {
        if (!serverHashSyncEnabled) return;
        Minecraft mc = Minecraft.getInstance();
        if (mc.getConnection() == null) return;
        ClientLevel level = mc.level;
        var player = mc.player;
        if (level == null || player == null) return;

        WorldIdentifier worldId = WorldIdentifier.of(level);
        if (worldId == null) return;
        Identifier dim = level.dimension().identifier();

        int radiusSections = Math.max(0, effectiveRadius()) >> 1;
        int playerSecX = (player.getBlockX() >> 4) >> 1;
        int playerSecZ = (player.getBlockZ() >> 4) >> 1;

        lastManifestCenterSecX = playerSecX;
        lastManifestCenterSecZ = playerSecZ;
        lastManifestDim = dim;
        manifestSent = true;

        pendingManifestDispatch.set(null);
        pendingManifestRequest.set(new ManifestRequest(
                worldId,
                dim,
                playerSecX,
                playerSecZ,
                radiusSections,
                connectionGeneration.get(),
                manifestRequestGeneration.incrementAndGet()
        ));
        scheduleManifestBuild();
    }

    private static void scheduleManifestBuild() {
        if (!manifestBuildScheduled.compareAndSet(false, true)) return;
        RemoteIngestService service = getIngestService();
        if (service == null || !service.isLive()) {
            manifestBuildScheduled.set(false);
            return;
        }
        service.enqueueManifestBuild(ClientLodSettings::runManifestBuildJob);
    }

    private static void runManifestBuildJob() {
        try {
            ManifestRequest request = pendingManifestRequest.getAndSet(null);
            if (request != null) {
                buildManifest(request);
            }
        } finally {
            manifestBuildScheduled.set(false);
            if (pendingManifestRequest.get() != null) {
                scheduleManifestBuild();
            }
        }
    }

    private static RemoteIngestService getIngestService() {
        VoxyInstance instance = VoxyCommon.getInstance();
        if (instance == null) return null;
        return ((IVoxyServerIngestAccess) instance).voxyserver$getRemoteIngestService();
    }

    private static void buildManifest(ManifestRequest request) {
        try {
            if (VoxyCommon.getInstance() == null) {
                dispatchManifest(request, null, null);
                return;
            }

            WorldEngine engine = request.worldId().getOrCreateEngine();
            if (engine == null || !engine.isLive()) {
                dispatchManifest(request, null, null);
                return;
            }

            engine.acquireRef();
            try {
                Long2LongOpenHashMap stored = ClientLodHashStore.get().snapshot(request.worldId().getWorldId());

                int playerSecX = request.playerSecX();
                int playerSecZ = request.playerSecZ();
                int radiusSections = request.radiusSections();

                LongArrayList keys = new LongArrayList();
                LongArrayList hashes = new LongArrayList();
                engine.storage.iteratePositions(0, key -> {
                    if (!isCurrentManifestRequest(request)) throw new ManifestSupersededException();
                    int sx = WorldEngine.getX(key);
                    int sz = WorldEngine.getZ(key);
                    if (Math.abs(sx - playerSecX) > radiusSections
                            || Math.abs(sz - playerSecZ) > radiusSections
                            || !stored.containsKey(key)) {
                        return;
                    }
                    keys.add(key);
                    hashes.add(stored.get(key));
                });

                if (isCurrentManifestRequest(request)) {
                    dispatchManifest(request, keys, hashes);
                }
            } finally {
                engine.releaseRef();
            }
        } catch (ManifestSupersededException ignored) {
        } catch (Exception e) {
            me.cortex.voxy.common.Logger.error("voxyserver manifest build failed, server will full send", e);
            if (isCurrentManifestRequest(request)) {
                dispatchManifest(request, null, null);
            }
        }
    }

    private static boolean isCurrentManifestRequest(ManifestRequest request) {
        return serverHashSyncEnabled
                && connectionGeneration.get() == request.connectionGeneration()
                && manifestRequestGeneration.get() == request.requestGeneration();
    }

    private static void dispatchManifest(ManifestRequest request, LongArrayList keys, LongArrayList hashes) {
        LongArrayList dispatchKeys = keys == null ? new LongArrayList() : keys;
        LongArrayList dispatchHashes = hashes == null ? new LongArrayList() : hashes;
        ManifestDispatch dispatch = new ManifestDispatch(
                request.dim(),
                dispatchKeys,
                dispatchHashes,
                request.connectionGeneration(),
                request.requestGeneration()
        );
        Minecraft.getInstance().execute(() -> {
            if (Minecraft.getInstance().getConnection() == null) return;
            if (connectionGeneration.get() != dispatch.connectionGeneration) return;
            if (manifestRequestGeneration.get() != dispatch.requestGeneration) return;
            pendingManifestDispatch.set(dispatch);
        });
    }

    private static void sendPendingManifestChunk(Identifier currentDimension) {
        if (!serverHashSyncEnabled) return;
        ManifestDispatch dispatch = pendingManifestDispatch.get();
        if (dispatch == null) return;
        if (connectionGeneration.get() != dispatch.connectionGeneration
                || manifestRequestGeneration.get() != dispatch.requestGeneration
                || !dispatch.dimension.equals(currentDimension)) {
            pendingManifestDispatch.compareAndSet(dispatch, null);
            return;
        }

        int total = dispatch.keys.size();
        int end = Math.min(total, dispatch.cursor + LODManifestPayload.MAX_ENTRIES);
        int count = end - dispatch.cursor;
        long[] keys = count == 0 ? EMPTY_LONG : new long[count];
        long[] hashes = count == 0 ? EMPTY_LONG : new long[count];
        for (int i = 0; i < count; i++) {
            keys[i] = dispatch.keys.getLong(dispatch.cursor + i);
            hashes[i] = dispatch.hashes.getLong(dispatch.cursor + i);
        }

        dispatch.cursor = end;
        boolean complete = end >= total;
        sendManifestChunk(dispatch.dimension, keys, hashes, complete);
        if (complete) {
            pendingManifestDispatch.compareAndSet(dispatch, null);
        }
    }

    private static void sendManifestChunk(Identifier dimension, long[] keys, long[] hashes, boolean complete) {
        if (Minecraft.getInstance().getConnection() == null) return;
        ClientPlayNetworking.send(new LODManifestPayload(dimension, keys, hashes, complete));
    }

    public static void reset() {
        serverMaxRadius = -1;
        serverMaxSections = -1;
        protocolOk = false;
        serverHashSyncEnabled = true;
        lastManifestDim = null;
        manifestSent = false;
        pendingManifestRequest.set(null);
        pendingManifestDispatch.set(null);
        manifestBuildScheduled.set(false);
        connectionGeneration.incrementAndGet();
        manifestRequestGeneration.incrementAndGet();
        activeServerKey = null;
        activePreferences = CONFIG.getPreferencesForServer(null);
    }

    public static boolean hasActiveServerProfile() {
        return activeServerKey != null;
    }

    public static boolean isEnabled() {
        return activePreferences.enabled;
    }

    public static int getServerMaxRadius() {
        return serverMaxRadius;
    }

    public static int getServerMaxSections() {
        return serverMaxSections;
    }

    public static int getPreferredRadius() {
        return activePreferences.preferredRadius;
    }

    public static int getPreferredMaxSections() {
        return activePreferences.preferredMaxSections;
    }

    public static void setEnabled(boolean value) {
        activePreferences.enabled = value;
    }

    public static void setPreferredRadius(int radius) {
        activePreferences.preferredRadius = Math.max(0, radius);
    }

    public static void setPreferredMaxSections(int maxSections) {
        activePreferences.preferredMaxSections = Math.max(0, maxSections);
    }

    public static void saveAndSendPreferences() {
        saveActiveProfile();
        sendPreferences();
    }

    public static void sendPreferences() {
        if (Minecraft.getInstance().getConnection() == null) {
            return;
        }

        ClientPlayNetworking.send(new LODPreferencesPayload(
                activePreferences.enabled,
                activePreferences.preferredRadius,
                activePreferences.preferredMaxSections));
    }

    private static void saveActiveProfile() {
        if (activeServerKey == null) {
            return;
        }

        CONFIG.savePreferencesForServer(activeServerKey, activePreferences);
    }

    private static String resolveCurrentServerKey() {
        Minecraft minecraft = Minecraft.getInstance();
        if (minecraft.hasSingleplayerServer() || minecraft.isLocalServer()) {
            return null;
        }

        ServerData currentServer = minecraft.getCurrentServer();
        if (currentServer == null || currentServer.isLan() || currentServer.ip == null || currentServer.ip.isBlank()) {
            return null;
        }

        ServerAddress address = ServerAddress.parseString(currentServer.ip);
        String host = address.getHost();
        if (host == null || host.isBlank()) {
            return null;
        }

        return host.toLowerCase(Locale.ROOT) + ":" + address.getPort();
    }
}

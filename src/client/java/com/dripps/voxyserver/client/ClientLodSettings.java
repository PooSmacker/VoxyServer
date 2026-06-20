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
import java.util.concurrent.atomic.AtomicReference;

public class ClientLodSettings {
    private static final ClientLodConfig CONFIG = ClientLodConfig.load();

    private static int serverMaxRadius = -1;
    private static int serverMaxSections = -1;
    private static volatile boolean protocolOk = false;

    private static int lastManifestCenterSecX;
    private static int lastManifestCenterSecZ;
    private static Identifier lastManifestDim;
    private static boolean manifestSent = false;

    private static String activeServerKey;
    private static ClientLodConfig.Preferences activePreferences = CONFIG.getPreferencesForServer(null);

    private static final long[] EMPTY_LONG = new long[0];
    private static final AtomicReference<ManifestRequest> pendingManifestRequest = new AtomicReference<>();
    private static final AtomicBoolean manifestBuildScheduled = new AtomicBoolean(false);

    private record ManifestRequest(WorldIdentifier worldId, Identifier dim,
                                   int playerSecX, int playerSecZ, int radiusSections) {}

    public static void prepareForCurrentConnection() {
        serverMaxRadius = -1;
        serverMaxSections = -1;
        protocolOk = false;
        lastManifestDim = null;
        manifestSent = false;
        pendingManifestRequest.set(null);
        manifestBuildScheduled.set(false);
        activeServerKey = resolveCurrentServerKey();
        activePreferences = CONFIG.getPreferencesForServer(activeServerKey);
    }

    private static final int MANIFEST_CHUNK = 4096;

    public static boolean isProtocolOk() {
        return protocolOk;
    }

    public static void setProtocolOk(boolean value) {
        protocolOk = value;
    }

    public static void applyServerSettings(int maxRadius, int maxSections) {
        serverMaxRadius = maxRadius;
        serverMaxSections = maxSections;
        sendPreferences();
        if (protocolOk) {
            requestManifestBuild();
        }
    }

    public static void onClientTick() {
        if (!protocolOk || !isEnabled() || serverMaxRadius < 0) return;

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
    }

    // client preferred radius clamped to the server max, mirrors the server side computation
    private static int effectiveRadius() {
        int preferred = activePreferences.preferredRadius;
        return (preferred <= 0) ? serverMaxRadius : Math.min(preferred, serverMaxRadius);
    }

    // tells the server which sections we already store so it can skip resending them
    // captures player state on the client thread then hands the heavy build to the ingest worker
    private static void requestManifestBuild() {
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

        pendingManifestRequest.set(new ManifestRequest(worldId, dim, playerSecX, playerSecZ, radiusSections));
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
        Identifier dim = request.dim();
        try {
            if (VoxyCommon.getInstance() == null) {
                dispatchManifest(dim, null, null);
                return;
            }

            Long2LongOpenHashMap stored = ClientLodHashStore.get().snapshot(request.worldId().getWorldId());

            int playerSecX = request.playerSecX();
            int playerSecZ = request.playerSecZ();
            int radiusSections = request.radiusSections();

            LongArrayList keys = new LongArrayList();
            LongArrayList hashes = new LongArrayList();
            for (var entry : stored.long2LongEntrySet()) {
                long key = entry.getLongKey();
                if (WorldEngine.getLevel(key) != 0) continue;
                int sx = WorldEngine.getX(key);
                int sz = WorldEngine.getZ(key);
                if (Math.abs(sx - playerSecX) > radiusSections || Math.abs(sz - playerSecZ) > radiusSections) continue;
                keys.add(key);
                hashes.add(entry.getLongValue());
            }

            dispatchManifest(dim, keys, hashes);
        } catch (Exception e) {
            me.cortex.voxy.common.Logger.error("voxyserver manifest build failed, server will full send", e);
            dispatchManifest(dim, null, null);
        }
    }

    private static void dispatchManifest(Identifier dim, LongArrayList keys, LongArrayList hashes) {
        int total = (keys == null) ? 0 : keys.size();
        if (total == 0) {
            Minecraft.getInstance().execute(() -> sendManifestChunk(dim, EMPTY_LONG, EMPTY_LONG, true));
            return;
        }

        int chunkCount = (total + MANIFEST_CHUNK - 1) / MANIFEST_CHUNK;
        long[][] keyChunks = new long[chunkCount][];
        long[][] hashChunks = new long[chunkCount][];
        for (int c = 0, i = 0; i < total; c++, i += MANIFEST_CHUNK) {
            int end = Math.min(total, i + MANIFEST_CHUNK);
            int n = end - i;
            long[] k = new long[n];
            long[] h = new long[n];
            for (int j = 0; j < n; j++) {
                k[j] = keys.getLong(i + j);
                h[j] = hashes.getLong(i + j);
            }
            keyChunks[c] = k;
            hashChunks[c] = h;
        }

        Minecraft.getInstance().execute(() -> {
            if (Minecraft.getInstance().getConnection() == null) return;
            for (int c = 0; c < keyChunks.length; c++) {
                sendManifestChunk(dim, keyChunks[c], hashChunks[c], c == keyChunks.length - 1);
            }
        });
    }

    private static void sendManifestChunk(Identifier dimension, long[] keys, long[] hashes, boolean complete) {
        if (Minecraft.getInstance().getConnection() == null) return;
        ClientPlayNetworking.send(new LODManifestPayload(dimension, keys, hashes, complete));
    }

    public static void reset() {
        serverMaxRadius = -1;
        serverMaxSections = -1;
        protocolOk = false;
        lastManifestDim = null;
        manifestSent = false;
        pendingManifestRequest.set(null);
        manifestBuildScheduled.set(false);
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

package com.dripps.voxyserver.client;

import com.dripps.voxyserver.network.LODManifestPayload;
import com.dripps.voxyserver.network.LODPreferencesPayload;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayNetworking;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.multiplayer.ServerData;
import net.minecraft.client.multiplayer.resolver.ServerAddress;
import net.minecraft.resources.Identifier;

import java.util.Locale;

public class ClientLodSettings {
    private static final ClientLodConfig CONFIG = ClientLodConfig.load();

    private static int serverMaxRadius = -1;
    private static int serverMaxSections = -1;
    private static volatile boolean protocolOk = false;

    private static int lastManifestCenterSecX;
    private static int lastManifestCenterSecZ;
    private static Identifier lastManifestDim;
    private static boolean manifestSent = false;
    private static volatile boolean manifestBuilding = false;

    private static String activeServerKey;
    private static ClientLodConfig.Preferences activePreferences = CONFIG.getPreferencesForServer(null);

    public static void prepareForCurrentConnection() {
        serverMaxRadius = -1;
        serverMaxSections = -1;
        protocolOk = false;
        lastManifestDim = null;
        manifestSent = false;
        manifestBuilding = false;
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
            buildAndSendManifest();
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
            buildAndSendManifest();
        }
    }

    // client preferred radius clamped to the server max, mirrors the server side computation
    private static int effectiveRadius() {
        int preferred = activePreferences.preferredRadius;
        return (preferred <= 0) ? serverMaxRadius : Math.min(preferred, serverMaxRadius);
    }

    // tells the server which sections we already store so it can skip resending them.
    // Cheap capture happens here on the render thread; the heavy hash-store load + copy +
    // iteration is offloaded to a background thread so it never blocks rendering.
    private static void buildAndSendManifest() {
        Minecraft mc = Minecraft.getInstance();
        if (mc.getConnection() == null) return;
        ClientLevel level = mc.level;
        var player = mc.player;
        if (level == null || player == null) return;
        WorldIdentifier worldId = WorldIdentifier.of(level);
        if (worldId == null) return;
        Identifier dim = level.dimension().identifier();

        // a build is already running; onClientTick will retry later if the player keeps moving
        if (manifestBuilding) return;

        final int radiusSections = Math.max(0, effectiveRadius()) >> 1;
        final int playerSecX = (player.getBlockX() >> 4) >> 1;
        final int playerSecZ = (player.getBlockZ() >> 4) >> 1;

        lastManifestCenterSecX = playerSecX;
        lastManifestCenterSecZ = playerSecZ;
        lastManifestDim = dim;
        manifestSent = true;
        manifestBuilding = true;

        final WorldIdentifier fWorldId = worldId;
        final Identifier fDim = dim;
        Thread t = new Thread(() -> {
            try {
                buildAndSendManifestWorker(fWorldId, fDim, playerSecX, playerSecZ, radiusSections);
            } catch (Exception e) {
                me.cortex.voxy.common.Logger.error("voxyserver manifest build failed, server will full send", e);
                sendManifestChunk(fDim, new long[0], new long[0], true);
            } finally {
                manifestBuilding = false;
            }
        }, "VoxyServer-ManifestBuilder");
        t.setDaemon(true);
        t.start();
    }

    // Runs on a background thread. Built from the in-memory hash record of sections we have
    // already received (loading that record from its sidecar file can be large), NOT by walking
    // Voxy's on-disk RocksDB store. Neither operation touches the render thread.
    private static void buildAndSendManifestWorker(WorldIdentifier worldId, Identifier dim,
                                                   int playerSecX, int playerSecZ, int radiusSections) {
        Long2LongOpenHashMap stored = ClientLodHashStore.get().snapshot(worldId.getWorldId());

        LongArrayList keys = new LongArrayList();
        LongArrayList hashes = new LongArrayList();
        for (var entry : stored.long2LongEntrySet()) {
            long key = entry.getLongKey();
            int sx = WorldEngine.getX(key);
            int sz = WorldEngine.getZ(key);
            if (Math.abs(sx - playerSecX) > radiusSections || Math.abs(sz - playerSecZ) > radiusSections) continue;
            keys.add(key);
            hashes.add(entry.getLongValue());
        }

        int total = keys.size();
        if (total == 0) {
            sendManifestChunk(dim, new long[0], new long[0], true);
            return;
        }
        for (int i = 0; i < total; i += MANIFEST_CHUNK) {
            int end = Math.min(total, i + MANIFEST_CHUNK);
            int n = end - i;
            long[] k = new long[n];
            long[] h = new long[n];
            for (int j = 0; j < n; j++) {
                k[j] = keys.getLong(i + j);
                h[j] = hashes.getLong(i + j);
            }
            sendManifestChunk(dim, k, h, end >= total);
        }
    }

    // may be called from the manifest worker thread, so send via the client executor
    private static void sendManifestChunk(Identifier dimension, long[] keys, long[] hashes, boolean complete) {
        Minecraft mc = Minecraft.getInstance();
        mc.execute(() -> {
            if (mc.getConnection() == null) return;
            ClientPlayNetworking.send(new LODManifestPayload(dimension, keys, hashes, complete));
        });
    }

    public static void reset() {
        serverMaxRadius = -1;
        serverMaxSections = -1;
        protocolOk = false;
        lastManifestDim = null;
        manifestSent = false;
        manifestBuilding = false;
        activeServerKey = null;
        activePreferences = CONFIG.getPreferencesForServer(null);
    }

    public static boolean hasActiveServerProfile() {
        return activeServerKey != null;
    }

    public static boolean isEnabled() {
        return activePreferences.enabled;
    }

    public static boolean isDownloadHudEnabled() {
        return Boolean.TRUE.equals(CONFIG.showDownloadHud);
    }

    public static void setDownloadHudEnabled(boolean value) {
        CONFIG.showDownloadHud = value;
    }

    public static boolean isDownloadHudTopLeft() {
        return Boolean.TRUE.equals(CONFIG.hudTopLeft);
    }

    public static void setDownloadHudTopLeft(boolean value) {
        CONFIG.hudTopLeft = value;
    }

    public static void saveClientConfig() {
        CONFIG.save();
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

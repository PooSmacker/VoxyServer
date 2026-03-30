package com.dripps.voxyserver.server;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import me.cortex.voxy.common.world.WorldEngine;
import net.minecraft.server.level.ServerPlayer;

public class PlayerLodTracker {
    public static final long NO_SECTION_KEY = -1L;

    private final Long2IntOpenHashMap sentSectionVersions = new Long2IntOpenHashMap();
    private volatile boolean ready = false;
    private int lastChunkX;
    private int lastChunkZ;

    private volatile boolean lodEnabled = true;
    private volatile int preferredRadius = -1;
    private volatile int preferredMaxSections = -1;

    private int scanCenterSecX;
    private int scanCenterSecZ;
    private int scanRadiusSections = -1;
    private int scanMinSectionY;
    private int scanMaxSectionY;
    private int scanCursorDist;
    private int scanCursorDx;
    private int scanCursorDz;
    private int scanCursorY;
    private boolean scanGeometryInitialized;
    private boolean scanCursorInitialized;
    private boolean scanExhausted;
    private long nextFullRescanTick;

    public PlayerLodTracker() {
        this.sentSectionVersions.defaultReturnValue(-1);
    }

    public boolean isReady() {
        return ready;
    }

    public void setReady(boolean ready) {
        this.ready = ready;
    }

    public synchronized boolean hasSent(long sectionKey, int version) {
        return sentSectionVersions.get(sectionKey) == version;
    }

    public synchronized void markSent(long sectionKey, int version) {
        sentSectionVersions.put(sectionKey, version);
    }

    public synchronized void reset() {
        sentSectionVersions.clear();
        resetScanStateLocked();
    }

    public synchronized void invalidate(long sectionKey) {
        sentSectionVersions.remove(sectionKey);
    }

    public int getLastChunkX() {
        return lastChunkX;
    }

    public int getLastChunkZ() {
        return lastChunkZ;
    }

    public void updatePosition(ServerPlayer player) {
        this.lastChunkX = player.getBlockX() >> 4;
        this.lastChunkZ = player.getBlockZ() >> 4;
    }

    public synchronized int sentCount() {
        return sentSectionVersions.size();
    }

    public synchronized boolean prepareScan(int centerSecX, int centerSecZ,
                                            int radiusSections,
                                            int minSectionY,
                                            int maxSectionY,
                                            long currentTick,
                                            long idleRescanIntervalTicks) {
        boolean geometryChanged = !scanGeometryInitialized
                || scanRadiusSections != radiusSections
                || scanMinSectionY != minSectionY
                || scanMaxSectionY != maxSectionY;

        boolean centerChanged = !scanGeometryInitialized
                || scanCenterSecX != centerSecX
                || scanCenterSecZ != centerSecZ;

        scanCenterSecX = centerSecX;
        scanCenterSecZ = centerSecZ;
        scanRadiusSections = radiusSections;
        scanMinSectionY = minSectionY;
        scanMaxSectionY = maxSectionY;
        scanGeometryInitialized = true;

        if (maxSectionY <= minSectionY) {
            markScanExhaustedLocked(currentTick, idleRescanIntervalTicks);
            return false;
        }

        if (geometryChanged || centerChanged) {
            resetScanCursorLocked();
            scanExhausted = false;
        }

        if (scanExhausted) {
            if (currentTick < nextFullRescanTick) {
                return false;
            }
            resetScanCursorLocked();
            scanExhausted = false;
        }

        if (!scanCursorInitialized) {
            resetScanCursorLocked();
        }

        return true;
    }

    public synchronized long nextSectionKeyToScan(long currentTick, long idleRescanIntervalTicks) {
        if (scanExhausted || !scanGeometryInitialized) {
            return NO_SECTION_KEY;
        }

        if (!scanCursorInitialized) {
            if (scanCursorDist > scanRadiusSections || scanMaxSectionY <= scanMinSectionY) {
                markScanExhaustedLocked(currentTick, idleRescanIntervalTicks);
                return NO_SECTION_KEY;
            }
            resetScanCursorLocked();
        }

        if (scanCursorDist > scanRadiusSections || scanMaxSectionY <= scanMinSectionY) {
            markScanExhaustedLocked(currentTick, idleRescanIntervalTicks);
            return NO_SECTION_KEY;
        }

        long sectionKey = WorldEngine.getWorldSectionId(
                0,
                scanCenterSecX + scanCursorDx,
                scanCursorY,
                scanCenterSecZ + scanCursorDz
        );

        advanceScanCursorLocked();
        return sectionKey;
    }

    public boolean isLodEnabled() {
        return lodEnabled;
    }

    public void setLodEnabled(boolean enabled) {
        this.lodEnabled = enabled;
    }

    public void setPreferredRadius(int radius) {
        this.preferredRadius = radius;
    }

    public void setPreferredMaxSections(int maxSections) {
        this.preferredMaxSections = maxSections;
    }

    // returns the clients preferred radius clamped to the server max, or server default if unset
    public int getEffectiveRadius(int serverMax) {
        return (preferredRadius <= 0) ? serverMax : Math.min(preferredRadius, serverMax);
    }

    // returns the clients preferred rate clamped to the server max, or server default if unset
    public int getEffectiveMaxSections(int serverMax) {
        return (preferredMaxSections <= 0) ? serverMax : Math.min(preferredMaxSections, serverMax);
    }

    private void resetScanStateLocked() {
        scanGeometryInitialized = false;
        scanExhausted = false;
        nextFullRescanTick = 0L;
        resetScanCursorLocked();
    }

    private void resetScanCursorLocked() {
        scanCursorDist = 0;
        scanCursorDx = 0;
        scanCursorDz = 0;
        scanCursorY = scanMinSectionY;
        scanCursorInitialized = true;
    }

    private void advanceScanCursorLocked() {
        scanCursorY++;
        if (scanCursorY < scanMaxSectionY) {
            return;
        }

        advanceScanColumnLocked();
    }

    private void advanceScanColumnLocked() {
        if (scanCursorDist == 0) {
            scanCursorDist = 1;
            if (scanCursorDist > scanRadiusSections) {
                scanCursorInitialized = false;
                return;
            }

            scanCursorDx = -scanCursorDist;
            scanCursorDz = -scanCursorDist;
            scanCursorY = scanMinSectionY;
            return;
        }

        while (true) {
            scanCursorDz++;
            while (scanCursorDx <= scanCursorDist) {
                while (scanCursorDz <= scanCursorDist) {
                    if (Math.abs(scanCursorDx) == scanCursorDist || Math.abs(scanCursorDz) == scanCursorDist) {
                        scanCursorY = scanMinSectionY;
                        return;
                    }
                    scanCursorDz++;
                }
                scanCursorDx++;
                scanCursorDz = -scanCursorDist;
            }

            scanCursorDist++;
            if (scanCursorDist > scanRadiusSections) {
                scanCursorInitialized = false;
                return;
            }

            scanCursorDx = -scanCursorDist;
            scanCursorDz = -scanCursorDist;
            scanCursorY = scanMinSectionY;
            return;
        }
    }

    private void markScanExhaustedLocked(long currentTick, long idleRescanIntervalTicks) {
        scanExhausted = true;
        scanCursorInitialized = false;
        nextFullRescanTick = currentTick + idleRescanIntervalTicks;
    }
}

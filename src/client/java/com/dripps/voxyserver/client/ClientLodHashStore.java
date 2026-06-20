package com.dripps.voxyserver.client;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import me.cortex.voxy.common.Logger;
import net.fabricmc.loader.api.FabricLoader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

// per server world sidecar of section content hashes for hash sync
// worldId is voxy's per dimension world id so one file holds one dimension
public class ClientLodHashStore {
    private static final ClientLodHashStore INSTANCE = new ClientLodHashStore();
    public static ClientLodHashStore get() { return INSTANCE; }

    private static final int FLUSH_EVERY_DIRTY = 512;

    private final Object lock = new Object();
    private String currentWorldId;
    private Long2LongOpenHashMap map;
    private int dirtyCount;

    private static Path fileFor(String worldId) {
        return FabricLoader.getInstance().getGameDir()
                .resolve("voxyserver").resolve("hashes").resolve(worldId + ".bin");
    }

    // swaps the in memory map to the requested world, flushing the previous one first
    private void ensureLoadedLocked(String worldId) {
        if (worldId.equals(currentWorldId) && map != null) return;
        flushLocked();
        currentWorldId = worldId;
        map = load(worldId);
        dirtyCount = 0;
    }

    public void put(String worldId, long sectionKey, long hash) {
        synchronized (lock) {
            ensureLoadedLocked(worldId);
            map.put(sectionKey, hash);
            if (++dirtyCount >= FLUSH_EVERY_DIRTY) {
                flushLocked();
            }
        }
    }

    // copy of the current world map for manifest building
    public Long2LongOpenHashMap snapshot(String worldId) {
        synchronized (lock) {
            ensureLoadedLocked(worldId);
            return new Long2LongOpenHashMap(map);
        }
    }

    public void flush() {
        synchronized (lock) {
            flushLocked();
        }
    }

    private void flushLocked() {
        if (map == null || currentWorldId == null || dirtyCount == 0) return;
        Path file = fileFor(currentWorldId);
        try {
            Files.createDirectories(file.getParent());
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
                out.writeInt(map.size());
                for (var entry : map.long2LongEntrySet()) {
                    out.writeLong(entry.getLongKey());
                    out.writeLong(entry.getLongValue());
                }
            }
            dirtyCount = 0;
        } catch (IOException ex) {
            Logger.error("failed to flush voxyserver hash sidecar", ex);
        }
    }

    private static Long2LongOpenHashMap load(String worldId) {
        Path file = fileFor(worldId);
        if (!Files.exists(file)) {
            return newSizedMap(16);
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
            int count = in.readInt();
            // pre-size to the entry count: this sidecar can hold millions of sections, and a
            // default-capacity map would rehash repeatedly while loading, taking long enough that
            // the join manifest misses the server's wait window (causing a full resend).
            Long2LongOpenHashMap loaded = newSizedMap(Math.max(16, count));
            for (int i = 0; i < count; i++) {
                long key = in.readLong();
                long hash = in.readLong();
                loaded.put(key, hash);
            }
            return loaded;
        } catch (IOException ex) {
            Logger.error("failed to load voxyserver hash sidecar, treating as empty", ex);
            return newSizedMap(16);
        }
    }

    private static Long2LongOpenHashMap newSizedMap(int expected) {
        Long2LongOpenHashMap m = new Long2LongOpenHashMap(expected, 0.75f);
        m.defaultReturnValue(0L);
        return m;
    }
}

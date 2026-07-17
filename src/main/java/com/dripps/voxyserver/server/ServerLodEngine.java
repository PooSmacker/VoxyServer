package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import me.cortex.voxy.common.StorageConfigUtil;
import me.cortex.voxy.common.config.ConfigBuildCtx;
import me.cortex.voxy.common.config.section.SectionStorage;
import me.cortex.voxy.common.config.section.SectionSerializationStorage;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.commonImpl.VoxyInstance;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.minecraft.resources.Identifier;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.chunk.LevelChunk;

import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;

public class ServerLodEngine extends VoxyInstance {
    @FunctionalInterface
    public interface DirtySectionListener {
        void onSectionDirty(Identifier dimension, long sectionKey);
    }

    private final Path basePath;
    private final SectionSerializationStorage.Config storageConfig;
    private final ConcurrentHashMap<WorldIdentifier, Identifier> dimensionsByWorld = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<WorldIdentifier, StoredSectionPresenceIndex> presenceIndexes = new ConcurrentHashMap<>();
    private final Object ingestReferenceLock = new Object();
    private final Set<WorldEngine> ingestReferencedWorlds = Collections.newSetFromMap(new IdentityHashMap<>());
    private volatile DirtySectionListener dirtySectionListener;

    public ServerLodEngine(Path worldFolder) {
        super();
        this.basePath = worldFolder.resolve("voxyserver");
        this.storageConfig = StorageConfigUtil.createDefaultSerializer();
        this.updateDedicatedThreads();
        Voxyserver.LOGGER.info("server lod engine started, storage at {}", this.basePath);
    }

    public void updateDedicatedThreadsCount(int threads){
        this.setNumThreads(threads);
    }

    public void setDirtySectionListener(DirtySectionListener dirtySectionListener) {
        this.dirtySectionListener = dirtySectionListener;
    }

    public WorldEngine getOrCreate(ServerLevel level) {
        WorldIdentifier worldId = WorldIdentifier.of(level);
        if (worldId == null) {
            return null;
        }
        return this.getOrCreate(worldId, level.dimension().identifier());
    }

    public WorldEngine getOrCreate(WorldIdentifier identifier, Identifier dimension) {
        if (identifier == null || !this.isRunning()) {
            return null;
        }
        this.dimensionsByWorld.put(identifier, dimension);
        WorldEngine world;
        try {
            world = super.getOrCreate(identifier);
        } catch (Exception e) {
            Voxyserver.LOGGER.error("couldnt get or create world for {}, this is prolly a leaked lock in VoxyInstance", identifier, e);
            return null;
        }
        if (world == null) {
            return null;
        }
        StoredSectionPresenceIndex presenceIndex = this.presenceIndexes.computeIfAbsent(
                identifier, ignored -> new StoredSectionPresenceIndex());
        this.attachDirtyCallback(identifier, world, presenceIndex);
        return world;
    }

    @Override
    public WorldEngine getOrCreate(WorldIdentifier identifier) {
        if (!this.isRunning()) {
            return null;
        }
        WorldEngine world;
        try {
            world = super.getOrCreate(identifier);
        } catch (Exception e) {
            Voxyserver.LOGGER.error("couldnt get or create world for {}, this is prolly a leaked lock in VoxyInstance", identifier, e);
            return null;
        }
        if (world == null) {
            return null;
        }
        StoredSectionPresenceIndex presenceIndex = this.presenceIndexes.computeIfAbsent(
                identifier, ignored -> new StoredSectionPresenceIndex());
        this.attachDirtyCallback(identifier, world, presenceIndex);
        return world;
    }

    boolean prepareStoredSectionPresence(WorldIdentifier identifier, WorldEngine world, Runnable heartbeat) {
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }
        if (identifier == null || world == null) {
            return true;
        }

        StoredSectionPresenceIndex index = this.presenceIndexes.computeIfAbsent(
                identifier, ignored -> new StoredSectionPresenceIndex());
        StoredSectionPresenceIndex.BuildState state = index.getBuildState();
        if (state == StoredSectionPresenceIndex.BuildState.READY
                || state == StoredSectionPresenceIndex.BuildState.FAILED) {
            return true;
        }
        if (!index.tryBeginBuild()) {
            return index.getBuildState() != StoredSectionPresenceIndex.BuildState.BUILDING;
        }

        long startedAt = System.nanoTime();
        long[] sectionCount = new long[1];
        boolean acquired = false;
        try {
            heartbeat.run();
            world.acquireRef();
            acquired = true;
            world.storage.iteratePositions(0, key -> {
                if (Thread.currentThread().isInterrupted()) {
                    throw new CancellationException("stored section presence build interrupted");
                }
                index.add(key);
                sectionCount[0]++;
                if ((sectionCount[0] & 4095L) == 0L) {
                    heartbeat.run();
                }
            });
            heartbeat.run();
            index.completeBuild();
            Voxyserver.LOGGER.info("stored section presence ready for {} with {} sections in {}ms",
                    identifier.getLongHash(), sectionCount[0],
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
        } catch (Exception e) {
            index.failBuild();
            Voxyserver.LOGGER.warn("failed to build stored section presence for {}, using bounded fallback scans",
                    identifier.getLongHash(), e);
        } finally {
            if (acquired) {
                world.releaseRef();
            }
        }
        return !Thread.currentThread().isInterrupted();
    }

    boolean mayHaveStoredSection(WorldIdentifier identifier, long sectionKey) {
        StoredSectionPresenceIndex index = this.presenceIndexes.get(identifier);
        return index == null || index.mayContain(sectionKey);
    }

    boolean enqueueIngest(WorldEngine world, LevelChunk chunk) {
        synchronized (this.ingestReferenceLock) {
            if (!this.ingestReferencedWorlds.contains(world)) {
                world.acquireRef();
                this.ingestReferencedWorlds.add(world);
            }
            return this.getIngestService().enqueueIngest(world, chunk);
        }
    }

    void releaseIdleIngestReferences() {
        synchronized (this.ingestReferenceLock) {
            if (this.getIngestService().getTaskCount() == 0) {
                this.releaseIngestReferencesLocked();
            }
        }
    }

    void releaseIngestReferences() {
        synchronized (this.ingestReferenceLock) {
            this.releaseIngestReferencesLocked();
        }
    }

    private void releaseIngestReferencesLocked() {
        for (WorldEngine world : this.ingestReferencedWorlds) {
            if (world.isLive()) {
                world.releaseRef();
            }
        }
        this.ingestReferencedWorlds.clear();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    protected SectionStorage createStorage(WorldIdentifier identifier) {
        if (com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE != null) {
            com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE.markEngineAction();
        }
        var ctx = new ConfigBuildCtx();
        ctx.setProperty(ConfigBuildCtx.BASE_SAVE_PATH, this.basePath.toString());
        ctx.setProperty(ConfigBuildCtx.WORLD_IDENTIFIER, identifier.getWorldId());
        ctx.pushPath(ConfigBuildCtx.DEFAULT_STORAGE_PATH);
        return this.storageConfig.build(ctx);
    }

    private void attachDirtyCallback(WorldIdentifier identifier, WorldEngine world,
                                     StoredSectionPresenceIndex presenceIndex) {
        if (world == null) {
            return;
        }

        Identifier dimension = this.dimensionsByWorld.get(identifier);
        if (dimension == null) {
            return;
        }

        world.setDirtyCallback((section, updateFlags, neighborMsk) -> {
            if (section.lvl != 0) {
                return;
            }
            presenceIndex.add(section.key);
            DirtySectionListener listener = this.dirtySectionListener;
            if (listener != null) {
                listener.onSectionDirty(dimension, section.key);
            }
        });
    }
}

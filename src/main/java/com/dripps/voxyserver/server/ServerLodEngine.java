package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import me.cortex.voxy.common.StorageConfigUtil;
import me.cortex.voxy.common.config.ConfigBuildCtx;
import me.cortex.voxy.common.config.section.SectionStorage;
import me.cortex.voxy.common.config.section.SectionSerializationStorage;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.commonImpl.VoxyInstance;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.chunk.LevelChunk;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class ServerLodEngine extends VoxyInstance {
    @FunctionalInterface
    public interface DirtySectionListener {
        void onSectionDirty(ResourceLocation dimension, long sectionKey);
    }

    private final Path basePath;
    private final SectionSerializationStorage.Config storageConfig;
    private final ConcurrentHashMap<WorldIdentifier, ResourceLocation> dimensionsByWorld = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<WorldIdentifier, StoredSectionPresenceIndex> presenceIndexes = new ConcurrentHashMap<>();
    private final ExecutorService presenceIndexExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "VoxyServer Presence Index");
        t.setDaemon(true);
        return t;
    });
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
        return this.getOrCreate(worldId, level.dimension().location());
    }

    public WorldEngine getOrCreate(WorldIdentifier identifier, ResourceLocation dimension) {
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
        this.attachDirtyCallback(identifier, world);
        this.ensurePresenceIndex(identifier, world);
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
        this.attachDirtyCallback(identifier, world);
        this.ensurePresenceIndex(identifier, world);
        return world;
    }

    public boolean mayHaveStoredSection(WorldIdentifier identifier, WorldEngine world, long sectionKey) {
        if (identifier == null || world == null || WorldEngine.getLevel(sectionKey) != 0) {
            return true;
        }
        return this.ensurePresenceIndex(identifier, world).mayContain(sectionKey);
    }

    public void markChunkPossiblyPresent(ServerLevel level, LevelChunk chunk) {
        WorldIdentifier identifier = WorldIdentifier.of(level);
        if (identifier == null) {
            return;
        }

        StoredSectionPresenceIndex index = this.presenceIndexes.get(identifier);
        if (index == null) {
            return;
        }

        int worldSecX = chunk.getPos().x >> 1;
        int worldSecZ = chunk.getPos().z >> 1;
        int chunkSectionY = chunk.getMinSection() - 1;
        int lastWorldSecY = Integer.MIN_VALUE;
        for (var ignored : chunk.getSections()) {
            chunkSectionY++;
            int worldSecY = chunkSectionY >> 1;
            if (worldSecY == lastWorldSecY) {
                continue;
            }
            lastWorldSecY = worldSecY;
            index.add(WorldEngine.getWorldSectionId(0, worldSecX, worldSecY, worldSecZ));
        }
    }

    public void invalidatePresenceIndex(ServerLevel level) {
        WorldIdentifier identifier = WorldIdentifier.of(level);
        if (identifier == null) {
            return;
        }

        WorldEngine world = this.getOrCreate(level);
        if (world == null) {
            return;
        }

        StoredSectionPresenceIndex index = new StoredSectionPresenceIndex();
        this.presenceIndexes.put(identifier, index);
        this.schedulePresenceIndexBuild(identifier, world, index);
    }

    @Override
    public void shutdown() {
        this.presenceIndexExecutor.shutdownNow();
        try {
            this.presenceIndexExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
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

    private void attachDirtyCallback(WorldIdentifier identifier, WorldEngine world) {
        if (world == null) {
            return;
        }

        ResourceLocation dimension = this.dimensionsByWorld.get(identifier);
        DirtySectionListener listener = this.dirtySectionListener;
        if (dimension == null || listener == null) {
            return;
        }

        world.setDirtyCallback((section, updateFlags, neighborMsk) -> {
            if (section.lvl != 0) {
                return;
            }
            listener.onSectionDirty(dimension, section.key);
        });
    }

    private StoredSectionPresenceIndex ensurePresenceIndex(WorldIdentifier identifier, WorldEngine world) {
        StoredSectionPresenceIndex index = this.presenceIndexes.computeIfAbsent(identifier, ignored -> new StoredSectionPresenceIndex());
        if (!index.isReady()) {
            this.schedulePresenceIndexBuild(identifier, world, index);
        }
        return index;
    }

    private void schedulePresenceIndexBuild(WorldIdentifier identifier, WorldEngine world, StoredSectionPresenceIndex index) {
        if (world == null || !index.tryScheduleBuild()) {
            return;
        }

        var filter = index.createBuildFilter();
        world.acquireRef();
        try {
            this.presenceIndexExecutor.execute(() -> {
                try {
                    world.storage.iteratePositions(0, key -> index.addTo(filter, key));
                    index.completeBuild(filter);
                } catch (Exception e) {
                    Voxyserver.LOGGER.warn("failed to build presence index for {}", identifier.getLongHash(), e);
                    index.failBuild();
                } finally {
                    world.releaseRef();
                }
            });
        } catch (RejectedExecutionException e) {
            world.releaseRef();
            index.failBuild();
        }
    }
}

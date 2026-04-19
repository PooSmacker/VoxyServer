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

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class ServerLodEngine extends VoxyInstance {
    @FunctionalInterface
    public interface DirtySectionListener {
        void onSectionDirty(Identifier dimension, long sectionKey);
    }

    private final Path basePath;
    private final SectionSerializationStorage.Config storageConfig;
    private final ConcurrentHashMap<WorldIdentifier, Identifier> dimensionsByWorld = new ConcurrentHashMap<>();
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
        this.attachDirtyCallback(identifier, world);
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
        return world;
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

    private void attachDirtyCallback(WorldIdentifier identifier, WorldEngine world) {
        if (world == null) {
            return;
        }

        Identifier dimension = this.dimensionsByWorld.get(identifier);
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
}

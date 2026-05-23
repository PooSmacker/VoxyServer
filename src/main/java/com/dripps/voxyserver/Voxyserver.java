package com.dripps.voxyserver;

import com.dripps.voxyserver.config.VoxyServerConfig;
import com.dripps.voxyserver.network.VoxyServerNetworking;
import com.dripps.voxyserver.server.ChunkVoxelizer;
import com.dripps.voxyserver.server.DirtyTracker;
import com.dripps.voxyserver.server.LodStreamingService;
import com.dripps.voxyserver.server.ServerLodEngine;
import com.dripps.voxyserver.server.VoxyServerCommands;
import com.dripps.voxyserver.server.WorldImportCoordinator;
import com.dripps.voxyserver.util.VoxyUpdateChecker;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.fabricmc.fabric.api.entity.event.v1.ServerEntityLevelChangeEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;

public class Voxyserver implements ModInitializer {
    public static final String MOD_ID = "voxyserver";
    public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

    private static final int CONFIG_CHECK_INTERVAL = 20;

    private static VoxyServerConfig config;
    private ServerLodEngine lodEngine;
    private ChunkVoxelizer chunkVoxelizer;
    private LodStreamingService streamingService;
    private WorldImportCoordinator importCoordinator;
    private DirtyTracker dirtyTracker;
    private long lastConfigModified;
    private int configCheckCounter;

    public static VoxyServerConfig getConfig() {
        return config;
    }

    @Override
    public void onInitialize() {
        config = VoxyServerConfig.load();
        LOGGER.info("VoxyServer initialized");
        VoxyServerNetworking.register();
        VoxyUpdateChecker.checkForUpdates();
        CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) ->
                VoxyServerCommands.register(dispatcher, () -> importCoordinator));
        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            if (!server.isDedicatedServer()) {
                LOGGER.info("VoxyServer disabled in singleplayer.");
                return;
            }

            var worldPath = server.getWorldPath(LevelResource.ROOT);
            lodEngine = new ServerLodEngine(worldPath);
            lodEngine.updateDedicatedThreadsCount(config.workerThreads);
            streamingService = new LodStreamingService(lodEngine, config);
            streamingService.register();
            chunkVoxelizer = new ChunkVoxelizer(lodEngine, streamingService, config);
            chunkVoxelizer.register();
            importCoordinator = new WorldImportCoordinator(lodEngine, streamingService);
            if (config.dirtyTrackingEnabled) {
                dirtyTracker = new DirtyTracker(chunkVoxelizer, streamingService, config.dirtyTrackingInterval);
                DirtyTracker.INSTANCE = dirtyTracker;
                ServerTickEvents.END_SERVER_TICK.register(dirtyTracker::tick);
            }
            if (config.debugTrackingEnabled) {
                com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE = new com.dripps.voxyserver.util.ServerStatsTracker(config.debugTrackingInterval);
                ServerTickEvents.END_SERVER_TICK.register(com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE::tick);
            }
            try {
                lastConfigModified = Files.getLastModifiedTime(VoxyServerConfig.getConfigPath()).toMillis();
            } catch (IOException ignored) {}

            ServerTickEvents.END_SERVER_TICK.register(s -> {
                if (lodEngine == null) return;
                if (++configCheckCounter < CONFIG_CHECK_INTERVAL) return;
                configCheckCounter = 0;
                checkConfigReload();
            });

            LOGGER.info("VoxyServer engine started for world: {}", worldPath);
        });

        ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
            if (lodEngine != null) {
                LOGGER.info("shutting down VoxyServer engine");
                DirtyTracker.INSTANCE = null;
                dirtyTracker = null;
                if (importCoordinator != null) importCoordinator.shutdown();
                if (streamingService != null) streamingService.shutdown();
                lodEngine.shutdown();
                lodEngine = null;
                chunkVoxelizer = null;
                streamingService = null;
                importCoordinator = null;
            }
        });

        // handle dimension changes, clear players lod cache for old dimensions
        ServerEntityLevelChangeEvents.AFTER_PLAYER_CHANGE_LEVEL.register((player, origin, destination) -> {
            if (streamingService != null) {
                streamingService.onDimensionChange(player, destination);
            }
        });
    }

    private void checkConfigReload() {
        try {
            var path = VoxyServerConfig.getConfigPath();
            if (!Files.exists(path)) return;
            long currentModified = Files.getLastModifiedTime(path).toMillis();
            if (currentModified == lastConfigModified) return;
            lastConfigModified = currentModified;

            VoxyServerConfig newConfig = VoxyServerConfig.reload();
            if (newConfig == null) return;

            applyConfigChanges(config, newConfig);
            config = newConfig;
            LOGGER.info("config reloaded successfully");
        } catch (IOException ignored) {}
    }

    private void applyConfigChanges(VoxyServerConfig oldCfg, VoxyServerConfig newCfg) {
        if (oldCfg.generateOnChunkLoad != newCfg.generateOnChunkLoad)
            LOGGER.info("generateonchunkload changed, requires server restart to take effect");
        if (oldCfg.dirtyTrackingEnabled != newCfg.dirtyTrackingEnabled)
            LOGGER.info("dirtytrackingenabled changed, requires server restart to take effect");
        if (oldCfg.debugTrackingEnabled != newCfg.debugTrackingEnabled)
            LOGGER.info("debugtrackingenabled changed, requires server restart to take effect");

        if (streamingService != null) {
            streamingService.updateConfig(
                    newCfg.lodStreamRadius,
                    newCfg.maxSectionsPerTickPerPlayer,
                    newCfg.sectionsPerPacket,
                    newCfg.tickInterval,
                    newCfg.dirtyTrackingInterval
            );
        }

        if (dirtyTracker != null) {
            dirtyTracker.updateFlushInterval(newCfg.dirtyTrackingInterval);
        }

        if (newCfg.workerThreads != oldCfg.workerThreads && lodEngine != null) {
            lodEngine.updateDedicatedThreadsCount(newCfg.workerThreads);
        }

        if (com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE != null) {
            com.dripps.voxyserver.util.ServerStatsTracker.INSTANCE.updateTickInterval(newCfg.debugTrackingInterval);
        }
    }
}

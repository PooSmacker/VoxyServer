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

public class Voxyserver implements ModInitializer {
    public static final String MOD_ID = "voxyserver";
    public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

    private static VoxyServerConfig config;
    private ServerLodEngine lodEngine;
    private ChunkVoxelizer chunkVoxelizer;
    private LodStreamingService streamingService;
    private WorldImportCoordinator importCoordinator;
    private DirtyTracker dirtyTracker;

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
            chunkVoxelizer = new ChunkVoxelizer(lodEngine, config);
            chunkVoxelizer.register();
            streamingService = new LodStreamingService(lodEngine, config);
            streamingService.register();
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

        // thius handles dimension changes, clear players lod cache for old dimension
        ServerEntityLevelChangeEvents.AFTER_PLAYER_CHANGE_LEVEL.register((player, origin, destination) -> {
            if (streamingService != null) {
                streamingService.onDimensionChange(player, destination);
            }
        });
    }
}

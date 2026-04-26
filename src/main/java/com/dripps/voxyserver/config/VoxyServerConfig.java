package com.dripps.voxyserver.config;

import com.dripps.voxyserver.Voxyserver;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.fabricmc.loader.api.FabricLoader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class VoxyServerConfig {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final String FILE_NAME = "voxyserver.json";

    public int lodStreamRadius = 256;
    public int maxSectionsPerTickPerPlayer = 100;
    public int sectionsPerPacket = 50;
    public boolean generateOnChunkLoad = true;
    public int tickInterval = 5;
    public int workerThreads = 3;
    public boolean dirtyTrackingEnabled = true;
    public int dirtyTrackingInterval = 40;
    public boolean debugTrackingEnabled = false;
    public int debugTrackingInterval = 200;
    public int playerCacheExpirationDays = 14;

    public static VoxyServerConfig load() {
        Path configPath = FabricLoader.getInstance().getConfigDir().resolve(FILE_NAME);
        if (Files.exists(configPath)) {
            try {
                String json = Files.readString(configPath);
                VoxyServerConfig config = GSON.fromJson(json, VoxyServerConfig.class);
                if (config != null) {
                    config.save();
                    return config;
                }
            } catch (Exception e) {
                Voxyserver.LOGGER.warn("failed to load config, using defaults", e);
            }
        }
        VoxyServerConfig config = new VoxyServerConfig();
        config.save();
        return config;
    }

    public void save() {
        Path configPath = FabricLoader.getInstance().getConfigDir().resolve(FILE_NAME);
        try {
            Files.createDirectories(configPath.getParent());
            Files.writeString(configPath, GSON.toJson(this));
        } catch (IOException e) {
            Voxyserver.LOGGER.warn("failed to save config", e);
        }
    }
}

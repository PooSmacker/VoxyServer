package com.dripps.voxyserver.config;

import com.dripps.voxyserver.Voxyserver;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.fabricmc.loader.api.FabricLoader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
    public boolean hashSyncEnabled = true;

    public static Path getConfigPath() {
        return FabricLoader.getInstance().getConfigDir().resolve(FILE_NAME);
    }

    public List<String> validate() {
        List<String> errors = new ArrayList<>();
        if (lodStreamRadius < 1) errors.add("lodstreamradius must be at least 1, got " + lodStreamRadius);
        if (maxSectionsPerTickPerPlayer < 1) errors.add("maxsectionspertickperplayer must be at least 1, got " + maxSectionsPerTickPerPlayer);
        if (sectionsPerPacket < 1) errors.add("sectionsperpacket must be at least 1, got " + sectionsPerPacket);
        if (tickInterval < 1) errors.add("tickinterval must be at least 1, got " + tickInterval);
        if (workerThreads < 1 || workerThreads > 32) errors.add("workerthreads must be between 1 and 32, got " + workerThreads);
        if (dirtyTrackingInterval < 1) errors.add("dirtytrackinginterval must be at least 1, got " + dirtyTrackingInterval);
        if (debugTrackingInterval < 1) errors.add("debugtrackinginterval must be at least 1, got " + debugTrackingInterval);
        return errors;
    }

    public static VoxyServerConfig reload() {
        Path path = getConfigPath();
        if (!Files.exists(path)) return null;
        try {
            String json = Files.readString(path);
            VoxyServerConfig candidate = GSON.fromJson(json, VoxyServerConfig.class);
            if (candidate == null) {
                Voxyserver.LOGGER.warn("config reload failed bc file parsed as null");
                return null;
            }
            List<String> errors = candidate.validate();
            if (!errors.isEmpty()) {
                for (String err : errors) {
                    Voxyserver.LOGGER.warn("config reload rejected: {}", err);
                }
                return null;
            }
            return candidate;
        } catch (Exception e) {
            Voxyserver.LOGGER.warn("config reload failed: {}", e.getMessage());
            return null;
        }
    }

    public static VoxyServerConfig load() {
        Path configPath = getConfigPath();
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

    public VoxyServerConfig copy() {
        return GSON.fromJson(GSON.toJson(this), VoxyServerConfig.class);
    }

    public void save() {
        Path configPath = getConfigPath();
        try {
            Files.createDirectories(configPath.getParent());
            Files.writeString(configPath, GSON.toJson(this));
        } catch (IOException e) {
            Voxyserver.LOGGER.warn("failed to save config", e);
        }
    }
}

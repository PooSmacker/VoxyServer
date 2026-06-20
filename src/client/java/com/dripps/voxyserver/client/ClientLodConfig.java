package com.dripps.voxyserver.client;

import com.dripps.voxyserver.Voxyserver;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.fabricmc.loader.api.FabricLoader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class ClientLodConfig {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final String FILE_NAME = "voxyserver-client.json";

    public Preferences defaults = new Preferences();
    public Map<String, Preferences> serverProfiles = new LinkedHashMap<>();
    // global client display settings (Boolean so a config missing the field defaults via sanitize)
    public Boolean showDownloadHud = Boolean.TRUE;
    public Boolean hudTopLeft = Boolean.FALSE;

    public static ClientLodConfig load() {
        Path configPath = FabricLoader.getInstance().getConfigDir().resolve(FILE_NAME);
        if (Files.exists(configPath)) {
            try {
                String json = Files.readString(configPath);
                ClientLodConfig config = GSON.fromJson(json, ClientLodConfig.class);
                if (config != null) {
                    config.sanitize();
                    config.save();
                    return config;
                }
            } catch (Exception e) {
                Voxyserver.LOGGER.warn("using defaults..", e);
            }
        }

        ClientLodConfig config = new ClientLodConfig();
        config.save();
        return config;
    }

    public Preferences getPreferencesForServer(String serverKey) {
        if (serverKey == null) {
            return defaults.copy();
        }

        Preferences profile = serverProfiles.get(normalizeServerKey(serverKey));
        if (profile == null) {
            return defaults.copy();
        }

        return profile.copy();
    }

    public void savePreferencesForServer(String serverKey, Preferences preferences) {
        String normalizedKey = normalizeServerKey(serverKey);
        if (normalizedKey == null || preferences == null) {
            return;
        }

        Preferences stored = preferences.copy();
        stored.sanitize();
        serverProfiles.put(normalizedKey, stored);
        save();
    }

    public void save() {
        Path configPath = FabricLoader.getInstance().getConfigDir().resolve(FILE_NAME);
        try {
            Files.createDirectories(configPath.getParent());
            Files.writeString(configPath, GSON.toJson(this));
        } catch (IOException e) {
            Voxyserver.LOGGER.warn("coukltnt save client config", e);
        }
    }

    private void sanitize() {
        if (showDownloadHud == null) {
            showDownloadHud = Boolean.TRUE;
        }
        if (hudTopLeft == null) {
            hudTopLeft = Boolean.FALSE;
        }
        if (defaults == null) {
            defaults = new Preferences();
        } else {
            defaults.sanitize();
        }

        if (serverProfiles == null) {
            serverProfiles = new LinkedHashMap<>();
            return;
        }

        Map<String, Preferences> sanitizedProfiles = new LinkedHashMap<>();
        for (Map.Entry<String, Preferences> entry : serverProfiles.entrySet()) {
            String normalizedKey = normalizeServerKey(entry.getKey());
            if (normalizedKey == null) {
                continue;
            }

            Preferences preferences = entry.getValue();
            if (preferences == null) {
                preferences = new Preferences();
            } else {
                preferences.sanitize();
            }

            sanitizedProfiles.put(normalizedKey, preferences);
        }

        serverProfiles = sanitizedProfiles;
    }

    private static String normalizeServerKey(String serverKey) {
        if (serverKey == null) {
            return null;
        }

        String normalized = serverKey.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return null;
        }

        return normalized;
    }

    public static class Preferences {
        public boolean enabled = true;
        public int preferredRadius = 0;
        public int preferredMaxSections = 0;

        public Preferences copy() {
            Preferences copy = new Preferences();
            copy.enabled = enabled;
            copy.preferredRadius = preferredRadius;
            copy.preferredMaxSections = preferredMaxSections;
            return copy;
        }

        public void sanitize() {
            if (preferredRadius < 0) {
                preferredRadius = 0;
            }
            if (preferredMaxSections < 0) {
                preferredMaxSections = 0;
            }
        }
    }
}
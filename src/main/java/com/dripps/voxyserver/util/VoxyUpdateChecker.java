package com.dripps.voxyserver.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.fabricmc.api.EnvType;
import net.fabricmc.loader.api.FabricLoader;
import net.fabricmc.loader.api.ModContainer;
import net.minecraft.ChatFormatting;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.MutableComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class VoxyUpdateChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger("VoxyServer-Update");
    private static final String UPDATE_URL = "https://gist.githubusercontent.com/PooSmacker/a4205e4b4d58a9b61054c9978c041408/raw/update.json";

    public record Notice(boolean updateAvailable, boolean breaking, String latest, String current,
                         List<String> breakingReasons, boolean voxyUnsupported,
                         List<String> supportedVoxy, String currentVoxy) {}

    private static volatile Notice pendingNotice;

    public static Notice getPendingNotice() {
        return pendingNotice;
    }

    public static void checkForUpdates() {
        if (FabricLoader.getInstance().getEnvironmentType() != EnvType.SERVER) {
            return;
        }
        CompletableFuture.runAsync(() -> {
            try {
                HttpClient client = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(UPDATE_URL))
                        .header("User-Agent", "VoxyServer")
                        .GET()
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    return;
                }

                JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
                String latestServerVer = json.get("latest_voxyserver").getAsString();

                List<String> supportedVoxyVersions = new ArrayList<>();
                if (json.has("supported_voxy") && json.get("supported_voxy").isJsonArray()) {
                    for (JsonElement element : json.getAsJsonArray("supported_voxy")) {
                        supportedVoxyVersions.add(element.getAsString());
                    }
                }

                String currentServerVer = getModVersion("voxyserver");
                String currentVoxyVer = getModVersion("voxy");

                boolean serverNeedsUpdate = !currentServerVer.equals("Unknown") && !currentServerVer.equals(latestServerVer);
                boolean voxyInstalled = !currentVoxyVer.equals("Unknown");
                boolean voxyUnsupported = voxyInstalled && !supportedVoxyVersions.isEmpty() && !supportedVoxyVersions.contains(currentVoxyVer);

                List<String> breakingReasons = new ArrayList<>();
                if (serverNeedsUpdate && json.has("breaking_changes") && json.get("breaking_changes").isJsonArray()) {
                    for (JsonElement element : json.getAsJsonArray("breaking_changes")) {
                        if (!element.isJsonObject()) continue;
                        JsonObject entry = element.getAsJsonObject();
                        if (!entry.has("version")) continue;
                        String version = entry.get("version").getAsString();
                        String reason = entry.has("reason") ? entry.get("reason").getAsString() : "not backwards compatible";
                        if (compareVersions(currentServerVer, version) < 0 && compareVersions(version, latestServerVer) <= 0) {
                            breakingReasons.add(version + ": " + reason);
                        }
                    }
                }
                boolean breaking = !breakingReasons.isEmpty();

                logToConsole(serverNeedsUpdate, breaking, latestServerVer, currentServerVer, breakingReasons,
                        voxyUnsupported, supportedVoxyVersions, currentVoxyVer);

                if (serverNeedsUpdate || voxyUnsupported) {
                    pendingNotice = new Notice(serverNeedsUpdate, breaking, latestServerVer, currentServerVer,
                            breakingReasons, voxyUnsupported, supportedVoxyVersions, currentVoxyVer);
                }
            } catch (Exception e) {
                LOGGER.debug("Could not check for VoxyServer updates.", e);
            }
        });
    }

    private static void logToConsole(boolean serverNeedsUpdate, boolean breaking, String latest, String current,
                                     List<String> breakingReasons, boolean voxyUnsupported,
                                     List<String> supportedVoxy, String currentVoxy) {
        if (serverNeedsUpdate) {
            LOGGER.warn("=====================================================");
            if (breaking) {
                LOGGER.warn("BREAKING UPDATE available for VoxyServer: {} (you run {})", latest, current);
                LOGGER.warn("This update is NOT backwards compatible. Connected clients must update too.");
                for (String reason : breakingReasons) {
                    LOGGER.warn("  - {}", reason);
                }
            } else {
                LOGGER.warn("A new version of VoxyServer is available: {}", latest);
                LOGGER.warn("You are currently running: {}", current);
            }
            LOGGER.warn("Please check modrinth for the latest version");
            if (voxyUnsupported) {
                LOGGER.warn("IMPORTANT: supported Voxy versions: {}", String.join(", ", supportedVoxy));
                LOGGER.warn("Your current Voxy version is: {}", currentVoxy);
            }
            LOGGER.warn("=====================================================");
        } else if (voxyUnsupported) {
            LOGGER.warn("=====================================================");
            LOGGER.warn("Your Voxy version ({}) is not in the supported list for VoxyServer {}.", currentVoxy, current);
            LOGGER.warn("Supported Voxy versions: {}", String.join(", ", supportedVoxy));
            LOGGER.warn("=====================================================");
        } else {
            LOGGER.info("VoxyServer is up to date.");
        }
    }

    public static MutableComponent buildNoticeComponent(Notice notice) {
        MutableComponent out = Component.empty();
        out.append(Component.literal("[VoxyServer] ").withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD));

        if (notice.breaking()) {
            out.append(Component.literal("breaking update available").withStyle(ChatFormatting.RED, ChatFormatting.BOLD));
            out.append(Component.literal("\n" + notice.current() + " -> " + notice.latest()
                            + ", not backwards compatible. clients must update too.")
                    .withStyle(ChatFormatting.RED));
            for (String reason : notice.breakingReasons()) {
                out.append(Component.literal("\n  " + reason).withStyle(ChatFormatting.GRAY));
            }
        } else if (notice.updateAvailable()) {
            out.append(Component.literal("update available").withStyle(ChatFormatting.YELLOW, ChatFormatting.BOLD));
            out.append(Component.literal("\n" + notice.current() + " -> " + notice.latest() + " (see modrinth)")
                    .withStyle(ChatFormatting.YELLOW));
        }

        if (notice.voxyUnsupported()) {
            out.append(Component.literal("\nyour Voxy " + notice.currentVoxy() + " is not in the supported list: "
                            + String.join(", ", notice.supportedVoxy()))
                    .withStyle(ChatFormatting.GOLD));
        }
        return out;
    }

    private static int compareVersions(String a, String b) {
        String[] pa = a.split("-")[0].split("\\.");
        String[] pb = b.split("-")[0].split("\\.");
        int n = Math.max(pa.length, pb.length);
        for (int i = 0; i < n; i++) {
            int x = i < pa.length ? parseIntSafe(pa[i]) : 0;
            int y = i < pb.length ? parseIntSafe(pb[i]) : 0;
            if (x != y) return Integer.compare(x, y);
        }
        return 0;
    }

    private static int parseIntSafe(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String getModVersion(String modId) {
        Optional<ModContainer> container = FabricLoader.getInstance().getModContainer(modId);
        if (container.isPresent()) {
            return container.get().getMetadata().getVersion().getFriendlyString();
        }
        return "Unknown";
    }
}

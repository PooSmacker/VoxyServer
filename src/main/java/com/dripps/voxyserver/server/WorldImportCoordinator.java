package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import me.cortex.voxy.commonImpl.importers.IDataImporter;
import me.cortex.voxy.commonImpl.importers.WorldImporter;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.dimension.DimensionType;
import net.minecraft.world.level.storage.LevelResource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class WorldImportCoordinator {
    private final ServerLodEngine engine;
    private final LodStreamingService streamingService;

    private final Object lock = new Object();
    private final ArrayDeque<ImportRequest> queue = new ArrayDeque<>();

    private volatile ActiveImport activeImport;
    private volatile long activeRunId;
    private volatile long nextRunId = 1L;

    public WorldImportCoordinator(ServerLodEngine engine, LodStreamingService streamingService) {
        this.engine = engine;
        this.streamingService = streamingService;
    }

    public String getStatusSummary() {
        ActiveImport active = this.activeImport;
        synchronized (this.lock) {
            int queued = this.queue.size();
            if (active == null) {
                if (queued == 0) {
                    return "no import is running";
                }
                return "queued " + queued + " dimension(s)";
            }

            int done = active.processedChunks.get();
            int total = Math.max(done, active.estimatedChunks.get());
            long elapsedMs = Math.max(1L, System.currentTimeMillis() - active.startedAtMs);

            String base = "running "
                    + active.dimensionId
                    + " "
                    + done
                    + "/"
                    + total
                    + " chunks"
                    + " in "
                    + formatDuration(elapsedMs);

            if (queued > 0) {
                base += ", " + queued + " queued";
            }
            return base;
        }
    }

    public boolean startAll(CommandSourceStack source) {
        MinecraftServer server = source.getServer();
        List<ImportRequest> requests = new ArrayList<>();

        for (ServerLevel level : server.getAllLevels()) {
            Path regionPath = getRegionPath(server, level);
            if (!Files.isDirectory(regionPath)) {
                continue;
            }
            requests.add(new ImportRequest(server, level, regionPath, source));
        }

        if (requests.isEmpty()) {
            source.sendFailure(Component.literal("no importable regoion folders found"));
            return false;
        }

        String dimensions = requests.stream()
                .map(request -> request.dimensionId)
                .toList()
                .toString();

        long runId;
        synchronized (this.lock) {
            if (this.activeImport != null) {
                source.sendFailure(Component.literal("an import is already running"));
                return false;
            }
            runId = this.nextRunId++;
            this.activeRunId = runId;
            this.queue.clear();
            this.queue.addAll(requests);
        }

        source.sendSuccess(() -> Component.literal("queued import for " + requests.size() + " dimension(s): " + dimensions), true);
        this.startNext(runId);
        return true;
    }

    public boolean startCurrent(CommandSourceStack source) {
        if (!(source.getEntity() instanceof net.minecraft.server.level.ServerPlayer player)) {
            source.sendFailure(Component.literal("current can only be used by a player"));
            return false;
        }
        return this.startDimension(source, player.level());
    }

    public boolean startDimension(CommandSourceStack source, ServerLevel level) {
        MinecraftServer server = source.getServer();
        Path regionPath = getRegionPath(server, level);
        if (!Files.isDirectory(regionPath)) {
            source.sendFailure(Component.literal("no region folder found for " + level.dimension().identifier()));
            return false;
        }

        long runId;
        synchronized (this.lock) {
            if (this.activeImport != null) {
                source.sendFailure(Component.literal("an import is already running"));
                return false;
            }
            runId = this.nextRunId++;
            this.activeRunId = runId;
            this.queue.clear();
            this.queue.addLast(new ImportRequest(server, level, regionPath, source));
        }

        source.sendSuccess(() -> Component.literal("queued import for " + level.dimension().identifier()), true);
        this.startNext(runId);
        return true;
    }

    public boolean cancel(CommandSourceStack source) {
        ActiveImport active;
        int removed;
        synchronized (this.lock) {
            active = this.activeImport;
            removed = this.queue.size();
            this.queue.clear();
            this.activeRunId = 0L;
        }

        if (active == null) {
            if (removed == 0) {
                source.sendFailure(Component.literal("no import is running"));
                return false;
            }
            source.sendSuccess(() -> Component.literal("cleared queued imports"), true);
            return true;
        }

        active.cancelled = true;
        active.importer.shutdown();
        source.sendSuccess(() -> Component.literal("cancelled import for " + active.dimensionId), true);
        return true;
    }

    public void shutdown() {
        ActiveImport active;
        synchronized (this.lock) {
            this.queue.clear();
            this.activeRunId = 0L;
            active = this.activeImport;
        }
        if (active != null) {
            active.cancelled = true;
            active.importer.shutdown();
        }
    }

    private void startNext(long runId) {
        ImportRequest request;
        synchronized (this.lock) {
            if (runId != this.activeRunId || this.activeImport != null) {
                return;
            }
            request = this.queue.pollFirst();
            if (request == null) {
                this.activeRunId = 0L;
                return;
            }
        }

        WorldIdentifier worldId = WorldIdentifier.of(request.level);
        if (worldId == null) {
            request.server.execute(() -> {
                sendFailure(request.source, "could not make up one's mind voxy world for " + request.dimensionId);
                this.onImportFinished(runId, null, request, false, 0);
            });
            return;
        }

        var world = this.engine.getOrCreate(worldId, request.level.dimension().identifier());
        if (world == null) {
            request.server.execute(() -> {
                sendFailure(request.source, "could not create voxy world for " + request.dimensionId);
                this.onImportFinished(runId, null, request, false, 0);
            });
            return;
        }

        WorldImporter importer = new WorldImporter(
                world,
                request.level,
                this.engine.getServiceManager(),
                this.engine.savingServiceRateLimiter
        );
        importer.importRegionDirectoryAsync(request.regionPath.toFile());

        ActiveImport active = new ActiveImport(request, importer);
        synchronized (this.lock) {
            if (runId != this.activeRunId) {
                importer.shutdown();
                return;
            }
            this.activeImport = active;
        }

        request.server.execute(() -> {
            sendSuccess(request.source, "starting import for " + request.dimensionId);
            Voxyserver.LOGGER.info("starting voxy import for {}", request.dimensionId);
        });

        importer.runImport(
                (finished, outOf) -> {
                    active.processedChunks.set(finished);
                    active.estimatedChunks.set(outOf);

                    long now = System.currentTimeMillis();
                    if (now - active.lastUpdateMs < 1000L) {
                        return;
                    }
                    active.lastUpdateMs = now;

                    request.server.execute(() -> {
                        String msg = "importing "
                                + active.dimensionId
                                + " "
                                + finished
                                + "/"
                                + Math.max(finished, outOf)
                                + " chunks";
                        sendSuccess(request.source, msg);
                        Voxyserver.LOGGER.info(msg);
                    });
                },
                total -> request.server.execute(() -> this.onImportFinished(runId, active, request, true, total))
        );
    }

    private void onImportFinished(long runId, ActiveImport active, ImportRequest request, boolean completed, int totalChunks) {
        boolean cancelled = active != null && active.cancelled;

        synchronized (this.lock) {
            if (this.activeImport != null && Objects.equals(this.activeImport, active)) {
                this.activeImport = null;
            }
        }

        if (completed && !cancelled) {
            this.engine.invalidatePresenceIndex(request.level);
            this.streamingService.clearDimensionForReadyPlayers(request.level);
            String msg = "finished import for "
                    + request.dimensionId
                    + " with "
                    + totalChunks
                    + " chunks";
            sendSuccess(request.source, msg);
            Voxyserver.LOGGER.info(msg);
            this.startNext(runId);
            return;
        }

        if (cancelled) {
            String msg = "import cancelled for " + request.dimensionId;
            sendSuccess(request.source, msg);
            Voxyserver.LOGGER.info(msg);
        } else {
            String msg = "import ended early for " + request.dimensionId;
            sendFailure(request.source, msg);
            Voxyserver.LOGGER.warn(msg);
        }

        synchronized (this.lock) {
            this.queue.clear();
            if (this.activeRunId == runId) {
                this.activeRunId = 0L;
            }
        }
    }

    private static Path getRegionPath(MinecraftServer server, ServerLevel level) {
        return DimensionType.getStorageFolder(level.dimension(), server.getWorldPath(LevelResource.ROOT))
                .resolve("region");
    }

    private static void sendSuccess(CommandSourceStack source, String message) {
        source.sendSuccess(() -> Component.literal(message), true);
    }

    private static void sendFailure(CommandSourceStack source, String message) {
        source.sendFailure(Component.literal(message));
    }

    private static String formatDuration(long elapsedMs) {
        long seconds = elapsedMs / 1000L;
        long minutes = seconds / 60L;
        long remSeconds = seconds % 60L;
        if (minutes > 0) {
            return minutes + "m " + remSeconds + "s";
        }
        return Math.max(1L, seconds) + "s";
    }

    private static final class ImportRequest {
        private final MinecraftServer server;
        private final ServerLevel level;
        private final Path regionPath;
        private final CommandSourceStack source;
        private final String dimensionId;

        private ImportRequest(MinecraftServer server, ServerLevel level, Path regionPath, CommandSourceStack source) {
            this.server = server;
            this.level = level;
            this.regionPath = regionPath;
            this.source = source;
            this.dimensionId = level.dimension().identifier().toString().toLowerCase(Locale.ROOT);
        }
    }

    private static final class ActiveImport {
        private final WorldImporter importer;
        private final String dimensionId;
        private final long startedAtMs;
        private final AtomicInteger processedChunks = new AtomicInteger();
        private final AtomicInteger estimatedChunks = new AtomicInteger();
        private volatile long lastUpdateMs;
        private volatile boolean cancelled;

        private ActiveImport(ImportRequest request, WorldImporter importer) {
            this.importer = importer;
            this.dimensionId = request.dimensionId;
            this.startedAtMs = System.currentTimeMillis();
            this.lastUpdateMs = this.startedAtMs;
        }
    }
}

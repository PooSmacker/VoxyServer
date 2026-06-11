package com.dripps.voxyserver.server;

import com.dripps.voxyserver.Voxyserver;
import com.dripps.voxyserver.config.VoxyServerConfig;
import com.dripps.voxyserver.util.ServerStatsTracker;
import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.arguments.StringArgumentType;
import com.mojang.brigadier.suggestion.SuggestionProvider;
import net.minecraft.ChatFormatting;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.SharedSuggestionProvider;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.MutableComponent;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.permissions.Permissions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public final class VoxyServerCommands {
    private VoxyServerCommands() {
    }

    public static void register(CommandDispatcher<CommandSourceStack> dispatcher, Voxyserver mod) {
        Supplier<WorldImportCoordinator> coordinatorSupplier = mod::getImportCoordinator;
        dispatcher.register(
                Commands.literal("voxyserver")
                        .requires(source -> source.permissions().hasPermission(Permissions.COMMANDS_ADMIN))
                        .then(Commands.literal("import")
                                .then(Commands.literal("existing")
                                        .then(Commands.literal("all")
                                                .executes(context -> executeAll(context.getSource(), coordinatorSupplier)))
                                        .then(Commands.literal("current")
                                                .executes(context -> executeCurrent(context.getSource(), coordinatorSupplier)))
                                        .then(Commands.literal("dimension")
                                                .then(Commands.argument("dimension", StringArgumentType.greedyString())
                                                        .suggests((context, builder) -> {
                                                            java.util.List<String> dimensions = new java.util.ArrayList<>();
                                                            for (ServerLevel level : context.getSource().getServer().getAllLevels()) {
                                                                dimensions.add(level.dimension().identifier().toString());
                                                            }
                                                            return SharedSuggestionProvider.suggest(dimensions, builder);
                                                        })
                                                        .executes(context -> executeDimension(
                                                                context.getSource(),
                                                                coordinatorSupplier,
                                                                StringArgumentType.getString(context, "dimension")
                                                        ))))
                                        .then(Commands.literal("status")
                                                .executes(context -> executeStatus(context.getSource(), coordinatorSupplier)))
                                        .then(Commands.literal("cancel")
                                                .executes(context -> executeCancel(context.getSource(), coordinatorSupplier)))))
                        .then(Commands.literal("config")
                                .then(Commands.literal("list")
                                        .executes(context -> configList(context.getSource())))
                                .then(Commands.literal("get")
                                        .then(Commands.argument("key", StringArgumentType.string())
                                                .suggests(KEY_SUGGEST)
                                                .executes(context -> configGet(context.getSource(),
                                                        StringArgumentType.getString(context, "key")))))
                                .then(Commands.literal("set")
                                        .then(Commands.argument("key", StringArgumentType.string())
                                                .suggests(KEY_SUGGEST)
                                                .then(Commands.argument("value", StringArgumentType.greedyString())
                                                        .executes(context -> configSet(context.getSource(), mod,
                                                                StringArgumentType.getString(context, "key"),
                                                                StringArgumentType.getString(context, "value")))))))
                        .then(Commands.literal("stats")
                                .executes(context -> statsOnce(context.getSource(), mod))
                                .then(Commands.literal("continuous")
                                        .executes(context -> statsContinuous(context.getSource(), mod)))
                                .then(Commands.literal("stop")
                                        .executes(context -> statsStop(context.getSource(), mod))))
        );
    }

    private static final SuggestionProvider<CommandSourceStack> KEY_SUGGEST =
            (context, builder) -> SharedSuggestionProvider.suggest(configKeys(), builder);

    static String ballsack = "import coordinator is not ready";

    private static int executeAll(CommandSourceStack source, Supplier<WorldImportCoordinator> coordinatorSupplier) {
        WorldImportCoordinator coordinator = coordinatorSupplier.get();
        if (coordinator == null) {
            source.sendFailure(Component.literal(ballsack));
            return 0;
        }
        return coordinator.startAll(source) ? 1 : 0;
    }

    private static int executeCurrent(CommandSourceStack source, Supplier<WorldImportCoordinator> coordinatorSupplier) {
        WorldImportCoordinator coordinator = coordinatorSupplier.get();
        if (coordinator == null) {
            source.sendFailure(Component.literal(ballsack));
            return 0;
        }
        return coordinator.startCurrent(source) ? 1 : 0;
    }

    private static int executeDimension(CommandSourceStack source, Supplier<WorldImportCoordinator> coordinatorSupplier, String dimensionId) {
        WorldImportCoordinator coordinator = coordinatorSupplier.get();
        if (coordinator == null) {
            source.sendFailure(Component.literal(ballsack));
            return 0;
        }

        ServerLevel level = findLevel(source, dimensionId);
        if (level == null) {
            source.sendFailure(Component.literal("dunno dimension: " + dimensionId));
            return 0;
        }
        return coordinator.startDimension(source, level) ? 1 : 0;
    }

    private static int executeStatus(CommandSourceStack source, Supplier<WorldImportCoordinator> coordinatorSupplier) {
        WorldImportCoordinator coordinator = coordinatorSupplier.get();
        if (coordinator == null) {
            source.sendFailure(Component.literal(ballsack));
            return 0;
        }
        source.sendSuccess(() -> Component.literal(coordinator.getStatusSummary()), false);
        return 1;
    }

    private static int executeCancel(CommandSourceStack source, Supplier<WorldImportCoordinator> coordinatorSupplier) {
        WorldImportCoordinator coordinator = coordinatorSupplier.get();
        if (coordinator == null) {
            source.sendFailure(Component.literal(ballsack));
            return 0;
        }
        return coordinator.cancel(source) ? 1 : 0;
    }

    private static ServerLevel findLevel(CommandSourceStack source, String dimensionId) {
        for (ServerLevel level : source.getServer().getAllLevels()) {
            if (level.dimension().identifier().toString().equals(dimensionId)) {
                return level;
            }
        }
        return null;
    }

    private static List<String> configKeys() {
        List<String> keys = new ArrayList<>();
        for (Field f : VoxyServerConfig.class.getFields()) {
            if (Modifier.isStatic(f.getModifiers())) continue;
            Class<?> t = f.getType();
            if (t == int.class || t == boolean.class) keys.add(f.getName());
        }
        return keys;
    }

    private static Field findConfigField(String key) {
        for (Field f : VoxyServerConfig.class.getFields()) {
            if (Modifier.isStatic(f.getModifiers())) continue;
            if (!f.getName().equals(key)) continue;
            Class<?> t = f.getType();
            if (t == int.class || t == boolean.class) return f;
        }
        return null;
    }

    private static int configList(CommandSourceStack source) {
        VoxyServerConfig cfg = Voxyserver.getConfig();
        if (cfg == null) {
            source.sendFailure(Component.literal("config not loaded"));
            return 0;
        }
        MutableComponent out = block("config");
        try {
            for (String key : configKeys()) {
                out.append(Component.literal("\n"))
                        .append(kv(key, findConfigField(key).get(cfg), ChatFormatting.WHITE));
            }
        } catch (IllegalAccessException e) {
            source.sendFailure(Component.literal("failed to read config"));
            return 0;
        }
        source.sendSuccess(() -> out, false);
        return 1;
    }

    private static int configGet(CommandSourceStack source, String key) {
        VoxyServerConfig cfg = Voxyserver.getConfig();
        Field f = findConfigField(key);
        if (cfg == null || f == null) {
            source.sendFailure(Component.literal("unknown key: " + key + ". valid: " + String.join(", ", configKeys())));
            return 0;
        }
        Object val;
        try {
            val = f.get(cfg);
        } catch (IllegalAccessException e) {
            source.sendFailure(Component.literal("failed to read " + key));
            return 0;
        }
        MutableComponent line = kv(key, val, ChatFormatting.WHITE);
        source.sendSuccess(() -> line, false);
        return 1;
    }

    private static int configSet(CommandSourceStack source, Voxyserver mod, String key, String rawValue) {
        VoxyServerConfig current = Voxyserver.getConfig();
        Field f = findConfigField(key);
        if (current == null || f == null) {
            source.sendFailure(Component.literal("unknown key: " + key + ". valid: " + String.join(", ", configKeys())));
            return 0;
        }

        String value = rawValue.trim();
        Object parsed;
        boolean isInt = f.getType() == int.class;
        try {
            parsed = isInt ? Integer.parseInt(value) : parseBoolStrict(value);
        } catch (IllegalArgumentException e) {
            source.sendFailure(Component.literal(key + " expects " + (isInt ? "an integer" : "true or false") + ", got: " + value));
            return 0;
        }

        VoxyServerConfig candidate = current.copy();
        try {
            f.set(candidate, parsed);
        } catch (IllegalAccessException e) {
            source.sendFailure(Component.literal("failed to set " + key));
            return 0;
        }

        List<String> errors = candidate.validate();
        if (!errors.isEmpty()) {
            source.sendFailure(Component.literal("invalid: " + String.join("; ", errors)));
            return 0;
        }

        List<String> notes = mod.applyConfigFromCommand(candidate);
        MutableComponent ok = Component.literal("set ").withStyle(ChatFormatting.GREEN)
                .append(Component.literal(key).withStyle(ChatFormatting.AQUA))
                .append(Component.literal(" = ").withStyle(ChatFormatting.GRAY))
                .append(Component.literal(value).withStyle(ChatFormatting.WHITE))
                .append(Component.literal(" (saved)").withStyle(ChatFormatting.DARK_GREEN));
        source.sendSuccess(() -> ok, true);
        for (String note : notes) {
            source.sendSuccess(() -> Component.literal("note: " + note).withStyle(ChatFormatting.YELLOW), false);
        }
        return 1;
    }

    private static boolean parseBoolStrict(String s) {
        if (s.equalsIgnoreCase("true")) return true;
        if (s.equalsIgnoreCase("false")) return false;
        throw new IllegalArgumentException("not a boolean");
    }

    private static int statsOnce(CommandSourceStack source, Voxyserver mod) {
        if (mod.getStreamingService() == null) {
            source.sendFailure(Component.literal("engine not started"));
            return 0;
        }
        MutableComponent stats = buildStats(source.getServer(), mod);
        source.sendSuccess(() -> stats, false);
        return 1;
    }

    private static int statsContinuous(CommandSourceStack source, Voxyserver mod) {
        ServerPlayer player = source.getPlayer();
        if (player == null) {
            source.sendFailure(Component.literal("continuous stats can only be sent to a player"));
            return 0;
        }
        if (mod.getStreamingService() == null) {
            source.sendFailure(Component.literal("engine not started"));
            return 0;
        }
        mod.addStatsSubscriber(player.getUUID());
        source.sendSuccess(() -> Component.literal("continuous stats started. /voxyserver stats stop to end")
                .withStyle(ChatFormatting.GREEN), false);
        return 1;
    }

    private static int statsStop(CommandSourceStack source, Voxyserver mod) {
        ServerPlayer player = source.getPlayer();
        if (player == null) {
            source.sendFailure(Component.literal("continuous stats can only be sent to a player"));
            return 0;
        }
        boolean wasOn = mod.removeStatsSubscriber(player.getUUID());
        source.sendSuccess(() -> Component.literal(wasOn ? "continuous stats stopped" : "continuous stats was not running")
                .withStyle(ChatFormatting.YELLOW), false);
        return wasOn ? 1 : 0;
    }

    public static MutableComponent buildStats(MinecraftServer server, Voxyserver mod) {
        LodStreamingService.StreamingStats s = mod.getStreamingService().snapshotStats(server);
        VoxyServerConfig cfg = Voxyserver.getConfig();
        ServerStatsTracker.Snapshot counters = ServerStatsTracker.INSTANCE != null
                ? ServerStatsTracker.INSTANCE.snapshot()
                : new ServerStatsTracker.Snapshot(0, 0, 0);

        MutableComponent out = block("stats");

        out.append(Component.literal("\n"))
                .append(kv("players", s.players(), ChatFormatting.AQUA)).append(sep())
                .append(kv("tracked", s.trackedSections(), ChatFormatting.WHITE)).append(sep())
                .append(kv("hashcache", s.hashCacheSize(), ChatFormatting.WHITE)).append(sep())
                .append(kv("pending", s.pendingDirty(), ChatFormatting.WHITE)).append(sep())
                .append(kv("queued", s.queuedDirty(), ChatFormatting.WHITE)).append(sep())
                .append(kv("chunks", s.loadedChunks(), ChatFormatting.WHITE));

        out.append(Component.literal("\n"))
                .append(kv("voxelized", counters.chunksVoxelized(), ChatFormatting.WHITE)).append(sep())
                .append(kv("streamed", counters.sectionsStreamed(), ChatFormatting.WHITE)).append(sep())
                .append(kv("engine", counters.engineActions(), ChatFormatting.WHITE));

        if (cfg != null) {
            out.append(Component.literal("\n"))
                    .append(kvc("hashsync", onOffComp(cfg.hashSyncEnabled))).append(sep())
                    .append(kvc("dirtytracking", onOffComp(cfg.dirtyTrackingEnabled))).append(sep())
                    .append(kvc("debuglog", onOffComp(cfg.debugTrackingEnabled))).append(sep())
                    .append(kv("radius", cfg.lodStreamRadius, ChatFormatting.WHITE)).append(sep())
                    .append(kv("maxpertick", cfg.maxSectionsPerTickPerPlayer, ChatFormatting.WHITE));
        }

        for (LodStreamingService.PlayerStat p : s.perPlayer()) {
            out.append(Component.literal("\n"))
                    .append(Component.literal("  " + p.name()).withStyle(ChatFormatting.YELLOW))
                    .append(Component.literal(" sent ").withStyle(ChatFormatting.GRAY))
                    .append(Component.literal(Integer.toString(p.sentCount())).withStyle(ChatFormatting.WHITE))
                    .append(Component.literal(" @ ").withStyle(ChatFormatting.GRAY))
                    .append(Component.literal(p.chunkX() + "," + p.chunkZ()).withStyle(ChatFormatting.WHITE));
        }
        return out;
    }

    private static MutableComponent block(String title) {
        return Component.empty().append(header(title));
    }

    private static MutableComponent header(String title) {
        return Component.literal("[VoxyServer] ").withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD)
                .append(Component.literal(title).withStyle(ChatFormatting.YELLOW, ChatFormatting.BOLD));
    }

    private static MutableComponent sep() {
        return Component.literal("  ").withStyle(ChatFormatting.DARK_GRAY);
    }

    private static MutableComponent kv(String label, Object value, ChatFormatting valueColor) {
        return Component.literal(label + " ").withStyle(ChatFormatting.GRAY)
                .append(Component.literal(String.valueOf(value)).withStyle(valueColor));
    }

    private static MutableComponent kvc(String label, Component value) {
        return Component.literal(label + " ").withStyle(ChatFormatting.GRAY).append(value);
    }

    private static MutableComponent onOffComp(boolean b) {
        return Component.literal(b ? "on" : "off").withStyle(b ? ChatFormatting.GREEN : ChatFormatting.RED);
    }
}

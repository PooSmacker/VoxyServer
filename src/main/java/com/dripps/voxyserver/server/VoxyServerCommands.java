package com.dripps.voxyserver.server;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.arguments.StringArgumentType;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.SharedSuggestionProvider;
import net.minecraft.network.chat.Component;
import net.minecraft.server.level.ServerLevel;

import java.util.function.Supplier;

public final class VoxyServerCommands {
    private VoxyServerCommands() {
    }

    public static void register(CommandDispatcher<CommandSourceStack> dispatcher, Supplier<WorldImportCoordinator> coordinatorSupplier) {
        dispatcher.register(
                Commands.literal("voxyserver")
                        .requires(source -> source.hasPermission(2))
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
                                                                dimensions.add(level.dimension().location().toString());
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
        );
    }

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
            if (level.dimension().location().toString().equals(dimensionId)) {
                return level;
            }
        }
        return null;
    }
}

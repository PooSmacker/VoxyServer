package com.dripps.voxyserver.client;

import me.shedaniel.clothconfig2.api.ConfigBuilder;
import me.shedaniel.clothconfig2.api.ConfigCategory;
import me.shedaniel.clothconfig2.api.ConfigEntryBuilder;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.network.chat.Component;

public class VoxyConfigScreen {

    public static Screen create(Screen parent) {
        ConfigBuilder builder = ConfigBuilder.create()
                .setParentScreen(parent)
                .setTitle(Component.literal("VoxyServer Client Settings"));

        ConfigEntryBuilder entryBuilder = builder.entryBuilder();

        // Display settings are global (saved to the client config) and always editable,
        // even when not connected to a server.
        ConfigCategory display = builder.getOrCreateCategory(Component.literal("Display"));
        display.addEntry(entryBuilder.startBooleanToggle(
                        Component.literal("Show Download HUD"),
                        ClientLodSettings.isDownloadHudEnabled())
                .setDefaultValue(true)
                .setTooltip(Component.literal("show the world-data download overlay at the top of the screen while LODs stream in"))
                .setSaveConsumer(ClientLodSettings::setDownloadHudEnabled)
                .build());
        display.addEntry(entryBuilder.startBooleanToggle(
                        Component.literal("Download HUD in Top-Left Corner"),
                        ClientLodSettings.isDownloadHudTopLeft())
                .setDefaultValue(false)
                .setTooltip(Component.literal("place the download overlay in the top-left corner instead of centered"))
                .setSaveConsumer(ClientLodSettings::setDownloadHudTopLeft)
                .build());

        ConfigCategory category = builder.getOrCreateCategory(Component.literal("LOD Streaming"));

        // set before the early return so it applies whether or not we are connected
        builder.setSavingRunnable(() -> {
            ClientLodSettings.saveClientConfig();
            if (ClientLodSettings.hasActiveServerProfile()) {
                ClientLodSettings.saveAndSendPreferences();
            }
        });

        if (!ClientLodSettings.hasActiveServerProfile()) {
            category.addEntry(entryBuilder.startTextDescription(
                    Component.literal("per server overrides can only be edited while connected to a server"))
                    .build());
            return builder.build();
        }

        int maxRadius = ClientLodSettings.getServerMaxRadius();
        int maxSections = ClientLodSettings.getServerMaxSections();

        if (maxRadius <= 0) maxRadius = 256;
        if (maxSections <= 0) maxSections = 50;

        category.addEntry(entryBuilder.startBooleanToggle(
                        Component.literal("Enable LOD Streaming"),
                        ClientLodSettings.isEnabled())
                .setDefaultValue(true)
                .setTooltip(Component.literal("toggle whether the server sends LOD data to you"))
                .setSaveConsumer(ClientLodSettings::setEnabled)
                .build());

        category.addEntry(entryBuilder.startIntSlider(
                        Component.literal("LOD Stream Radius"),
                        ClientLodSettings.getPreferredRadius(),
                        0, maxRadius)
                .setDefaultValue(0)
                .setTooltip(Component.literal("how far LODs are streamed in blocks, 0 = server default (" + maxRadius + ")"))
                .setSaveConsumer(ClientLodSettings::setPreferredRadius)
                .build());

        category.addEntry(entryBuilder.startIntSlider(
                        Component.literal("Max Sections Per Tick"),
                        ClientLodSettings.getPreferredMaxSections(),
                        0, maxSections)
                .setDefaultValue(0)
                .setTooltip(Component.literal("rate limit for sections sent per tick, 0 = server default (" + maxSections + ")"))
                .setSaveConsumer(ClientLodSettings::setPreferredMaxSections)
                .build());

        return builder.build();
    }
}

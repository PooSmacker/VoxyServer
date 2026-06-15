package com.dripps.voxyserver.client;

import com.dripps.voxyserver.client.service.IVoxyServerIngestAccess;
import com.dripps.voxyserver.client.service.RemoteIngestService;
import com.dripps.voxyserver.network.LODClearPayload;
import com.dripps.voxyserver.network.LODHandshakePayload;
import com.dripps.voxyserver.network.LODProtocolPayload;
import com.dripps.voxyserver.network.LODReadyPayload;
import com.dripps.voxyserver.network.LODServerSettingsPayload;
import com.dripps.voxyserver.network.PreSerializedLodPayload;
import com.dripps.voxyserver.network.VoxyServerNetworking;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.commonImpl.VoxyCommon;
import me.cortex.voxy.commonImpl.VoxyInstance;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayConnectionEvents;
import net.fabricmc.fabric.api.client.networking.v1.ClientPlayNetworking;
import net.minecraft.ChatFormatting;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.core.RegistryAccess;
import net.minecraft.network.chat.Component;

public class ClientLodReceiver {

    public static void register() {
        ClientTickEvents.END_CLIENT_TICK.register(client -> ClientLodSettings.onClientTick());

        ClientPlayConnectionEvents.JOIN.register((handler, sender, client) -> {
            ClientLodSettings.prepareForCurrentConnection();
            ClientPlayNetworking.send(new LODReadyPayload());
            if (ClientPlayNetworking.canSend(LODHandshakePayload.TYPE)) {
                ClientPlayNetworking.send(new LODHandshakePayload(VoxyServerNetworking.PROTOCOL_VERSION));
            } else {
                client.execute(() -> {
                    ClientLodSettings.setProtocolOk(false);
                    tellPlayer(serverOutOfDateMessage());
                });
            }
        });

        ClientPlayConnectionEvents.DISCONNECT.register((handler, client) -> {
            ClientLodSettings.reset();
            setRemoteIngest(false);
            ClientLodHashStore.get().flush();
        });

        ClientPlayNetworking.registerGlobalReceiver(LODProtocolPayload.TYPE, (payload, context) -> {
            context.client().execute(() -> {
                int serverProto = payload.protocol();
                int clientProto = VoxyServerNetworking.PROTOCOL_VERSION;
                if (serverProto == clientProto) {
                    ClientLodSettings.setProtocolOk(true);
                } else {
                    ClientLodSettings.setProtocolOk(false);
                    if (serverProto < clientProto) {
                        tellPlayer(serverOutOfDateMessage());
                    }
                }
            });
        });

        ClientPlayNetworking.registerGlobalReceiver(LODServerSettingsPayload.TYPE, (payload, context) -> {
            context.client().execute(() -> ClientLodSettings.applyServerSettings(
                    payload.maxLodStreamRadius(), payload.maxSectionsPerTick()));
        });

        ClientPlayNetworking.registerGlobalReceiver(PreSerializedLodPayload.TYPE, (payload, context) -> {
            context.client().execute(() -> {
                if (!ClientLodSettings.isProtocolOk()) return;
                ClientLevel level = context.client().level;
                if (level == null) return;

                VoxyInstance instance = VoxyCommon.getInstance();
                if (instance == null) return;

                ((IVoxyServerIngestAccess) instance).voxyserver$setUsingRemoteIngest(true);

                RemoteIngestService ingestService =
                        ((IVoxyServerIngestAccess) instance).voxyserver$getRemoteIngestService();
                if (ingestService == null || !ingestService.isLive()) return;

                WorldIdentifier worldId = WorldIdentifier.of(level);
                if (worldId == null) return;

                WorldEngine engine = instance.getOrCreate(worldId);
                if (engine == null || !engine.isLive()) return;

                RegistryAccess registryAccess = level.registryAccess();

                // decodeBulk happens on the ingest worker thread
                ingestService.enqueueIngest(engine, payload, registryAccess, worldId.getWorldId());
            });
        });

        ClientPlayNetworking.registerGlobalReceiver(LODClearPayload.TYPE, (payload, context) -> {
            context.client().execute(() -> handleClear(payload));
        });
    }

    private static void setRemoteIngest(boolean enabled) {
        VoxyInstance instance = VoxyCommon.getInstance();
        if (instance == null) return;
        ((IVoxyServerIngestAccess) instance).voxyserver$setUsingRemoteIngest(enabled);
    }

    private static void tellPlayer(Component message) {
        Minecraft mc = Minecraft.getInstance();
        if (mc.player != null) {
            mc.gui.getChat().addClientSystemMessage(message);
        }
    }

    private static Component serverOutOfDateMessage() {
        return Component.literal("[VoxyServer] ").withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD)
                .append(Component.literal("this server's VoxyServer is out of date. LODs disabled, ask the admin to update.")
                        .withStyle(ChatFormatting.RED));
    }

    private static void handleClear(LODClearPayload payload) {
        // dimension change clear is handled by voxy itself when the world changes
        // this is a signal from the server to reset any cached state
    }
}
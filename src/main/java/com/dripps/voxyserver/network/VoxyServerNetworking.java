package com.dripps.voxyserver.network;

import net.fabricmc.fabric.api.networking.v1.PayloadTypeRegistry;

public class VoxyServerNetworking {

    public static void register() {
        PayloadTypeRegistry.clientboundPlay().register(LODSectionPayload.TYPE, LODSectionPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(LODBulkPayload.TYPE, LODBulkPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(PreSerializedLodPayload.TYPE, PreSerializedLodPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(LODClearPayload.TYPE, LODClearPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(LODServerSettingsPayload.TYPE, LODServerSettingsPayload.CODEC);

        PayloadTypeRegistry.serverboundPlay().register(LODReadyPayload.TYPE, LODReadyPayload.CODEC);
        PayloadTypeRegistry.serverboundPlay().register(LODPreferencesPayload.TYPE, LODPreferencesPayload.CODEC);
    }
}

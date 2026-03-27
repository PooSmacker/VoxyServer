package com.dripps.voxyserver.network;

import net.fabricmc.fabric.api.networking.v1.PayloadTypeRegistry;

public class VoxyServerNetworking {

    public static void register() {
        PayloadTypeRegistry.clientboundPlay().register(LODSectionPayload.TYPE, LODSectionPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(LODBulkPayload.TYPE, LODBulkPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(LODClearPayload.TYPE, LODClearPayload.CODEC);
        //server
        PayloadTypeRegistry.serverboundPlay().register(LODReadyPayload.TYPE, LODReadyPayload.CODEC);
    }
}

package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.ResourceLocation;

// client signals it has VoxyServer + Voxy and is ready to receive lods
public record LODReadyPayload() implements CustomPacketPayload {

    public static final Type<LODReadyPayload> TYPE =
            new Type<>(ResourceLocation.parse("voxyserver:lod_ready"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODReadyPayload> CODEC =
            StreamCodec.of(LODReadyPayload::write, LODReadyPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODReadyPayload payload) {
        // empty payload :D
    }

    private static LODReadyPayload read(RegistryFriendlyByteBuf buf) {
        return new LODReadyPayload();
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}

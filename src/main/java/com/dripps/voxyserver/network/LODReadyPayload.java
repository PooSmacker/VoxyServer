package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

// client signals it has VoxyServer + Voxy and is ready to receive lods
public record LODReadyPayload(long cacheId) implements CustomPacketPayload {

    public static final Type<LODReadyPayload> TYPE =
            new Type<>(Identifier.parse("voxyserver:lod_ready"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODReadyPayload> CODEC =
            StreamCodec.of(LODReadyPayload::write, LODReadyPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODReadyPayload payload) {
        buf.writeLong(payload.cacheId);
    }

    private static LODReadyPayload read(RegistryFriendlyByteBuf buf) {
        return new LODReadyPayload(buf.readLong());
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}

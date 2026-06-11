package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

public record LODHandshakePayload(int protocol) implements CustomPacketPayload {

    public static final Type<LODHandshakePayload> TYPE =
            new Type<>(Identifier.parse("voxyserver:lod_handshake"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODHandshakePayload> CODEC =
            StreamCodec.of(LODHandshakePayload::write, LODHandshakePayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODHandshakePayload payload) {
        buf.writeVarInt(payload.protocol);
    }

    private static LODHandshakePayload read(RegistryFriendlyByteBuf buf) {
        return new LODHandshakePayload(buf.readVarInt());
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}

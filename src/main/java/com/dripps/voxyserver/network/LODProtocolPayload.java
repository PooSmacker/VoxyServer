package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

public record LODProtocolPayload(int protocol) implements CustomPacketPayload {

    public static final Type<LODProtocolPayload> TYPE =
            new Type<>(Identifier.parse("voxyserver:lod_protocol"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODProtocolPayload> CODEC =
            StreamCodec.of(LODProtocolPayload::write, LODProtocolPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODProtocolPayload payload) {
        buf.writeVarInt(payload.protocol);
    }

    private static LODProtocolPayload read(RegistryFriendlyByteBuf buf) {
        return new LODProtocolPayload(buf.readVarInt());
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}

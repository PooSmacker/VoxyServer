package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

// server to client streaming progress for a dimension.
// sent = sections streamed so far this scan session, total = sections we expect to send,
// complete = the scan has caught up (bulk transfer finished).
public record LODProgressPayload(
        Identifier dimension,
        int sent,
        int total,
        boolean complete
) implements CustomPacketPayload {

    public static final Type<LODProgressPayload> TYPE =
            new Type<>(Identifier.parse("voxyserver:lod_progress"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODProgressPayload> CODEC =
            StreamCodec.of(LODProgressPayload::write, LODProgressPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODProgressPayload payload) {
        buf.writeIdentifier(payload.dimension);
        buf.writeVarInt(payload.sent);
        buf.writeVarInt(payload.total);
        buf.writeBoolean(payload.complete);
    }

    private static LODProgressPayload read(RegistryFriendlyByteBuf buf) {
        Identifier dimension = buf.readIdentifier();
        int sent = buf.readVarInt();
        int total = buf.readVarInt();
        boolean complete = buf.readBoolean();
        return new LODProgressPayload(dimension, sent, total, complete);
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}

package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

// client to server manifest of sections it already stores for a dimension
// keys and hashes are parallel arrays. complete marks the final chunk
public record LODManifestPayload(
        Identifier dimension,
        long[] keys,
        long[] hashes,
        boolean complete
) implements CustomPacketPayload {

    public static final Type<LODManifestPayload> TYPE =
            new Type<>(Identifier.parse("voxyserver:lod_manifest"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODManifestPayload> CODEC =
            StreamCodec.of(LODManifestPayload::write, LODManifestPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODManifestPayload payload) {
        buf.writeIdentifier(payload.dimension);
        buf.writeBoolean(payload.complete);
        int count = payload.keys.length;
        buf.writeVarInt(count);
        for (int i = 0; i < count; i++) {
            buf.writeLong(payload.keys[i]);
            buf.writeLong(payload.hashes[i]);
        }
    }

    private static LODManifestPayload read(RegistryFriendlyByteBuf buf) {
        Identifier dimension = buf.readIdentifier();
        boolean complete = buf.readBoolean();
        int count = buf.readVarInt();
        long[] keys = new long[count];
        long[] hashes = new long[count];
        for (int i = 0; i < count; i++) {
            keys[i] = buf.readLong();
            hashes[i] = buf.readLong();
        }
        return new LODManifestPayload(dimension, keys, hashes, complete);
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}

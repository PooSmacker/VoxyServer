package com.dripps.voxyserver.network;

import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.codec.StreamCodec;
import net.minecraft.network.protocol.common.custom.CustomPacketPayload;
import net.minecraft.resources.Identifier;

public record LODHashSyncSettingsPayload(
        boolean enabled
) implements CustomPacketPayload {

    public static final Type<LODHashSyncSettingsPayload> TYPE =
            new Type<>(Identifier.parse("voxyserver:lod_hash_sync_settings"));

    public static final StreamCodec<RegistryFriendlyByteBuf, LODHashSyncSettingsPayload> CODEC =
            StreamCodec.of(LODHashSyncSettingsPayload::write, LODHashSyncSettingsPayload::read);

    private static void write(RegistryFriendlyByteBuf buf, LODHashSyncSettingsPayload payload) {
        buf.writeBoolean(payload.enabled);
    }

    private static LODHashSyncSettingsPayload read(RegistryFriendlyByteBuf buf) {
        return new LODHashSyncSettingsPayload(buf.readBoolean());
    }

    @Override
    public Type<? extends CustomPacketPayload> type() {
        return TYPE;
    }
}
